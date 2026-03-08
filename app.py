from flask import Flask, render_template, request, redirect, url_for, flash, session
import boto3
import os
import uuid
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import json

app = Flask(__name__)
app.secret_key = "stocker_secret_2024"

# AWS Configuration
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

# Boto3 session
if AWS_ACCESS_KEY and AWS_SECRET_KEY:
    # Local development with explicit credentials
    boto3_session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
else:
    # EC2 instance with IAM role
    boto3_session = boto3.Session(region_name=AWS_REGION)

# DynamoDB resource
dynamodb = boto3_session.resource('dynamodb')

# SNS client
sns = boto3_session.client('sns')

# DynamoDB Table Names
USER_TABLE = 'stocker_users'
STOCK_TABLE = 'stocker_stocks'
TRANSACTION_TABLE = 'stocker_transactions'
PORTFOLIO_TABLE = 'stocker_portfolio'

# SNS Topic ARNs - Set to None to safely skip notifications if not configured
USER_ACCOUNT_TOPIC_ARN = None
TRANSACTION_TOPIC_ARN = None

# --- Helper Classes & Functions ---
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)

def clean_dynamo_response(response):
    if not response:
        return None
    return json.loads(json.dumps(response, cls=DecimalEncoder))

def send_notification(topic_arn, subject, message, attributes=None):
    if not topic_arn:
        print(f"Warning: Missing SNS topic ARN for notification: {subject}")
        return False
    try:
        kwargs = {'TopicArn': topic_arn, 'Subject': subject, 'Message': message}
        if attributes:
            kwargs['MessageAttributes'] = attributes
        sns.publish(**kwargs)
        return True
    except Exception as e:
        print(f"SNS notification failed: {str(e)}")
        return False

# --- Data Access Functions ---
def get_user_by_email(email):
    table = dynamodb.Table(USER_TABLE)
    response = table.get_item(Key={'email': email})
    return response.get('Item')

def get_user_by_id(user_id):
    table = dynamodb.Table(USER_TABLE)
    response = table.scan(FilterExpression=Attr('id').eq(user_id))
    items = response.get('Items', [])
    return items[0] if items else None

def create_user(username, email, password, role):
    table = dynamodb.Table(USER_TABLE)
    user = {
        'id': str(uuid.uuid4()),
        'username': username,
        'email': email,
        'password': password,
        'role': role
    }
    table.put_item(Item=user)
    return user

def get_all_stocks():
    table = dynamodb.Table(STOCK_TABLE)
    response = table.scan()
    return response.get('Items', [])

def get_stock_by_id(stock_id):
    table = dynamodb.Table(STOCK_TABLE)
    response = table.get_item(Key={'id': stock_id})
    return response.get('Item')

def get_traders():
    table = dynamodb.Table(USER_TABLE)
    response = table.scan(FilterExpression=Attr('role').eq('trader'))
    return response.get('Items', [])

def get_portfolio_item(user_id, stock_id):
    table = dynamodb.Table(PORTFOLIO_TABLE)
    response = table.get_item(Key={'user_id': user_id, 'stock_id': stock_id})
    return response.get('Item')

def update_portfolio(user_id, stock_id, quantity, average_price):
    table = dynamodb.Table(PORTFOLIO_TABLE)
    if not isinstance(quantity, Decimal):
        quantity = Decimal(str(quantity))
    if not isinstance(average_price, Decimal):
        average_price = Decimal(str(average_price))
    
    existing = get_portfolio_item(user_id, stock_id)
    
    if existing and quantity > 0:
        table.update_item(
            Key={'user_id': user_id, 'stock_id': stock_id},
            UpdateExpression="set quantity=:q, average_price=:p",
            ExpressionAttributeValues={':q': quantity, ':p': average_price}
        )
    elif existing and quantity <= 0:
        table.delete_item(Key={'user_id': user_id, 'stock_id': stock_id})
    elif quantity > 0:
        table.put_item(Item={
            'user_id': user_id,
            'stock_id': stock_id,
            'quantity': quantity,
            'average_price': average_price
        })

def create_transaction(user_id, stock_id, action, quantity, price, status='completed'):
    table = dynamodb.Table(TRANSACTION_TABLE)
    transaction_id = str(uuid.uuid4())
    transaction = {
        'id': transaction_id,
        'user_id': user_id,
        'stock_id': stock_id,
        'action': action,
        'quantity': quantity,
        'price': Decimal(str(price)),
        'status': status,
        'transaction_date': datetime.now().isoformat()
    }
    table.put_item(Item=transaction)
    return transaction

def get_user_transactions(user_id):
    table = dynamodb.Table(TRANSACTION_TABLE)
    response = table.scan(FilterExpression=Attr('user_id').eq(user_id))
    transactions = response.get('Items', [])
    for t in transactions:
        stock = get_stock_by_id(t['stock_id'])
        if stock:
            t['stock'] = stock
    transactions.sort(key=lambda x: x.get('transaction_date', ''), reverse=True)
    return transactions

def delete_trader_by_id(trader_id):
    user = get_user_by_id(trader_id)
    if not user:
        return False
    # Delete portfolio items
    portfolio_table = dynamodb.Table(PORTFOLIO_TABLE)
    portfolio_response = portfolio_table.query(KeyConditionExpression=Key('user_id').eq(trader_id))
    for item in portfolio_response.get('Items', []):
        portfolio_table.delete_item(Key={'user_id': trader_id, 'stock_id': item['stock_id']})
    # Delete user
    user_table = dynamodb.Table(USER_TABLE)
    user_table.delete_item(Key={'email': user['email']})
    return True

def get_user_portfolio(user_id):
    table = dynamodb.Table(PORTFOLIO_TABLE)
    response = table.query(KeyConditionExpression=Key('user_id').eq(user_id))
    portfolio = response.get('Items', [])
    for item in portfolio:
        stock = get_stock_by_id(item['stock_id'])
        if stock:
            item['stock'] = stock
    return portfolio

def get_transactions():
    table = dynamodb.Table(TRANSACTION_TABLE)
    transactions = table.scan().get('Items', [])
    for t in transactions:
        t['user'] = get_user_by_id(t['user_id'])
        t['stock'] = get_stock_by_id(t['stock_id'])
    return transactions

def get_portfolios():
    table = dynamodb.Table(PORTFOLIO_TABLE)
    portfolios = table.scan().get('Items', [])
    for p in portfolios:
        p['stock'] = get_stock_by_id(p['stock_id'])
    return portfolios

# --- Routes ---
@app.route('/')
def index():
    return render_template("index.html")

@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']
        role = request.form['role']
        if get_user_by_email(email):
            flash('User already exists. Please login.', 'warning')
            return redirect(url_for('login'))
        user = create_user(username, email, password, role)
        send_notification(
            USER_ACCOUNT_TOPIC_ARN,
            'New User Registration',
            f"New user registered: {username} ({email}) as {role}",
            {'event_type': {'DataType': 'String','StringValue':'ACCOUNT_CREATION'},
             'user_role': {'DataType':'String','StringValue':role}}
        )
        flash(f"Account created for {username}", 'success')
        return redirect(url_for('login'))
    return render_template('signup.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form.get('email')
        password = request.form.get('password')
        role = request.form.get('role')
        user = get_user_by_email(email)
        if user and user['password'] == password and user['role'] == role:
            session['email'] = user['email']
            session['role'] = user['role']
            session['user_id'] = user['id']
            send_notification(
                USER_ACCOUNT_TOPIC_ARN,
                'User Login',
                f"User logged in: {user['username']} ({email}) as {role}",
                {'event_type':{'DataType':'String','StringValue':'LOGIN'},
                 'user_role':{'DataType':'String','StringValue':role}}
            )
            flash('Login successful', 'success')
            return redirect(url_for('dashboard_admin' if role=='admin' else 'dashboard_trader'))
        else:
            flash('Invalid credentials or role mismatch', 'danger')
            return redirect(url_for('login'))
    return render_template('login.html')

@app.route('/dashboard_admin')
def dashboard_admin():
    if 'role' not in session or session['role'] != 'admin':
        flash("Admins only", "danger")
        return redirect(url_for('login'))
    user = get_user_by_email(session['email'])
    stocks = get_all_stocks()
    return render_template('dashboard_admin.html', user=user, market_data=stocks)

@app.route('/dashboard_trader')
def dashboard_trader():
    if 'role' not in session or session['role'] != 'trader':
        flash("Traders only", "danger")
        return redirect(url_for('login'))
    user = get_user_by_email(session['email'])
    stocks = get_all_stocks()
    return render_template('dashboard_trader.html', user=user, market_data=stocks)

# --- Admin Services ---
@app.route('/service01')
def service01():
    if 'role' not in session or session['role'] != 'admin':
        flash("Admins only", "danger")
        return redirect(url_for('login'))
    traders = get_traders()
    for trader in traders:
        portfolio = get_user_portfolio(trader['id'])
        trader['total_portfolio_value'] = sum(float(p['quantity'])*float(p['stock']['price']) for p in portfolio)
    return render_template('service-details-1.html', traders=traders)

@app.route('/service02')
def service02():
    if 'role' not in session or session['role'] != 'admin':
        flash("Admins only", "danger")
        return redirect(url_for('login'))
    transactions = get_transactions()
    for t in transactions:
        try:
            t['transaction_date'] = datetime.fromisoformat(t['transaction_date'])
        except:
            t['transaction_date'] = None
    return render_template('service-details-2.html', transactions=transactions)

@app.route('/service03')
def service03():
    if 'role' not in session or session['role'] != 'admin':
        flash("Admins only", "danger")
        return redirect(url_for('login'))
    portfolios = get_portfolios()
    total_value = sum(float(p['quantity'])*float(p['stock']['price']) for p in portfolios if 'stock' in p)
    return render_template('service-details-3.html', portfolios=portfolios, total_portfolio_value=total_value)

# --- Trader Services ---
@app.route('/service04')
def service04():
    if 'role' not in session or session['role'] != 'trader':
        flash("Traders only", "danger")
        return redirect(url_for('login'))
    user = get_user_by_email(session['email'])
    stocks = get_all_stocks()
    return render_template('service-details-4.html', user=user, stocks=stocks)

@app.route('/service04/buy_stock/<string:stock_id>', methods=['GET', 'POST'])
def buy_stock(stock_id):
    if 'role' not in session or session['role'] != 'trader':
        flash("Traders only", "danger")
        return redirect(url_for('login'))
    user = get_user_by_email(session['email'])
    stock = get_stock_by_id(stock_id)
    if not stock:
        flash("Stock not found", "danger")
        return redirect(url_for('service04'))
    if request.method == 'POST':
        quantity = int(request.form.get("quantity", 0))
        if quantity <= 0:
            flash("Enter a valid quantity", "danger")
            return redirect(url_for('buy_stock', stock_id=stock_id))
        # Create transaction
        create_transaction(user['id'], stock_id, "buy", quantity, float(stock['price']))
        portfolio_entry = get_portfolio_item(user['id'], stock_id)
        if portfolio_entry:
            current_qty = Decimal(str(portfolio_entry['quantity']))
            current_avg = Decimal(str(portfolio_entry['average_price']))
            stock_price = Decimal(str(stock['price']))
            total_qty = current_qty + Decimal(str(quantity))
            avg_price = (current_qty*current_avg + Decimal(str(quantity))*stock_price)/total_qty
            update_portfolio(user['id'], stock_id, total_qty, avg_price)
        else:
            update_portfolio(user['id'], stock_id, Decimal(str(quantity)), Decimal(str(stock['price'])))
        send_notification(
            TRANSACTION_TOPIC_ARN,
            f"Stock Purchase: {stock['symbol']}",
            f"{user['username']} purchased {quantity} shares of {stock['symbol']} at {stock['price']}",
            {'event_type': {'DataType':'String','StringValue':'BUY'}}
        )
        flash(f"Purchased {quantity} shares of {stock['symbol']}", 'success')
        return redirect(url_for('service05'))
    return render_template('buy_stock.html', user=user, stock=stock)

@app.route('/service04/sell_stock/<string:stock_id>', methods=['GET', 'POST'])
def sell_stock(stock_id):
    if 'role' not in session or session['role'] != 'trader':
        flash("Traders only", "danger")
        return redirect(url_for('login'))
    user = get_user_by_email(session['email'])
    stock = get_stock_by_id(stock_id)
    portfolio_entry = get_portfolio_item(user['id'], stock_id)
    if not stock or not portfolio_entry:
        flash("Stock not found or you do not own shares", "danger")
        return redirect(url_for('service04'))
    if request.method == 'POST':
        quantity = int(request.form.get("quantity",0))
        if quantity<=0 or quantity>portfolio_entry['quantity']:
            flash("Invalid quantity", "danger")
            return redirect(url_for('sell_stock', stock_id=stock_id))
        create_transaction(user['id'], stock_id, "sell", quantity, float(stock['price']))
        remaining_qty = portfolio_entry['quantity'] - quantity
        avg_price = float(portfolio_entry['average_price']) if remaining_qty>0 else 0
        update_portfolio(user['id'], stock_id, remaining_qty, avg_price)
        flash(f"Sold {quantity} shares of {stock['symbol']}", "success")
        return redirect(url_for('service05'))
    return render_template('sell_stock.html', user=user, stock=stock, portfolio_entry=portfolio_entry)

@app.route('/service05')
def service05():
    if 'role' not in session or session['role'] != 'trader':
        flash("Traders only", "danger")
        return redirect(url_for('login'))
    user = get_user_by_email(session['email'])
    portfolio = get_user_portfolio(user['id'])
    total_value = sum(float(p['quantity'])*float(p['stock']['price']) for p in portfolio if 'stock' in p)
    transactions = get_user_transactions(user['id'])
    for t in transactions:
        try:
            t['transaction_date'] = datetime.fromisoformat(t['transaction_date'])
        except:
            t['transaction_date'] = None
    return render_template('service-details-5.html', user=user, portfolio=portfolio, total_value=total_value, transactions=transactions)

@app.route('/logout')
def logout():
    session.clear()
    flash("Logged out", "info")
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
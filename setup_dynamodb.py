import boto3
import uuid
import os
from decimal import Decimal
from datetime import datetime, date
from boto3.dynamodb.conditions import Attr

# AWS Configuration
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# Session
if AWS_ACCESS_KEY and AWS_SECRET_KEY:
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
else:
    session = boto3.Session(region_name=AWS_REGION)

dynamodb = session.resource('dynamodb')
dynamodb_client = session.client('dynamodb')

# Tables
USER_TABLE = 'stocker_users'
STOCK_TABLE = 'stocker_stocks'
TRANSACTION_TABLE = 'stocker_transactions'
PORTFOLIO_TABLE = 'stocker_portfolio'

existing_tables = dynamodb_client.list_tables()['TableNames']

def create_table_if_not_exists(table_name, key_schema, attribute_definitions):
    if table_name not in existing_tables:
        print(f"Creating table: {table_name}")
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            BillingMode='PAY_PER_REQUEST'
        )
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print(f"{table_name} created")
    else:
        print(f"Table {table_name} already exists.")

# Users table
create_table_if_not_exists(
    USER_TABLE,
    [{'AttributeName': 'email', 'KeyType': 'HASH'}],
    [{'AttributeName': 'email', 'AttributeType': 'S'}]
)

# Stocks table
create_table_if_not_exists(
    STOCK_TABLE,
    [{'AttributeName': 'id', 'KeyType': 'HASH'}],
    [{'AttributeName': 'id', 'AttributeType': 'S'}]
)

# Transactions table
create_table_if_not_exists(
    TRANSACTION_TABLE,
    [{'AttributeName': 'id', 'KeyType': 'HASH'}],
    [{'AttributeName': 'id', 'AttributeType': 'S'}]
)

# Portfolio table
create_table_if_not_exists(
    PORTFOLIO_TABLE,
    [
        {'AttributeName': 'user_id', 'KeyType': 'HASH'},
        {'AttributeName': 'stock_id', 'KeyType': 'RANGE'}
    ],
    [
        {'AttributeName': 'user_id', 'AttributeType': 'S'},
        {'AttributeName': 'stock_id', 'AttributeType': 'S'}
    ]
)

def add_sample_data():

    user_table = dynamodb.Table(USER_TABLE)

    users = [
        {"id": str(uuid.uuid4()), "username": "Admin", "email": "admin@example.com", "password": "admin123", "role": "admin"},
        {"id": str(uuid.uuid4()), "username": "Trader One", "email": "trader1@example.com", "password": "trader123", "role": "trader"},
        {"id": str(uuid.uuid4()), "username": "Trader Two", "email": "trader2@example.com", "password": "trader123", "role": "trader"}
    ]

    for user in users:
        res = user_table.get_item(Key={'email': user['email']})
        if 'Item' not in res:
            user_table.put_item(Item=user)
            print("Added user:", user["username"])

    trader1 = users[1]
    trader2 = users[2]

    stock_table = dynamodb.Table(STOCK_TABLE)

    stocks = [
        {"id": str(uuid.uuid4()), "symbol": "RELIANCE", "name": "Reliance Industries", "price": Decimal("2500"), "date_added": date.today().isoformat()},
        {"id": str(uuid.uuid4()), "symbol": "TCS", "name": "Tata Consultancy Services", "price": Decimal("3600"), "date_added": date.today().isoformat()},
        {"id": str(uuid.uuid4()), "symbol": "INFY", "name": "Infosys", "price": Decimal("1500"), "date_added": date.today().isoformat()},
        {"id": str(uuid.uuid4()), "symbol": "HDFCBANK", "name": "HDFC Bank", "price": Decimal("1600"), "date_added": date.today().isoformat()}
    ]

    stock_ids = {}

    for stock in stocks:
        res = stock_table.scan(
            FilterExpression=Attr("symbol").eq(stock["symbol"])
        )

        if not res["Items"]:
            stock_table.put_item(Item=stock)
            print("Added stock:", stock["symbol"])
            stock_ids[stock["symbol"]] = stock["id"]
        else:
            stock_ids[stock["symbol"]] = res["Items"][0]["id"]

    transaction_table = dynamodb.Table(TRANSACTION_TABLE)
    portfolio_table = dynamodb.Table(PORTFOLIO_TABLE)

    txn = {
        "id": str(uuid.uuid4()),
        "user_id": trader1["id"],
        "stock_id": stock_ids["RELIANCE"],
        "action": "buy",
        "quantity": 10,
        "price": Decimal("2500"),
        "status": "completed",
        "transaction_date": datetime.now().isoformat()
    }

    transaction_table.put_item(Item=txn)

    portfolio_table.put_item(
        Item={
            "user_id": trader1["id"],
            "stock_id": stock_ids["RELIANCE"],
            "quantity": 10,
            "average_price": Decimal("2500")
        }
    )

    print("Sample transactions added.")

add_sample_data()

print("DynamoDB setup completed successfully.")
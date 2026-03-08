document.addEventListener('DOMContentLoaded', function () {
          // Form validation
          const forms = document.querySelectorAll('form');
          forms.forEach(form => {
                    form.addEventListener('submit', function (e) {
                              // Password confirmation check (signup)
                              const password = form.querySelector('input[type="password"]');
                              const confirm = form.querySelector('input[name="confirm_password"]');

                              if (confirm && password && confirm.value !== password.value) {
                                        e.preventDefault();
                                        alert('Passwords do not match!');
                                        return false;
                              }

                              // Email validation
                              const emailInputs = form.querySelectorAll('input[type="email"]');
                              emailInputs.forEach(email => {
                                        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
                                        if (!emailRegex.test(email.value)) {
                                                  e.preventDefault();
                                                  alert('Please enter a valid email address');
                                                  return false;
                                        }
                              });
                    });
          });

          // Toggle password visibility
          const togglePassword = document.querySelectorAll('.toggle-password');
          togglePassword.forEach(btn => {
                    btn.addEventListener('click', function () {
                              const input = document.querySelector(this.dataset.target);
                              if (input.type === 'password') {
                                        input.type = 'text';
                                        this.textContent = 'Hide';
                              } else {
                                        input.type = 'password';
                                        this.textContent = 'Show';
                              }
                    });
          });

          // Responsive navigation
          const navToggle = document.getElementById('nav-toggle');
          const navMenu = document.getElementById('nav-menu');

          if (navToggle && navMenu) {
                    navToggle.addEventListener('click', () => {
                              navMenu.classList.toggle('active');
                    });
          }

          // Auto-hide alerts
          setTimeout(() => {
                    const alerts = document.querySelectorAll('.alert');
                    alerts.forEach(alert => {
                              alert.style.transition = 'opacity 0.5s ease';
                              alert.style.opacity = '0';
                              setTimeout(() => alert.remove(), 500);
                    });
          }, 5000);
      });
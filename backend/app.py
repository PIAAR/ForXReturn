from backend.api.routes.routes import create_app
import sys
import os

# Add the parent directory to sys.path
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


'''
Flask is a web application framework written in Python. It is based on the Werkzeug WSGI toolkit and Jinja2 template engine. 
Flask is called a micro framework because it does not require particular tools or libraries. It has no database abstraction layer and form validation. 
This file should create and run your Flask application. You need to import the create_app function from routes.py and use it to initialize the Flask app.
'''

# Create the Flask application
app = create_app()

if __name__ == '__main__':
    app.run(debug=True)
    # for rule in app.url_map.iter_rules():
    #     print(rule)


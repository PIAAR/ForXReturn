'''
This file contains the middleware functions that are used to add functionality to the API endpoints. 
Handles middleware functions.
'''

def require_authentication(f):
    def wrap(*args, **kwargs):
        # Add authentication logic
        return f(*args, **kwargs)
    return wrap

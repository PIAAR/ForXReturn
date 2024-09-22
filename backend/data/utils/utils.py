'''
This file contains utility functions that are used in the data module.
Utility functions for the data module.
'''

def convert_to_float(value):
    try:
        return float(value)
    except ValueError:
        return None

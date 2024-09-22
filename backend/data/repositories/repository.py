from .models import Trade
from ..utils.extensions import db

'''
This file contains the repository for the database. 
Data access and repository logic.
'''

def get_trade_by_id(trade_id):
    return Trade.query.filter_by(id=trade_id).first()

def save_trade(trade):
    db.session.add(trade)
    db.session.commit()

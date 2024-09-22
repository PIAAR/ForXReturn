from sqlalchemy import Column, Integer, String
from .database import Base

'''
This file contains the models for the database. 
Data models for interacting with the database.
'''

class Trade(Base):
    __tablename__ = 'trades'
    id = Column(Integer, primary_key=True)
    symbol = Column(String)
    entry_price = Column(Integer)
    exit_price = Column(Integer)
    status = Column(String)

# backend/trading/strategies/strategy.py
from trading.indicators.rsi import RSI
from trading.indicators.sma import SMA
import pandas as pd

'''
This file contains the implementation of trading strategies.
Defines trading strategies and their application logic.
'''

def setup_strategy():
    """
    Sets up the trading strategy.
    Initializes any required settings or configurations for the strategy.
    """
    print("Trading strategy set up.")

def teardown_strategy():
    """
    Teardowns the trading strategy.
    Cleans up resources or any finalization steps for the strategy.
    """
    print("Trading strategy torn down.")

def calculate_rsi(df, period=14):
    """
    Calculates the RSI (Relative Strength Index) on the provided DataFrame.
    :parameter df: Pandas DataFrame containing historical data.
    :parameter period: Period for RSI calculation.
    :return: DataFrame with the RSI values.
    """
    rsi_calculator = RSI()
    df['RSI'] = rsi_calculator.calculate(df, period)
    return df

def calculate_sma(df, period=20):
    """
    Calculates the SMA (Simple Moving Average) on the provided DataFrame.
    :parameter df: Pandas DataFrame containing historical data.
    :parameter period: Period for SMA calculation.
    :return: DataFrame with the SMA values.
    """
    sma_calculator = SMA()
    df['SMA'] = sma_calculator.calculate(df, period)
    return df

def simple_moving_average_strategy(df):
    """
    Example strategy using Simple Moving Average (SMA).
    :parameter df: Pandas DataFrame containing historical data.
    :return: DataFrame with the strategy signals.
    """
    # Example logic for a basic SMA crossover strategy
    df['signal'] = 0  # Default signal is 0 (no action)
    df['signal'][df['SMA'] > df['close']] = 1  # Buy signal
    df['signal'][df['SMA'] < df['close']] = -1  # Sell signal
    return df

def apply_strategy(df, indicator_params):
    """
    Applies the specified trading strategy by calculating indicators and generating signals.
    :parameter df: Pandas DataFrame containing historical data.
    :parameter indicator_params: Dictionary containing indicator parameters (e.g., 'rsi_period', 'sma_period').
    :return: DataFrame with applied strategy and signals.
    """
    # Fetch indicator parameters from the input (use defaults if not provided)
    rsi_period = indicator_params.get('rsi_period', 14)
    sma_period = indicator_params.get('sma_period', 20)

    # Calculate indicators using the specified periods
    df = calculate_rsi(df, period=rsi_period)
    df = calculate_sma(df, period=sma_period)

    # Apply the trading strategy logic (e.g., SMA crossover)
    df = simple_moving_average_strategy(df)

    # Example strategy logic: you can expand or modify this to fit more advanced strategies
    return df

from trading.brokers.oanda_client import OandaClient
import pandas as pd
from backend.trading.indicators.macd import MACD
from backend.trading.indicators.rsi import RSI
from backend.api.services.state_machine import StateMachine
from variables import TRADE_INSTRUMENTS, STATE_MACHINE, SWITCHES, SCENARIOS, BT_TYPE

class TradeMachine:
    def __init__(self, oanda_api):
        self.oanda_api = oanda_api
        self.state_machine = StateMachine() if STATE_MACHINE else None
        self.backtesting_enabled = BT_TYPE == 'Strategy'
        self.indicator_switches = SWITCHES
        self.initialize_states()

    def initialize_states(self):
        self.states = {pair: 'red' for pair in TRADE_INSTRUMENTS}

    def update_state(self, instrument, new_state):
        self.states[instrument] = new_state

    def fetch_data(self, instrument, granularity="H1", count=1000):
        data = self.oanda_api.get_historical_data(instrument, granularity, count)
        return pd.DataFrame(data['candles'])

    def process_data(self, df):
        df['time'] = pd.to_datetime(df['time'])
        for col in ['c', 'h', 'l', 'o']:
            df[col] = df['mid'][col].astype(float)
        df.rename(columns={'c': 'close', 'h': 'high', 'l': 'low', 'o': 'open'}, inplace=True)
        return df

    def analyze_pair(self, instrument):
        df = self.process_data(self.fetch_data(instrument))
        scenario = SCENARIOS['LONG'] if self.states[instrument] == 'green' else SCENARIOS['SHORT']

        if self.indicator_switches["RSI"]:
            rsi_calculator = RSI()
            rsi_df = rsi_calculator.calculate(df)
        if self.indicator_switches["MACD"]:
            macd_calculator = MACD()
            macd_df = macd_calculator.calculate(df)
        
        # Example logic for updating states based on indicators
        if rsi_df['rsi'].iloc[-1] < 30:
            self.update_state(instrument, 'green')
        elif 30 <= rsi_df['rsi'].iloc[-1] <= 70:
            self.update_state(instrument, 'yellow')
        else:
            self.update_state(instrument, 'red')

    def run(self):
        self.initialize_states()
        for pair in self.states:
            self.analyze_pair(pair)
            print(f"{pair} state: {self.states[pair]}")

        if self.backtesting_enabled:
            self.run_backtest()

    def run_backtest(self):
        print("Running backtest...")
        for pair in self.states:
            df = self.process_data(self.fetch_data(pair))
            # Example: Use indicator classes for backtesting
            if self.indicator_switches["RSI"]:
                rsi_calculator = RSI()
                rsi_df = rsi_calculator.calculate(df)
            if self.indicator_switches["MACD"]:
                macd_calculator = MACD()
                macd_df = macd_calculator.calculate(df)

# Example usage
if __name__ == "__main__":
    oanda = OandaClient()
    trade_machine = TradeMachine(oanda)
    trade_machine.run()

import unittest
import numpy as np
import pandas as pd
from backend.trading.indicators.rsi import RSI
from backend.trading.indicators.ema import EMA
from backend.trading.indicators.macd import MACD
from backend.trading.indicators.stoch import StochasticOscillator
from backend.trading.indicators.sma import SMA
from backend.trading.indicators.bollinger import BollingerBands
from backend.trading.indicators.ma_crossover import MACrossover
from backend.trading.indicators.adx import ADX
from backend.trading.indicators.aroon import Aroon
from backend.trading.indicators.cci import CCI
from backend.trading.indicators.mfi import MFI
from backend.trading.indicators.obv import OBV
from backend.trading.indicators.vwap import VWAP
from backend.trading.indicators.williams_r import WilliamsR
from backend.trading.indicators.atr import ATR

class TestIndicators(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({
            'high': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
            'low': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            'close': [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5, 18.5, 19.5],
            'volume': [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000]
        })

    def test_rsi_calculation(self):
        df_with_rsi = RSI.calculate(self.df, period=10)
        self.assertIn('rsi', df_with_rsi.columns)
        self.assertEqual(len(df_with_rsi), len(self.df))
        
    def test_ema_calculation(self):
        df_with_ema = EMA.calculate(self.df, period=5)
        self.assertIn('ema', df_with_ema.columns)
        self.assertEqual(len(df_with_ema), len(self.df))

    def test_macd_calculation(self):
        df_with_macd = MACD.calculate(self.df, short_period=3, long_period=6, signal_period=2)
        self.assertIn('macd', df_with_macd.columns)
        self.assertIn('signal', df_with_macd.columns)
        self.assertIn('histogram', df_with_macd.columns)
        self.assertEqual(len(df_with_macd), len(self.df))
    
    def test_stochastic_calculation(self):
        df_with_stoch = StochasticOscillator.calculate(self.df, period=5)
        self.assertIn('stoch', df_with_stoch.columns)
        self.assertEqual(len(df_with_stoch), len(self.df))
        
    def test_sma_calculation(self):
        df_with_sma = SMA.calculate(self.df, period=5)
        self.assertIn('sma', df_with_sma.columns)
        self.assertEqual(len(df_with_sma), len(self.df))
        
    def test_bollinger_bands_calculation(self):
        df_with_bollinger = BollingerBands.calculate(self.df, period=5, std=2)
        self.assertIn('middle_5', df_with_bollinger.columns)
        self.assertIn('upper_5', df_with_bollinger.columns)
        self.assertIn('lower_5', df_with_bollinger.columns)
        self.assertEqual(len(df_with_bollinger), len(self.df))
        
    def test_adx_calculation(self):
        period = 5
        df_with_adx = ADX.calculate(self.df, period=period)
        # Ensure ADX column contains numeric values
        self.assertTrue(df_with_adx['adx'].dtype in [np.float64, np.float32])
        
        # Check if there are NaN values in 'adx'
        self.assertFalse(df_with_adx['adx'].isnull().any(), "ADX calculation contains NaN values")

        # Optionally, check that the ADX column has the expected number of non-NaN values
        self.assertEqual(df_with_adx['adx'].notna().sum(), len(df_with_adx), "ADX calculation does not cover all data rows")

        # Optionally, check if 'adx' has the correct length
        self.assertEqual(len(df_with_adx), len(self.df), "Output DataFrame length mismatch")

        self.assertIn('tr', df_with_adx.columns)
        self.assertIn('tr_smooth', df_with_adx.columns)
        self.assertIn('plus_di', df_with_adx.columns)
        self.assertIn('minus_di', df_with_adx.columns)
        self.assertIn('dx', df_with_adx.columns)
        self.assertIn('adx', df_with_adx.columns)


        # Add more assertions based on expected values
        # Example assertion for checking non-null values
        self.assertFalse(df_with_adx['adx'].isnull().any())
        
    def test_aroon_calculation(self):
        df_with_aroon = Aroon.calculate(self.df, period=5)
        self.assertIn('aroon_up', df_with_aroon.columns)
        self.assertIn('aroon_down', df_with_aroon.columns)
        self.assertEqual(len(df_with_aroon), len(self.df))
        
    def test_cci_calculation(self):
        df_with_cci = CCI.calculate(self.df, period=5)
        self.assertIn('cci', df_with_cci.columns)
        self.assertEqual(len(df_with_cci), len(self.df))
        
    def test_mfi_calculation(self):
        df_with_mfi = MFI.calculate(self.df, period=5)
        self.assertIn('mfi', df_with_mfi.columns)
        self.assertEqual(len(df_with_mfi), len(self.df))
        
    def test_obv_calculation(self):
        df_with_obv = OBV.calculate(self.df)
        self.assertIn('obv', df_with_obv.columns)
        self.assertEqual(len(df_with_obv), len(self.df))
        
    def test_vwap_calculation(self):
        df_with_vwap = VWAP.calculate(self.df)
        self.assertIn('vwap', df_with_vwap.columns)
        self.assertEqual(len(df_with_vwap), len(self.df))
        
    def test_williams_r_calculation(self):
        period = 5
        df_with_williams_r = WilliamsR.calculate(self.df, period)
        self.assertIn('williams_r', df_with_williams_r.columns)
        self.assertEqual(len(df_with_williams_r), len(self.df))

    def test_atr_calculation(self):
        df_with_atr = ATR.calculate(self.df, period=5)
        self.assertIn('atr', df_with_atr.columns)
        self.assertEqual(len(df_with_atr), len(self.df))
        
    def test_moving_average_calculation(self):
        fast_period = 3
        slow_period = 5
        df_with_ma = MACrossover.calculate(self.df, fast_period, slow_period)
        
        self.assertIn('fast_ma', df_with_ma.columns)
        self.assertIn('slow_ma', df_with_ma.columns)
        self.assertIn('crossover', df_with_ma.columns)
        self.assertIn('crossover_signal', df_with_ma.columns)
        
        # Add more assertions based on expected values
        self.assertEqual(df_with_ma['fast_ma'].iloc[fast_period-1], sum(self.df['close'][:fast_period]) / fast_period)
        self.assertEqual(df_with_ma['slow_ma'].iloc[slow_period-1], sum(self.df['close'][:slow_period]) / slow_period)

    # def test_bop_calculation(self):
    #     df_with_bop = calculate_bop(self.df)
    #     self.assertIn('bop', df_with_bop.columns)
    #     self.assertEqual(len(df_with_bop), len(self.df))

    # def test_adl_calculation(self):
    #     df_with_adl = calculate_adl(self.df)
    #     self.assertIn('adl', df_with_adl.columns)
    #     self.assertEqual(len(df_with_adl), len(self.df))
        
    # def test_chaikin_oscillator_calculation(self):
    #     df_with_chaikin = calculate_chaikin_oscillator(self.df)
    #     self.assertIn('chaikin', df_with_chaikin.columns)
    #     self.assertEqual(len(df_with_chaikin), len(self.df))
        
    # def test_cmf_calculation(self):
    #     df_with_cmf = calculate_cmf(self.df, period=5)
    #     self.assertIn('cmf', df_with_cmf.columns)
    #     self.assertEqual(len(df_with_cmf), len(self.df))
        
    # def test_dc_calculation(self):
    #     df_with_dc = calculate_dc(self.df, period=5)
    #     self.assertIn('dc_upper', df_with_dc.columns)
    #     self.assertIn('dc_lower', df_with_dc.columns)
    #     self.assertEqual(len(df_with_dc), len(self.df))
        
    # def test_keltner_channel_calculation(self):
    #     df_with_keltner = calculate_keltner_channel(self.df, period=5)
    #     self.assertIn('kc_upper', df_with_keltner.columns)
    #     self.assertIn('kc_lower', df_with_keltner.columns)
    #     self.assertEqual(len(df_with_keltner), len(self.df))
        
    # def test_mom_calculation(self):
    #     df_with_mom = calculate_mom(self.df, period=5)
    #     self.assertIn('mom', df_with_mom.columns)
    #     self.assertEqual(len(df_with_mom), len(self.df))
        
    # def test_roc_calculation(self):
    #     df_with_roc = calculate_roc(self.df, period=5)
    #     self.assertIn('roc', df_with_roc.columns)
    #     self.assertEqual(len(df_with_roc), len(self.df))
                
    # def test_stc_calculation(self):
    #     df_with_stc = calculate_stc(self.df, period=5)
    #     self.assertIn('stc', df_with_stc.columns)
    #     self.assertIn('signal', df_with_stc.columns)
    #     self.assertEqual(len(df_with_stc), len(self.df))
        
    # def test_trix_calculation(self):
    #     df_with_trix = calculate_trix(self.df, period=5)
    #     self.assertIn('trix', df_with_trix.columns)
    #     self.assertEqual(len(df_with_trix), len(self.df))
        
    # def test_uo_calculation(self):
    #     df_with_uo = calculate_uo(self.df, period1=4, period2=7, period3=14)
    #     self.assertIn('uo', df_with_uo.columns)
    #     self.assertEqual(len(df_with_uo), len(self.df))
        
    # def test_vortex_calculation(self):
    #     df_with_vortex = calculate_vortex(self.df, period=5)
    #     self.assertIn('vortex', df_with_vortex.columns)
    #     self.assertEqual(len(df_with_vortex), len(self.df))
        
    # def test_kama_calculation(self):
    #     df_with_kama = calculate_kama(self.df, period=5)
    #     self.assertIn('kama', df_with_kama.columns)
    #     self.assertEqual(len(df_with_kama), len(self.df))
        
    # def test_zlema_calculation(self):
        # df_with_zlema = calculate_zlema(self.df, period=5)
        # self.assertIn('zlema', df_with_zlema.columns)
        # self.assertEqual(len(df_with_zlema), len(self.df))
        
        
if __name__ == '__main__':
    unittest.main()

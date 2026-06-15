import numpy as np
import pandas as pd
from typing import List, Dict, Any

class IndicatorEngine:
    def __init__(self):
        self.configs = []
        
    def update_config(self, configs: List[Dict[str, Any]]):
        self.configs = configs
        
    def calculate(self, candles: List[Dict[str, float]]) -> Dict[str, Any]:
        if not candles or len(candles) < 2:
            return {}
            
        df = pd.DataFrame(candles)
        df.rename(columns={'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'}, inplace=True)
        # ensure numerical types
        for col in ['open', 'high', 'low', 'close', 'volume', 'ts']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        if 'ts' in df.columns:
            df.index = pd.to_datetime(df['ts'], unit='ms')
        
        results = {}
        
        for config in self.configs:
            if not isinstance(config, dict):
                continue
            if not config.get('enabled', True):
                continue
                
            ind_type = config.get('type')
            ind_id = config.get('id', ind_type)
            
            if ind_type == 'RSI':
                length = config.get('length', 14)
                if len(df) >= length + 1:
                    delta = df['close'].diff()
                    gain = (delta.where(delta > 0, 0)).rolling(window=length).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(window=length).mean()
                    rs = gain / loss
                    rsi = 100 - (100 / (1 + rs))
                    results[ind_id] = rsi.replace({np.nan: None}).tolist()
                        
            elif ind_type == 'MACD':
                fast = config.get('fast_length', 12)
                slow = config.get('slow_length', 26)
                signal = config.get('signal_length', 9)
                if len(df) >= slow:
                    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
                    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
                    macd = ema_fast - ema_slow
                    macd_signal = macd.ewm(span=signal, adjust=False).mean()
                    histogram = macd - macd_signal
                    results[ind_id] = {
                        'macd': macd.replace({np.nan: None}).tolist(),
                        'histogram': histogram.replace({np.nan: None}).tolist(),
                        'signal': macd_signal.replace({np.nan: None}).tolist()
                    }
                        
            elif ind_type == 'BB':
                length = config.get('length', 20)
                stdDev = config.get('stdDev', 2.0)
                if len(df) >= length:
                    mid = df['close'].rolling(window=length).mean()
                    std = df['close'].rolling(window=length).std()
                    upper = mid + (std * stdDev)
                    lower = mid - (std * stdDev)
                    results[ind_id] = {
                        'lower': lower.replace({np.nan: None}).tolist(),
                        'mid': mid.replace({np.nan: None}).tolist(),
                        'upper': upper.replace({np.nan: None}).tolist()
                    }
                        
            elif ind_type == 'CCI':
                length = config.get('length', 20)
                if len(df) >= length:
                    tp = (df['high'] + df['low'] + df['close']) / 3
                    sma = tp.rolling(window=length).mean()
                    mad = tp.rolling(window=length).apply(lambda x: pd.Series(x).mad(), raw=True) if hasattr(pd.Series, 'mad') else tp.rolling(window=length).apply(lambda x: np.abs(x - x.mean()).mean(), raw=True)
                    cci = (tp - sma) / (0.015 * mad)
                    results[ind_id] = cci.replace({np.nan: None}).tolist()
                    
            elif ind_type == 'EMA':
                length = config.get('length', 14)
                if len(df) >= length:
                    ema = df['close'].ewm(span=length, adjust=False).mean()
                    results[ind_id] = ema.replace({np.nan: None}).tolist()
                        
            elif ind_type == 'LSMA':
                length = config.get('length', 25)
                offset = config.get('offset', 0)
                if len(df) >= length:
                    # Linear Regression Curve (Least Squares Moving Average)
                    lsma = np.zeros(len(df))
                    lsma[:] = np.nan
                    x = np.arange(length)
                    x_sum = x.sum()
                    x_sq_sum = (x**2).sum()
                    denominator = length * x_sq_sum - x_sum**2
                    
                    if denominator > 0:
                        src = df['close'].values
                        for i in range(length - 1, len(df)):
                            y = src[i - length + 1 : i + 1]
                            y_sum = y.sum()
                            xy_sum = (x * y).sum()
                            m = (length * xy_sum - x_sum * y_sum) / denominator
                            b = (y_sum - m * x_sum) / length
                            lsma[i] = m * (length - 1 - offset) + b
                            
                    results[ind_id] = [None if np.isnan(v) else float(v) for v in lsma]

            elif ind_type == 'VWAP':
                if len(df) > 0 and 'volume' in df.columns:
                    typical_price = (df['high'] + df['low'] + df['close']) / 3
                    vwap = (typical_price * df['volume']).cumsum() / df['volume'].cumsum()
                    results[ind_id] = vwap.replace({np.nan: None}).tolist()
                    
            elif ind_type == 'VWMA':
                length = config.get('length', 20)
                if len(df) >= length and 'volume' in df.columns:
                    pv_sum = (df['close'] * df['volume']).rolling(window=length).sum()
                    v_sum = df['volume'].rolling(window=length).sum()
                    vwma = pv_sum / v_sum
                    results[ind_id] = vwma.replace({np.nan: None}).tolist()
                    
            elif ind_type == 'TWAP':
                length = config.get('length', 14)
                if len(df) >= length:
                    twap = df['close'].rolling(window=length).mean()
                    results[ind_id] = twap.replace({np.nan: None}).tolist()
                    
            elif ind_type == 'RVOL':
                length = config.get('length', 24)
                if len(df) >= length and 'volume' in df.columns:
                    avg_vol = df['volume'].rolling(window=length).mean()
                    rvol = df['volume'] / avg_vol
                    results[ind_id] = rvol.replace({np.nan: None}).tolist()
                    
            elif ind_type == 'VOLATILITY':
                length = config.get('length', 14)
                if len(df) >= length:
                    # Annualized historical volatility
                    log_ret = np.log(df['close'] / df['close'].shift(1))
                    volatility = log_ret.rolling(window=length).std() * np.sqrt(365 * 24)  # Crypto roughly continuous
                    results[ind_id] = volatility.replace({np.nan: None}).tolist()
                    
            elif ind_type == 'KAMA':
                length = config.get('length', 10)
                fast_ema = config.get('fast_ema', 2)
                slow_ema = config.get('slow_ema', 30)
                if len(df) >= max(length, slow_ema):
                    change = abs(df['close'] - df['close'].shift(length))
                    volatility_sum = abs(df['close'] - df['close'].shift(1)).rolling(window=length).sum()
                    er = change / volatility_sum
                    fast_sc = 2 / (fast_ema + 1)
                    slow_sc = 2 / (slow_ema + 1)
                    sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2
                    
                    # Compute KAMA iteratively (pandas doesn't have a vectorized EMA with dynamic alpha easily)
                    kama = np.zeros(len(df))
                    kama[:] = np.nan
                    
                    # Start calculation after enough data
                    start_idx = max(length, slow_ema)
                    if start_idx < len(df):
                        kama[start_idx-1] = df['close'].iloc[start_idx-1]
                        for i in range(start_idx, len(df)):
                            prev = kama[i-1]
                            curr_sc = sc.iloc[i]
                            if np.isnan(curr_sc): 
                                curr_sc = 0
                            kama[i] = prev + curr_sc * (df['close'].iloc[i] - prev)
                    
                    results[ind_id] = [None if np.isnan(x) else float(x) for x in kama]
            
            elif ind_type == 'SUPERTREND':
                length = config.get('length', 10)
                multiplier = config.get('multiplier', 3.0)
                if len(df) >= length:
                    hl2 = (df['high'] + df['low']) / 2
                    tr1 = df['high'] - df['low']
                    tr2 = (df['high'] - df['close'].shift(1)).abs()
                    tr3 = (df['low'] - df['close'].shift(1)).abs()
                    tr = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
                    atr = tr.ewm(alpha=1/length, adjust=False).mean()
                    
                    basic_ub = hl2 + (multiplier * atr)
                    basic_lb = hl2 - (multiplier * atr)
                    
                    final_ub = pd.Series(0.0, index=df.index)
                    final_lb = pd.Series(0.0, index=df.index)
                    supertrend = pd.Series(0.0, index=df.index)
                    st_direction = pd.Series(1, index=df.index)

                    final_ub.iloc[0] = basic_ub.iloc[0]
                    final_lb.iloc[0] = basic_lb.iloc[0]

                    for i in range(1, len(df)):
                        if basic_ub.iloc[i] < final_ub.iloc[i-1] or df['close'].iloc[i-1] > final_ub.iloc[i-1]:
                            final_ub.iloc[i] = basic_ub.iloc[i]
                        else:
                            final_ub.iloc[i] = final_ub.iloc[i-1]

                        if basic_lb.iloc[i] > final_lb.iloc[i-1] or df['close'].iloc[i-1] < final_lb.iloc[i-1]:
                            final_lb.iloc[i] = basic_lb.iloc[i]
                        else:
                            final_lb.iloc[i] = final_lb.iloc[i-1]

                        if st_direction.iloc[i-1] == 1 and df['close'].iloc[i] <= final_lb.iloc[i]:
                            st_direction.iloc[i] = -1
                        elif st_direction.iloc[i-1] == -1 and df['close'].iloc[i] >= final_ub.iloc[i]:
                            st_direction.iloc[i] = 1
                        else:
                            st_direction.iloc[i] = st_direction.iloc[i-1]

                        if st_direction.iloc[i] == 1:
                            supertrend.iloc[i] = final_lb.iloc[i]
                        else:
                            supertrend.iloc[i] = final_ub.iloc[i]
                            
                    results[ind_id] = {
                        'supertrend': supertrend.replace({np.nan: None}).tolist(),
                        'direction': st_direction.replace({np.nan: None}).tolist()
                    }
            elif ind_type == 'ADX':
                length = config.get('length', 14)
                if len(df) >= length:
                    df['up'] = df['high'] - df['high'].shift(1)
                    df['down'] = df['low'].shift(1) - df['low']
                    df['plus_dm'] = np.where((df['up'] > df['down']) & (df['up'] > 0), df['up'], 0.0)
                    df['minus_dm'] = np.where((df['down'] > df['up']) & (df['down'] > 0), df['down'], 0.0)
                    tr1 = df['high'] - df['low']
                    tr2 = (df['high'] - df['close'].shift(1)).abs()
                    tr3 = (df['low'] - df['close'].shift(1)).abs()
                    tr = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
                    atr = tr.ewm(span=length, adjust=False).mean()
                    plus_di = 100 * (pd.Series(df['plus_dm']).ewm(span=length, adjust=False).mean() / atr)
                    minus_di = 100 * (pd.Series(df['minus_dm']).ewm(span=length, adjust=False).mean() / atr)
                    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
                    adx = dx.ewm(span=length, adjust=False).mean()
                    results[ind_id] = {
                        'adx': adx.replace({np.nan: None}).tolist(),
                        'plus_di': plus_di.replace({np.nan: None}).tolist(),
                        'minus_di': minus_di.replace({np.nan: None}).tolist()
                    }

            elif ind_type == 'ICHIMOKU':
                tenkan_period = config.get('tenkan', 9)
                kijun_period = config.get('kijun', 26)
                senkou_span_b_period = config.get('senkou_b', 52)
                if len(df) >= senkou_span_b_period:
                    tenkan_sen = (df['high'].rolling(window=tenkan_period).max() + df['low'].rolling(window=tenkan_period).min()) / 2
                    kijun_sen = (df['high'].rolling(window=kijun_period).max() + df['low'].rolling(window=kijun_period).min()) / 2
                    senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(kijun_period)
                    senkou_span_b = ((df['high'].rolling(window=senkou_span_b_period).max() + df['low'].rolling(window=senkou_span_b_period).min()) / 2).shift(kijun_period)
                    chikou_span = df['close'].shift(-kijun_period)
                    results[ind_id] = {
                        'tenkan': tenkan_sen.replace({np.nan: None}).tolist(),
                        'kijun': kijun_sen.replace({np.nan: None}).tolist(),
                        'senkou_a': senkou_span_a.replace({np.nan: None}).tolist(),
                        'senkou_b': senkou_span_b.replace({np.nan: None}).tolist(),
                        'chikou': chikou_span.replace({np.nan: None}).tolist()
                    }

            elif ind_type == 'PIVOTS':
                if len(df) >= 2:
                    h = df['high'].iloc[-2]
                    l = df['low'].iloc[-2]
                    c = df['close'].iloc[-2]
                    pp = (h + l + c) / 3.0
                    r1 = (2 * pp) - l
                    s1 = (2 * pp) - h
                    r2 = pp + (h - l)
                    s2 = pp - (h - l)
                    results[ind_id] = { 'pp': pp, 'r1': r1, 's1': s1, 'r2': r2, 's2': s2 }

            elif ind_type == 'NADARAYA_WATSON':
                h = config.get('bandwidth', 8)
                mult = config.get('multiplier', 3.0)
                if len(df) >= h:
                    src = df['close'].values
                    n = len(src)
                    nw_mid = np.zeros(n)
                    nw_mid[:] = np.nan
                    
                    for i in range(max(0, n - 200), n):
                        sum_val = 0.0
                        sum_weight = 0.0
                        for j in range(max(0, i - min(100, i)), i + 1):
                            w = np.exp(-((i - j) ** 2) / (2 * (h ** 2)))
                            sum_val += src[j] * w
                            sum_weight += w
                        if sum_weight > 0:
                            nw_mid[i] = sum_val / sum_weight
                    
                    diff = np.abs(src - nw_mid)
                    mae = pd.Series(diff).rolling(window=h).mean().values
                    
                    upper = nw_mid + mae * mult
                    lower = nw_mid - mae * mult
                    
                    results[ind_id] = {
                        'mid': [None if np.isnan(x) else float(x) for x in nw_mid],
                        'upper': [None if np.isnan(x) else float(x) for x in upper],
                        'lower': [None if np.isnan(x) else float(x) for x in lower],
                    }

            elif ind_type == 'Z_SCORE':
                length = config.get('length', 20)
                if len(df) >= length:
                    mid = df['close'].rolling(window=length).mean()
                    std = df['close'].rolling(window=length).std()
                    z_score = (df['close'] - mid) / std
                    results[ind_id] = z_score.replace({np.nan: None}).tolist()

            elif ind_type == 'TRENDLINE':
                length = config.get('length', 20)
                if len(df) >= length:
                    x = np.arange(length)
                    y = df['close'].values[-length:]
                    slope, intercept = np.polyfit(x, y, 1)
                    current_val = slope * (length - 1) + intercept
                    results[ind_id] = {
                        'slope': float(slope),
                        'intercept': float(intercept),
                        'current_val': float(current_val)
                    }

            elif ind_type == 'SR':
                # Auto Support/Resistance using historical local min/max
                window = config.get('window', 20)
                if len(df) >= window:
                    local_max = df['high'].rolling(window=window, center=True).max()
                    local_min = df['low'].rolling(window=window, center=True).min()
                    resistances = df['high'][df['high'] == local_max].dropna().tail(3).tolist()
                    supports = df['low'][df['low'] == local_min].dropna().tail(3).tolist()
                    results[ind_id] = { 'supports': supports, 'resistances': resistances }
                        
        results['price'] = df['close'].iloc[-1] if len(df) > 0 else 0
        return results

    def generate_signals(self, latest_indicators: Dict[str, Any], previous_indicators: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        # Simplified signals - keeping it basic for now since standard TA is mostly visual
        return []

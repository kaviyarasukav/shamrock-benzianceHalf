import pandas as pd
import numpy as np
from typing import List, Dict, Any

def detect_fractals(df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
    """Detects swing highs and swing lows (fractals)."""
    df = df.copy()
    
    # Needs window bars on left and right
    df['pivot_high'] = False
    df['pivot_low'] = False
    
    for i in range(window, len(df) - window):
        is_ph = True
        is_pl = True
        
        center_high = df['high'].iloc[i]
        center_low = df['low'].iloc[i]
        
        for j in range(1, window + 1):
            if df['high'].iloc[i-j] >= center_high or df['high'].iloc[i+j] >= center_high:
                is_ph = False
            if df['low'].iloc[i-j] <= center_low or df['low'].iloc[i+j] <= center_low:
                is_pl = False
                
        df.loc[df.index[i], 'pivot_high'] = is_ph
        df.loc[df.index[i], 'pivot_low'] = is_pl
        
    return df

class SMCEngine:
    def __init__(self, ob_lookback: int = 50):
        self.ob_lookback = ob_lookback
        self.order_blocks = []

    def _generate_id(self, prefix: str, ts: int) -> str:
        return f"{prefix}_{ts}"

    def update(self, candles: List[Dict[str, float]], current_price: float) -> bool:
        if not candles or len(candles) < 3:
            return False

        changed = False
        # Keep state updated, but check for mitigation on tick
        if self.check_mitigation(current_price):
            changed = True
        
        # Calculate FVGs and OBs
        df = pd.DataFrame(candles)
        df.rename(columns={'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'}, inplace=True)
        # Ensure correct types
        for col in ['open', 'high', 'low', 'close', 'volume', 'ts']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if self._scan_structure_and_obs(df):
            changed = True
            
        return changed

    def _scan_structure_and_obs(self, df: pd.DataFrame) -> bool:
        # To avoid heavy computation on every tick, we just do a small rolling window or use tail
        window = 5
        if len(df) < window * 2 + 1: return False
        
        changed = False
        recent_df = df.tail(100).copy()
        recent_df = detect_fractals(recent_df, window=window)
        
        # Track structure breaks
        last_ph_idx = -1
        last_pl_idx = -1
        
        # Fair Value Gaps (FVGs) detection
        # Iterate up to len-2 to allow looking at i, i+1, i+2
        if not hasattr(self, 'fvgs'):
            self.fvgs = []
            
        for i in range(len(recent_df) - 2):
            # Bullish FVG: Low of candle 3 > High of candle 1
            if recent_df['low'].iloc[i + 2] > recent_df['high'].iloc[i] and recent_df['close'].iloc[i+1] > recent_df['open'].iloc[i+1]:
                fvg_id = self._generate_id('FVG_BULL', int(recent_df['ts'].iloc[i+1]))
                if not any(f['id'] == fvg_id for f in self.fvgs):
                    self.fvgs.append({
                        'id': fvg_id,
                        'type': 'FVG',
                        'direction': 'BULLISH',
                        'top_price': float(recent_df['low'].iloc[i + 2]),
                        'bottom_price': float(recent_df['high'].iloc[i]),
                        'status': 'active'
                    })
                    changed = True

            # Bearish FVG: High of candle 3 < Low of candle 1
            if recent_df['high'].iloc[i + 2] < recent_df['low'].iloc[i] and recent_df['close'].iloc[i+1] < recent_df['open'].iloc[i+1]:
                fvg_id = self._generate_id('FVG_BEAR', int(recent_df['ts'].iloc[i+1]))
                if not any(f['id'] == fvg_id for f in self.fvgs):
                    self.fvgs.append({
                        'id': fvg_id,
                        'type': 'FVG',
                        'direction': 'BEARISH',
                        'top_price': float(recent_df['low'].iloc[i]),
                        'bottom_price': float(recent_df['high'].iloc[i + 2]),
                        'status': 'active'
                    })
                    changed = True

        for i in range(len(recent_df)):
            if recent_df['pivot_high'].iloc[i]:
                last_ph_idx = i
            
            if recent_df['pivot_low'].iloc[i]:
                last_pl_idx = i
                
            # Check for BOS/CHoCH (Bullish break of Pivot High)
            if last_ph_idx != -1 and recent_df['close'].iloc[i] > recent_df['high'].iloc[last_ph_idx]:
                # Found an impulsive upward break
                origin_idx = last_pl_idx if last_pl_idx != -1 else max(0, last_ph_idx - 10)
                
                # OTE (Optimal Trade Entry) Calculation 0.618 to 0.786 of the swing block
                swing_low_val = recent_df['low'].iloc[origin_idx]
                swing_high_val = recent_df['high'].iloc[i]
                swing_range = swing_high_val - swing_low_val
                
                ote_top = swing_high_val - (swing_range * 0.618)
                ote_bottom = swing_high_val - (swing_range * 0.786)

                # OB Bullish: last down candle before the up move
                ob_idx = origin_idx
                while ob_idx > 0 and recent_df['close'].iloc[ob_idx] >= recent_df['open'].iloc[ob_idx]:
                    ob_idx -= 1
                    
                obc = recent_df.iloc[ob_idx]
                ob_id = self._generate_id('OB_BULL', int(obc['ts']))
                
                if not any(o['id'] == ob_id for o in self.order_blocks):
                    self.order_blocks.append({
                        'id': ob_id,
                        'type': 'OB',
                        'direction': 'BULLISH',
                        'start_time': int(recent_df['ts'].iloc[origin_idx]),
                        'end_time': None,
                        'top_price': float(max(obc['open'], obc['close'])),
                        'bottom_price': float(obc['low']),
                        'ote_top': float(ote_top),
                        'ote_bottom': float(ote_bottom),
                        'status': 'active'
                    })
                    changed = True
                
                # Avoid re-triggering the same break
                last_ph_idx = -1 
                
            # Check for BOS/CHoCH (Bearish break of Pivot Low)
            if last_pl_idx != -1 and recent_df['close'].iloc[i] < recent_df['low'].iloc[last_pl_idx]:
                # Found an impulsive downward break
                origin_idx = last_ph_idx if last_ph_idx != -1 else max(0, last_pl_idx - 10)
                
                # OTE (Optimal Trade Entry) Calculation 0.618 to 0.786 of the swing block
                swing_high_val = recent_df['high'].iloc[origin_idx]
                swing_low_val = recent_df['low'].iloc[i]
                swing_range = swing_high_val - swing_low_val
                
                ote_bottom = swing_low_val + (swing_range * 0.618)
                ote_top = swing_low_val + (swing_range * 0.786)

                # OB Bearish: last up candle before the down move
                ob_idx = origin_idx
                while ob_idx > 0 and recent_df['close'].iloc[ob_idx] <= recent_df['open'].iloc[ob_idx]:
                    ob_idx -= 1
                    
                obc = recent_df.iloc[ob_idx]
                ob_id = self._generate_id('OB_BEAR', int(obc['ts']))
                
                if not any(o['id'] == ob_id for o in self.order_blocks):
                    self.order_blocks.append({
                        'id': ob_id,
                        'type': 'OB',
                        'direction': 'BEARISH',
                        'start_time': int(recent_df['ts'].iloc[origin_idx]),
                        'end_time': None,
                        'top_price': float(obc['high']),
                        'bottom_price': float(min(obc['open'], obc['close'])),
                        'ote_top': float(ote_top),
                        'ote_bottom': float(ote_bottom),
                        'status': 'active'
                    })
                    changed = True
                
                # Avoid re-triggering the same break
                last_pl_idx = -1
        
        # Enforce max length constraint
        if len(self.order_blocks) > self.ob_lookback:
            self.order_blocks = self.order_blocks[-self.ob_lookback:]
            changed = True
            
        if hasattr(self, 'fvgs') and len(self.fvgs) > self.ob_lookback:
            self.fvgs = self.fvgs[-self.ob_lookback:]
            changed = True
                    
        return changed

    def check_mitigation(self, current_price: float, current_ts: int = None) -> bool:
        changed = False
        for o in self.order_blocks:
            if o['status'] == 'active':
                if o['direction'] == 'BEARISH' and current_price >= o['top_price']:
                    o['status'] = 'mitigated'
                    if current_ts is not None: o['end_time'] = current_ts
                    changed = True
                elif o['direction'] == 'BULLISH' and current_price <= o['bottom_price']:
                    o['status'] = 'mitigated'
                    if current_ts is not None: o['end_time'] = current_ts
                    changed = True
        return changed

    def get_state(self):
        # Return all for now
        return {
            "order_blocks": self.order_blocks,
            "fvgs": self.fvgs if hasattr(self, 'fvgs') else []
        }

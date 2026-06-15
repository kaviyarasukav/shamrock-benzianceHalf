import time
import json
import pandas as pd
from datetime import datetime
from collections import deque

from risk_manager import RiskManager

def analyze_market_structure(df: pd.DataFrame):
    """
    Analyzes market structure for Institutional Trend Follower without using pandas_ta.
    Returns signal ('LONG', 'SHORT', 'HOLD') and current ATR.
    """
    if len(df) < 200:
        return 'HOLD', 0.0
        
    df['EMA_50'] = df['Close'].ewm(span=50, adjust=False).mean()
    df['EMA_200'] = df['Close'].ewm(span=200, adjust=False).mean()
    
    # Calculate ATR manually
    high_low = df['High'] - df['Low']
    high_close = (df['High'] - df['Close'].shift()).abs()
    low_close = (df['Low'] - df['Close'].shift()).abs()
    tr = pd.DataFrame({'hl': high_low, 'hc': high_close, 'lc': low_close}).max(axis=1)
    df['ATR'] = tr.rolling(window=14).mean()
    
    # Calculate VWAP manually
    typical_price = (df['High'] + df['Low'] + df['Close']) / 3
    df['VWAP'] = (typical_price * df['Volume']).cumsum() / df['Volume'].cumsum()
        
    df['VOL_SMA_20'] = df['Volume'].rolling(window=20).mean()
    
    last_row = df.iloc[-1]
    
    if pd.isna(last_row['EMA_200']):
        return 'HOLD', 0.0
        
    close = last_row['Close']
    ema50 = last_row['EMA_50']
    ema200 = last_row['EMA_200']
    vwap = last_row['VWAP']
    vol = last_row['Volume']
    vol_sma = last_row['VOL_SMA_20']
    atr = last_row['ATR']
    
    if close > ema50 > ema200 and close > vwap and vol > 1.5 * vol_sma:
        return 'LONG', atr
    elif close < ema50 < ema200 and close < vwap and vol > 1.5 * vol_sma:
        return 'SHORT', atr
        
    return 'HOLD', atr

class SessionManager:
    """
    Crypto-native Session Profiling.
    Adjusts risk multipliers and volatility expectations based on the time of day and weekend effects.
    """
    def __init__(self):
        self.session = "UNKNOWN"
        self.volatility_multiplier = 1.0

    def update(self):
        now = datetime.utcnow()
        hour = now.hour
        day = now.weekday() # 0 = Monday, 6 = Sunday

        is_weekend = (day >= 5)
        
        # Crypto Sessions (UTC)
        # ASIAN: 00:00 - 08:00
        # LONDON: 08:00 - 13:00
        # NY: 13:00 - 21:00
        # DEAD: 21:00 - 00:00
        
        if 0 <= hour < 8:
            self.session = "ASIAN"
            self.volatility_multiplier = 0.8
        elif 8 <= hour < 13:
            self.session = "LONDON"
            self.volatility_multiplier = 1.2
        elif 13 <= hour < 21:
            self.session = "NY"
            self.volatility_multiplier = 1.5
        else:
            self.session = "DEAD"
            self.volatility_multiplier = 0.6

        # Institutional market makers turn off algorithms on weekends, 
        # causing thinner order books and easily spoofed volatility.
        if is_weekend:
            self.volatility_multiplier *= 0.5

        return {
            "session": self.session,
            "is_weekend": is_weekend,
            "volatility_multiplier": self.volatility_multiplier
        }

class ValidationEngine:
    """
    Flicker Filter. Requires a signal to hold for a Time of Lasting before validation.
    """
    def __init__(self, time_of_lasting: float = 0.5):
        self.time_of_lasting = time_of_lasting
        self.pending_signals = {}
        
    def validate(self, identity: str, direction: str) -> bool:
        if direction == "NONE":
            if identity in self.pending_signals:
                del self.pending_signals[identity]
            return False
            
        now = time.time()
        if identity in self.pending_signals:
            if self.pending_signals[identity]["direction"] == direction:
                if now - self.pending_signals[identity]["start_time"] >= self.time_of_lasting:
                    return True
            else:
                self.pending_signals[identity] = {"direction": direction, "start_time": now}
        else:
            self.pending_signals[identity] = {"direction": direction, "start_time": now}
            
        return False

class VolumeAgent:
    def evaluate(self, state, cvd_slope, cmf, vol_climax, z_score, is_near_val, is_breakout_vah, is_near_vah, is_breakout_val, has_pro_edge, threshold_mult, mean_reversion) -> str:
        if vol_climax:
            return "NONE"
            
        required_z = 3.0 * threshold_mult

        if mean_reversion:
            if z_score > 3.5 or has_pro_edge or is_near_val:
                return "LONG"
            if z_score > 3.5 or has_pro_edge or is_near_vah:
                return "SHORT"
        else:
            required_cvd = 0.0 * threshold_mult
            if (cvd_slope > required_cvd or cmf > 0.25) or has_pro_edge or is_breakout_vah:
                if z_score > required_z or has_pro_edge or is_near_val:
                    return "LONG"
            if (cvd_slope < -required_cvd or cmf < -0.25) or has_pro_edge or is_breakout_val:
                if z_score > required_z or has_pro_edge or is_near_vah:
                    return "SHORT"
        return "NONE"

class MomentumAgent:
    def evaluate(self, divergence, obv_div, custom_bullish_edge, custom_bearish_edge, supertrend_dir=0, rsi=None) -> str:
        is_bullish = False
        is_bearish = False
        
        if divergence == "BULLISH" or obv_div == "BULLISH" or custom_bullish_edge:
            is_bullish = True
            
        if divergence == "BEARISH" or obv_div == "BEARISH" or custom_bearish_edge:
            is_bearish = True
            
        # Supertrend matching acts as primary, RSI as secondary filter
        if supertrend_dir == 1 and (rsi is None or rsi < 70):
            is_bullish = True
            
        if supertrend_dir == -1 and (rsi is None or rsi > 30):
            is_bearish = True
            
        if is_bullish and not is_bearish:
            return "LONG"
        if is_bearish and not is_bullish:
            return "SHORT"
        return "NONE"

class SMCAgent:
    def evaluate(self, is_liq_sweep_long, is_liq_sweep_short, iceberg_support, iceberg_resistance, call_sweep_active, put_sweep_active, bullish_ob_support, bearish_ob_resistance) -> str:
        if is_liq_sweep_long or iceberg_support or call_sweep_active or bullish_ob_support:
            return "LONG"
        if is_liq_sweep_short or iceberg_resistance or put_sweep_active or bearish_ob_resistance:
            return "SHORT"
        return "NONE"

class SignalRouter:
    """
    Phase 3: MTF Compass & Execution Logic
    Routes signals between HTF Compass (Macro Trend) and LTF Sniper (Agents).
    Implements Ruthless Exit based on Z-Score Volume bursts.
    """
    def __init__(self):
        self.htf_bias = "NEUTRAL"
        self.ltf_action = "NONE"
        self.position_active = False
        self.position_direction = "NONE"
        
    def update_htf_bias(self, bias: str):
        self.htf_bias = bias
        
    def set_position_state(self, active: bool, direction: str):
        self.position_active = active
        self.position_direction = direction

    def process_ltf_signal(self, payload: dict, z_score: float, mtf_bias: str, market_state: dict = None):
        if not payload:
            return None
            
        direction = payload.get("direction", "NONE")
        
        # MTF / Macro Gates
        if market_state:
            macro_regime = market_state.get("macro_regime", "CHOP")
            tf = payload.get("timeframe", "1m")
            
            # Inject Volatility and SMC borders into payload for Order Router (Phase 3)
            # Find the most relevant ATR and nearest OB
            if "metadata" not in payload:
                payload["metadata"] = {}
                
            metrics = market_state.get("macro_metrics", {})
            atr_proxy = payload.get("trailing_atr") or metrics.get("atr_proxy", 0)
            payload["metadata"]["atr"] = atr_proxy
            
            # Find nearest 15m order block for stop loss placement
            ob_mtf = market_state.get("order_blocks_mtf", {})
            ob_15m = ob_mtf.get("15m", [])
            payload["metadata"]["ob_borders"] = ob_15m
            
            # Example Condition: DO NOT allow a LONG signal on 1m if 15m SMC is bearish and Macro is Risk-Off
            if tf == "1m" and direction == "LONG":
                # Check 15m SMC blocks
                ob_mtf = market_state.get("order_blocks_mtf", {})
                ob_15m = ob_mtf.get("15m", [])
                
                # If there are active bearish order blocks on 15m
                has_15m_bearish_ob = any(
                    ob.get("direction") == "BEARISH" and ob.get("status") == "active"
                    for ob in (ob_15m if isinstance(ob_15m, list) else [])
                )
                
                if has_15m_bearish_ob and macro_regime in ["RISK_OFF", "SHOCK"]:
                    print(f"[SignalRouter] MTF GATE ACTIVE. Blocked 1m LONG due to 15m Bearish OB + {macro_regime}", flush=True)
                    return None

        # Ruthless Exit Exception
        if self.position_active:
            # If holding LONG, and an opposing bearish setup triggers
            if self.position_direction == "LONG" and direction == "SHORT":
                print(f"[SignalRouter] RUTHLESS EXIT TRIGGERED for LONG position (LTF SHORT signal). Z-Score: {z_score}", flush=True)
                payload["direction"] = "CLOSE_LONG"
                return payload
            # If holding SHORT, and an opposing bullish setup triggers
            if self.position_direction == "SHORT" and direction == "LONG":
                print(f"[SignalRouter] RUTHLESS EXIT TRIGGERED for SHORT position (LTF LONG signal). Z-Score: {z_score}", flush=True)
                payload["direction"] = "CLOSE_SHORT"
                return payload

        # Standard MTF Alignment
        if mtf_bias == "LONG":
            if direction == "LONG":
                return payload
        elif mtf_bias == "SHORT":
            if direction == "SHORT":
                return payload
                
        # If HTF is neither Bullish nor Bearish (or in conflict), do not allow entry.
        return None

class ConfluenceEngine:
    """
    Multi-Dimensional Confluence Rules Engine with Macro Governance.
    Synchronizes disparate data points to evaluate high-conviction trade signals.
    """
    def __init__(self, cooldown_seconds: int = 10, risk_manager: RiskManager = None):
        self.strategy_mode = "HYBRID"
        self.is_shadow = False
        self.conviction_threshold = 75
        self.smc_armed_state = "SCANNING" # 'SCANNING', 'ARMED_LONG', 'ARMED_SHORT'
        self.smc_armed_time = 0
        self.smc_armed_timer_limit = 300 # 5 minutes
        self.market_state = {
            "cvd_history": deque(maxlen=21), # Store last 21 CVD points for CMF/slope
            "price_history": deque(maxlen=20), # Store last 20 Price points for trap divergence
            "vol_history": deque(maxlen=21), # 21 periods for CMF and Climax
            "cvd_diff_history": deque(maxlen=21), # Period deltas for CMF
            "obv": 0.0,
            "obv_history": deque(maxlen=20), # Divergence tracking
            "swing_highs": deque(maxlen=10),
            "swing_lows": deque(maxlen=10),
            "last_z_score": 0.0,
            "active_icebergs": [], # List of {price, timestamp}
            "active_spoofs": [], # List of spoofing events
            "order_blocks": {"bullish": [], "bearish": []},
            "latest_indicators": {},
            "last_call_sweep": None, # {timestamp, usd_value}
            "last_put_sweep": None,
            "current_price": 0.0,
            "ticker": "UNKNOWN",
            "mtf_context": {
                "1m": deque(maxlen=60),
                "5m": deque(maxlen=60),
                "15m": deque(maxlen=60),
                "1h": deque(maxlen=24)
            },
            "macro_regime": "CHOP",
            "macro_killswitch": False,
            "macro_metrics": {}
        }
        self.cooldown_seconds = cooldown_seconds
        self.last_signal_time = 0
        self.last_eval_time = 0
        self.eval_interval = 0.1 # 100ms
        self.strategy_mode = "HYBRID"
        self.session_manager = SessionManager()
        self.risk_manager = risk_manager if risk_manager is not None else RiskManager()
        
        self.volume_agent = VolumeAgent()
        self.momentum_agent = MomentumAgent()
        self.smc_agent = SMCAgent()
        self.validator = ValidationEngine(time_of_lasting=0.5)
        self.signal_router = SignalRouter()
        self.is_shadow = False
        self.execution_rules = []
        self.json_evaluator = __import__('json_evaluator').JsonTreeEvaluator()
        
    def update_execution_rules(self, rules: list):
        self.execution_rules = rules

    def evaluate_custom_rules(self):
        """ Evaluates custom execution rules defined by front-end """
        if not self.execution_rules:
            return None
        
        state = self.market_state
        ticker = state.get("ticker", "UNKNOWN")
        price = state.get("current_price", 0)
        
        for rule in self.execution_rules:
            if not rule.get("enabled", True):
                continue
                
            rule_symbol = rule.get("symbol")
            if rule_symbol and rule_symbol != ticker and rule_symbol != "ALL":
                continue
                
            tf = rule.get("timeframe", "1m")
            indicators = state.get("latest_indicators", {}).get(tf, {})
            conditions = rule.get("conditions", [])
            action = rule.get("action", "LONG")
            logic_op = rule.get("logic_operator", "AND")

            if "rule_tree" in rule:
                context = {
                    "HTF_Trend": "Bullish" if state.get("macro_regime") == "BULL" else ("Bearish" if state.get("macro_regime") == "BEAR" else "Chop"),
                    "macro_regime": state.get("macro_regime", "CHOP"),
                    "vix": state.get("macro_metrics", {}).get("vix", 0),
                    "cvd_delta": state.get("cvd_delta", 0),
                    # Provide all current indicators as flat paths for the tree
                }
                for tf_key, tf_inds in state.get("latest_indicators", {}).items():
                    for ind_key, ind_val in tf_inds.items():
                        # Use flat path e.g. 1m.rsi
                        if isinstance(ind_val, list) and len(ind_val) > 0 and ind_val[-1] is not None:
                            context[f"{tf_key}.{ind_key}"] = ind_val[-1]
                            
                # Check for SMC mitigated or not (dummy mapping for demonstration)
                context["LTF_SMC"] = "FVG_Mitigated" # Mock logic
                
                is_true = self.json_evaluator.evaluate(rule["rule_tree"], context)
                if is_true:
                    return rule
                continue

            # Fallback for old rules format
            if not conditions and "condition" in rule:
                 # Translate old condition format
                 old_c = rule["condition"]
                 conditions.append({
                     "type": "INDICATOR",
                     "key": old_c.get("indicator"),
                     "operator": old_c.get("operator"),
                     "value": old_c.get("value")
                 })
                 
            if not conditions:
                continue
                
            results = []
            for cond in conditions:
                c_type = cond.get("type", "INDICATOR")
                key = cond.get("key")
                operator = cond.get("operator")
                val = cond.get("value", 0)
                cond_tf = cond.get("timeframe", tf)
                indicators_for_cond = state.get("latest_indicators", {}).get(cond_tf, {})
                
                triggered = False
                current_val = None
                prev_val = None
                
                if c_type == "INDICATOR":
                    sub_key = None
                    main_key = key
                    if "." in key:
                        main_key, sub_key = key.split(".", 1)
                        
                    if main_key in indicators_for_cond:
                        arr = indicators_for_cond[main_key]
                        if isinstance(arr, dict) and sub_key:
                            arr = arr.get(sub_key, [])
                        elif isinstance(arr, dict) and not sub_key:
                            # Default fallbacks if no subkey provided
                            if "histogram" in arr:
                                arr = arr.get("histogram", [])
                            elif "direction" in arr:
                                arr = arr.get("direction", [])
                            else:
                                arr = []
                                
                        if isinstance(arr, list) and len(arr) > 0 and arr[-1] is not None:
                            current_val = arr[-1]
                            if len(arr) > 1:
                                prev_val = arr[-2]
                elif c_type == "MACRO":
                    if key == "regime":
                        regime_str = state.get("macro_regime", "CHOP")
                        current_val = 1 if regime_str == "BULL" else (-1 if regime_str == "BEAR" else 0)
                    elif key == "vix":
                        current_val = state.get("macro_metrics", {}).get("vix")
                    elif key == "dxy":
                        current_val = state.get("macro_metrics", {}).get("dxy_correlation")
                elif c_type == "VOLUME":
                    if key == "cvd":
                        current_val = state.get("cvd_delta", 0)
                    # For walls we could check orderbook but keeping it simple for now
                    
                if current_val is not None:
                    if operator == "GREATER_THAN" and current_val > val:
                        triggered = True
                    elif operator == "LESS_THAN" and current_val < val:
                        triggered = True
                    elif operator == "EQUALS" and current_val == val:
                        triggered = True
                    elif operator == "CROSS_ABOVE" and prev_val is not None and prev_val <= val and current_val > val:
                        triggered = True
                    elif operator == "CROSS_BELOW" and prev_val is not None and prev_val >= val and current_val < val:
                        triggered = True
                
                results.append(triggered)
                
            if len(results) > 0:
                if logic_op == "OR" and any(results):
                    return rule
                elif logic_op == "AND" and all(results):
                    return rule
                    
        return None

    def update_balance(self, max_balance: float):
        if self.risk_manager:
            self.risk_manager.update_account_size(max_balance)

    def update_macro_regime(self, regime: str, killswitch: bool, metrics: dict):
        was_killswitch_active = self.market_state.get("macro_killswitch", False)
        was_shock = self.market_state.get("macro_regime", "") == "SHOCK"
        
        self.market_state["macro_regime"] = regime
        self.market_state["macro_killswitch"] = killswitch
        self.market_state["macro_metrics"] = metrics
        
        if self.risk_manager:
            self.risk_manager.update_regime(regime, killswitch)
            
            # If killswitch or SHOCK JUST turned on, flatten all positions for this ticker
            is_shock = regime == "SHOCK"
            if (killswitch or is_shock) and not (was_killswitch_active or was_shock):
                ticker = self.market_state.get("ticker", "UNKNOWN")
                price = self.market_state.get("current_price", 0)
                
                # Close any active long/short for this symbol
                if self.signal_router.position_active:
                    if self.risk_manager:
                        tids_to_close = [tid for tid, trade in self.risk_manager.active_trades.items() if trade.get("symbol") == ticker and trade.get("direction") == self.signal_router.position_direction]
                        for tid in tids_to_close:
                            pos_size += self.risk_manager.active_trades[tid].get("position_size_usd", 0)
                            self.risk_manager.remove_trade(tid, exit_price=price)
                        
                    sig = {
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                        "ticker": ticker,
                        "direction": "CLOSE_" + self.signal_router.position_direction,
                        "trigger_price": price,
                        "position_size_usd": pos_size,
                        "conditions_met": {"reason": "MARKET_CLOSE_ALL via MACRO_SHOCK"}
                    }
                    self.publish_signal(sig)
        
    def sync_position(self, active: bool, direction: str):
        self.signal_router.set_position_state(active, direction)
        
        # Sync with risk manager for heat tracking and sector tagging
        if self.risk_manager:
            ticker = self.market_state.get("ticker", "UNKNOWN")
            price = self.market_state.get("current_price", 0)
            if active:
                # Approximate trade parameters if not already present
                # In a real scenario, these would come from the execution receipt
                params = self.risk_manager.calculate_trade_parameters(ticker, direction, price)
                if not params:
                    return # Stop loss too wide, but here we're syncing externally so maybe we shouldn't fail but keep minimum parameters
                
                self.risk_manager.add_trade(
                    trade_id=f"{ticker}_{direction}",
                    symbol=ticker,
                    direction=direction,
                    entry_price=price,
                    stop_loss=params["stop_loss"],
                    take_profit=params["take_profit"],
                    break_even_target=params["break_even_target"],
                    risk_amount=params["risk_amount"],
                    position_size_usd=params["position_size_usd"]
                )
            else:
                self.risk_manager.remove_all_trades_for_symbol(ticker, exit_price=price)

    def handle_signal_rejection(self):
        """
        Feedback Loop Trigger: If Node.js fails to route a signal, 
        reset cooldown to allow a retry after a brief pause (2s),
        preventing high-frequency "Signal Storms".
        """
        self.last_signal_time = time.time() - (self.cooldown_seconds - 2)

    def get_leverage_multiplier(self):
        """
        Dynamically 'choke' leverage based on Macro Yield Spikes.
        If Yield Z-Score > 1.5, reduce leverage.
        """
        metrics = self.market_state.get("macro_metrics", {})
        yield_z = metrics.get("yield_z_score", 0.0)
        
        # Base multiplier 1.0 (Full Leverage)
        multiplier = 1.0
        
        # Choke logic
        if yield_z > 2.5:
            multiplier = 0.25 # 75% reduction
        elif yield_z > 2.0:
            multiplier = 0.50 # 50% reduction
        elif yield_z > 1.5:
            multiplier = 0.75 # 25% reduction
            
        return multiplier

    def update_state(self, ticker: str, price: float = None, cvd: float = None, z_score: float = None, iceberg: dict = None, sweep: dict = None, market_profile: dict = None, volume: float = 0.0, candle: dict = None, spoof_events: list = None, indicators: dict = None, order_blocks: dict = None, fvgs: list = None, tf: str = None, market_data: pd.DataFrame = None, book_imbalance: float = None):
        self.market_state["ticker"] = ticker
        
        if book_imbalance is not None:
            self.market_state["last_book_imbalance"] = book_imbalance
        
        if fvgs is not None:
            if not "fvgs" in self.market_state: self.market_state["fvgs"] = {}
            if tf: self.market_state["fvgs"][tf] = fvgs
        
        if market_data is not None:
             self.market_state["market_data"] = market_data

        
        if candle:
            tf = candle.get("tf")
            if tf in self.market_state["mtf_context"]:
                self.market_state["mtf_context"][tf].append(candle)
                
        if spoof_events is not None:
            self.market_state["active_spoofs"] = spoof_events
            
        if indicators is not None and tf is not None:
            if "latest_indicators" not in self.market_state:
                self.market_state["latest_indicators"] = {}
            self.market_state["latest_indicators"][tf] = indicators
            
        if order_blocks is not None and tf is not None:
            if "order_blocks_mtf" not in self.market_state:
                self.market_state["order_blocks_mtf"] = {}
            self.market_state["order_blocks_mtf"][tf] = order_blocks
            
            # Map SMCEngine array order_blocks to legacy dictionary
            if isinstance(order_blocks, list):
                legacy_ob = {"bullish": [], "bearish": []}
                for ob in order_blocks:
                    if ob.get("status") == "active":
                        ob_range = [ob.get("bottom_price", 0), ob.get("top_price", 0)]
                        if ob.get("direction") == "BULLISH":
                            legacy_ob["bullish"].append(ob_range)
                        elif ob.get("direction") == "BEARISH":
                            legacy_ob["bearish"].append(ob_range)
                self.market_state["order_blocks"] = legacy_ob
            else:
                self.market_state["order_blocks"] = order_blocks # legacy support

        last_price = self.market_state["current_price"]
        if price is not None:
            self.market_state["current_price"] = price
            self.risk_manager.update_price(price)
            
            # OBV Logic (Tick based)
            if price > last_price:
                self.market_state["obv"] += volume
            elif price < last_price:
                self.market_state["obv"] -= volume
            
            # Swing Low/High (Basic proxy: if V-shape forms over 3 ticks)
            if len(self.market_state["price_history"]) >= 3:
                p1 = self.market_state["price_history"][-3][1]
                t2, p2 = self.market_state["price_history"][-2]
                p3 = self.market_state["price_history"][-1][1]
                if p2 < p1 and p2 < p3: # V-shape low
                    self.market_state["swing_lows"].append((t2, p2))
                elif p2 > p1 and p2 > p3: # inverted V-shape high
                    self.market_state["swing_highs"].append((t2, p2))
            
        if volume > 0:
            self.market_state["vol_history"].append(volume)
            
        if cvd is not None:
            if len(self.market_state["cvd_history"]) > 0:
                cvd_delta = cvd - self.market_state["cvd_history"][-1][1]
                self.market_state["cvd_diff_history"].append(cvd_delta)
                
            self.market_state["cvd_history"].append((time.time(), cvd))
            self.market_state["price_history"].append((time.time(), self.market_state["current_price"]))
            self.market_state["obv_history"].append((time.time(), self.market_state["obv"]))
        if z_score is not None:
            self.market_state["last_z_score"] = z_score
        if market_profile is not None:
            self.market_state["market_profile"] = market_profile
        if iceberg:
            # Add new iceberg and prune old ones (> 60s)
            now = time.time()
            self.market_state["active_icebergs"].append({
                "price": iceberg["price"],
                "side": iceberg.get("side"),
                "timestamp": now
            })
            self.market_state["active_icebergs"] = [i for i in self.market_state["active_icebergs"] if now - i["timestamp"] < 60]
        if sweep:
            if sweep.get("type") == "GAMMA_EXPOSURE_ALERT":
                self.market_state["active_gamma_exposure"] = {
                    "timestamp": time.time(),
                    "hedge": sweep.get("estimated_hedge")
                }
            elif sweep.get("option_type") == "CALL":
                self.market_state["last_call_sweep"] = {
                    "timestamp": time.time(),
                    "usd_value": sweep.get("usd_value", 0)
                }
            elif sweep.get("option_type") == "PUT":
                self.market_state["last_put_sweep"] = {
                    "timestamp": time.time(),
                    "usd_value": sweep.get("usd_value", 0)
                }

    def get_cvd_slope(self):
        if len(self.market_state["cvd_history"]) < 5:
            return 0.0
        
        # Simple linear regression slope or just delta
        first = self.market_state["cvd_history"][0][1]
        last = self.market_state["cvd_history"][-1][1]
        return last - first

    def get_delta_divergence(self) -> str:
        """
        Detects Order Flow Traps (Bullish/Bearish Divergence) using Price vs Cumulative Volume Delta.
        Compares the oldest half of the rolling window to the newest half.
        """
        if len(self.market_state["cvd_history"]) < 10 or len(self.market_state["price_history"]) < 10:
            return "NONE"
            
        prices = [x[1] for x in self.market_state["price_history"]]
        cvds = [x[1] for x in self.market_state["cvd_history"]]
        
        mid = len(prices) // 2
        min_old_p, min_rec_p = min(prices[:mid]), min(prices[mid:])
        min_old_c, min_rec_c = min(cvds[:mid]), min(cvds[mid:])
        
        max_old_p, max_rec_p = max(prices[:mid]), max(prices[mid:])
        max_old_c, max_rec_c = max(cvds[:mid]), max(cvds[mid:])
        
        # Bullish Divergence (Trap): Price lower low, but CVD higher low (Passive Buying Absorption)
        if min_rec_p < min_old_p and min_rec_c > min_old_c:
            return "BULLISH"
            
        # Bearish Divergence (Trap): Price higher high, but CVD lower high (Passive Selling Absorption)
        if max_rec_p > max_old_p and max_rec_c < max_old_c:
            return "BEARISH"
            
        return "NONE"

    def get_cmf(self) -> float:
        """
        Calculates Tick-Based CMF (Chaikin Money Flow) approximation.
        Formula: Sum(CVD_Delta, 21) / Sum(Volume, 21)
        """
        if len(self.market_state["vol_history"]) < 21:
            return 0.0
        sum_vol = sum(self.market_state["vol_history"])
        if sum_vol == 0:
            return 0.0
        sum_cvd_delta = sum(self.market_state["cvd_diff_history"])
        return sum_cvd_delta / sum_vol

    def get_obv_divergence(self) -> str:
        """
        Detects Price vs OBV Divergence. 
        Highly accurate when combined with Volume Profile.
        """
        if len(self.market_state["obv_history"]) < 10 or len(self.market_state["price_history"]) < 10:
            return "NONE"
            
        prices = [x[1] for x in self.market_state["price_history"]]
        obvs = [x[1] for x in self.market_state["obv_history"]]
        
        mid = len(prices) // 2
        min_old_p, min_rec_p = min(prices[:mid]), min(prices[mid:])
        min_old_o, min_rec_o = min(obvs[:mid]), min(obvs[mid:])
        
        max_old_p, max_rec_p = max(prices[:mid]), max(prices[mid:])
        max_old_o, max_rec_o = max(obvs[:mid]), max(obvs[mid:])
        
        # Bullish Divergence: Price lower low, but OBV higher low (Accumulation)
        if min_rec_p < min_old_p and min_rec_o > min_old_o:
            return "BULLISH"
            
        # Bearish Divergence: Price higher high, but OBV lower high (Distribution)
        if max_rec_p > max_old_p and max_rec_o < max_old_o:
            return "BEARISH"
            
        return "NONE"

    def is_liquidity_sweep(self, direction: str) -> bool:
        """
        SMC Retail Trap detection. Identifies sweeps of Equal Highs (EQH) or Equal Lows (EQL).
        Tolerance: 0.1% buffer.
        """
        price = self.market_state["current_price"]
        tolerance = price * 0.001
        
        if direction == "LONG":
            # Looking for sweeps of EQL
            for swing_item in self.market_state["swing_lows"]:
                eq_low = swing_item[1] if isinstance(swing_item, (tuple, list)) and len(swing_item) == 2 else float(swing_item)
                if abs(price - eq_low) <= tolerance:
                    return True
        else:
            # Looking for sweeps of EQH
            for swing_item in self.market_state["swing_highs"]:
                eq_high = swing_item[1] if isinstance(swing_item, (tuple, list)) and len(swing_item) == 2 else float(swing_item)
                if abs(price - eq_high) <= tolerance:
                    return True
        return False

    def get_mtf_bias(self) -> str:
        """
        Calculates Multi-Timeframe bias.
        Checks if the HTF (1h, 4h) and LTF (15m, 5m) trends are aligned.
        """
        bias_score = 0
        indicators_mtf = self.market_state.get("latest_indicators", {})
        
        # Helper to get trend from indicators
        def get_trend(tf: str):
            ind = indicators_mtf.get(tf, {})
            score = 0
            
            # Supertrend
            st = ind.get("supertrend1")
            if st and "direction" in st and len(st["direction"]) > 0:
                score += st["direction"][-1]
                
            # EMA Crossover
            ema7 = ind.get("ema_7")
            ema25 = ind.get("ema_25")
            if ema7 and ema25 and len(ema7) > 0 and len(ema25) > 0:
                if ema7[-1] is not None and ema25[-1] is not None:
                    if ema7[-1] > ema25[-1]:
                        score += 1
                    else:
                        score -= 1
                        
            # RSI Confirmation
            rsi = ind.get("rsi1")
            if rsi and len(rsi) > 0 and rsi[-1] is not None:
                if rsi[-1] > 50:
                    score += 0.5
                else:
                    score -= 0.5
                    
            return score

        # Evaluate HTF (1h is primary)
        h1_score = get_trend("1h")
        # If 1h lacks indicators, fallback to candles
        if h1_score == 0:
            context = self.market_state["mtf_context"]
            h1_candles = list(context.get("1h", []))
            if len(h1_candles) >= 2:
                if h1_candles[-1]["c"] > h1_candles[-2]["c"]: h1_score = 1
                else: h1_score = -1

        # Evaluate LTF confirmation (15m is secondary)
        m15_score = get_trend("15m")
        if m15_score == 0:
            context = self.market_state["mtf_context"]
            m15_candles = list(context.get("15m", []))
            if len(m15_candles) >= 3:
                avg_15 = sum([c["c"] for c in m15_candles[-3:]]) / 3
                if m15_candles[-1]["c"] > avg_15: m15_score = 1
                else: m15_score = -1

        # Combine HTF and LTF
        if h1_score > 0 and m15_score > 0:
            return "LONG"
        if h1_score < 0 and m15_score < 0:
            return "SHORT"
            
        return "NEUTRAL"

    def get_mtf_conviction(self) -> int:
        score = 50.0
        indicators_mtf = self.market_state.get("latest_indicators", {})
        
        def get_trend_intensity(tf: str):
            ind = indicators_mtf.get(tf, {})
            val = 0
            
            # Use ADX for Trend Strength
            adx_data = ind.get("adx")
            adx_val = adx_data["adx"][-1] if isinstance(adx_data, dict) and len(adx_data.get("adx", [])) > 0 else 0
            # If ADX > 25, trend is strong. Increase the weight of the signal.
            adx_multiplier = 1.5 if adx_val > 25 else (0.5 if adx_val < 20 else 1.0)
            
            st = ind.get("supertrend1")
            if st and "direction" in st and len(st["direction"]) > 0:
                val += (st["direction"][-1] * 20) * adx_multiplier
                
            ema7 = ind.get("ema_7")
            ema25 = ind.get("ema_25")
            if ema7 and ema25 and len(ema7) > 0 and len(ema25) > 0:
                if ema7[-1] is not None and ema25[-1] is not None:
                    if ema7[-1] > ema25[-1]: val += 15 * adx_multiplier
                    else: val -= 15 * adx_multiplier
                        
            rsi = ind.get("rsi1")
            if rsi and len(rsi) > 0 and rsi[-1] is not None:
                if rsi[-1] > 60: val += 15
                elif rsi[-1] < 40: val -= 15
            return val
            
        h1_intensity = get_trend_intensity("1h")
        m15_intensity = get_trend_intensity("15m")
        
        total_intensity = h1_intensity + m15_intensity
        
        # High volume/Z-score boosts conviction
        z_score = self.market_state.get("last_z_score", 0.0)
        cvd_slope = self.get_cvd_slope()
        
        bonus = 0
        if z_score > 3.0: bonus += 15
        elif z_score > 2.0: bonus += 10
        if abs(cvd_slope) > 0.005: bonus += 10
        
        # Determine direction of bonus
        if total_intensity < 0:
            final_score = 50 + (total_intensity / 2.0) - bonus
        else:
            final_score = 50 + (total_intensity / 2.0) + bonus
            
        return min(max(int(final_score), 0), 100)

    def is_volume_climax(self) -> bool:
        """
        Detects a 300% volume spike (>3x Average) indicating trend exhaustion.
        Use as a hard-veto for chasing entries.
        """
        if len(self.market_state["vol_history"]) < 10:
            return False
            
        current_vol = self.market_state["vol_history"][-1]
        historical_vols = list(self.market_state["vol_history"])[:-1]
        if not historical_vols: return False
        
        avg_vol = sum(historical_vols) / len(historical_vols)
        if avg_vol == 0: return False
        
        return current_vol > (3.0 * avg_vol)

    def get_regime_modifiers(self, direction: str):
        """
        Calculates signal weights and threshold multipliers based on the Macro Regime.
        """
        regime = self.market_state.get("macro_regime", "CHOP")
        
        # Default multipliers
        weight = 1.0
        threshold_mult = 1.0
        mean_reversion = False
        
        if regime == "SHOCK" or self.market_state.get("macro_killswitch"):
            # Instead of a full weight=0 halt for LONG, we'll allow mean-reversion
            if direction == "LONG":
                weight = 0.25 # Severely limit size
                threshold_mult = 3.0 # Require massive confirmation
                mean_reversion = True
            else:
                # Shorts are still allowed during SHOCK/KILLSWITCH
                weight = 1.0
                threshold_mult = 0.5
        elif regime in ["RISK_OFF", "CONTRACTION"]:
            if direction == "LONG":
                weight = 0.5  # Divides bullish signals
                threshold_mult = 2.0 # Requires 2x stronger signal
            else:
                weight = 1.5  # Multiplies bearish signals
                threshold_mult = 0.66 # Requires less confirmation
        elif regime == "RISK_ON":
            if direction == "LONG":
                weight = 1.5  # Multiplies bullish signals
                threshold_mult = 0.66
            else:
                weight = 0.5
                threshold_mult = 2.0
        elif regime == "CHOP":
            pass

        # If it's the weekend (multiplier 0.5), we require stronger thresholds to enter
        session_data = self.session_manager.update()
        vol_mod = session_data["volatility_multiplier"]
        
        # In low liquidity (e.g. weekend DEAD zone where vol_mod is 0.3), 
        # threshold_mult goes up, requiring a much larger physical move to trigger a trade
        if vol_mod > 0:
            threshold_mult = threshold_mult / vol_mod

        return {"weight": weight, "threshold_mult": threshold_mult, "mean_reversion": mean_reversion, "session": session_data["session"]}

    def evaluate_playbook_combos(self):
        """
        Part 2: High-Win-Rate Combinations (The Playbook)
        Returns (success_bool, signal_payload_dict)
        """
        state = self.market_state
        ticker = state.get("ticker", "UNKNOWN")
        price = state.get("current_price", 0)
        
        ind_1m = state.get("latest_indicators", {}).get("1m", {})
        
        # Combo 1: The Elite Trend Follower (CCI + OBV + SMA)
        # Entry: SMA Fast > SMA Slow, OBV trending up, CCI pulls below 100 then crosses back above 100.
        sma_fast_1m = ind_1m.get("sma", {}).get("fast", [])
        sma_slow_1m = ind_1m.get("sma", {}).get("slow", [])
        cci_1m = ind_1m.get("cci", [])
        
        if len(sma_fast_1m) > 0 and len(sma_slow_1m) > 0 and isinstance(cci_1m, list) and len(cci_1m) > 1:
            fast_ma = sma_fast_1m[-1]
            slow_ma = sma_slow_1m[-1]
            if fast_ma > slow_ma:
                # Basic OBV check - is it increasing
                obv_hist = list(state.get("obv_history", []))
                if len(obv_hist) > 5 and obv_hist[-1] > obv_hist[-5]:
                    # CCI pulls back below 100, then crosses above 100
                    if cci_1m[-2] < 100 and cci_1m[-1] >= 100:
                        trade_params = self.risk_manager.calculate_trade_parameters(ticker, "LONG", price, is_a_plus_setup=True)
                        if trade_params:
                            return True, {
                                "timestamp": datetime.utcnow().isoformat() + "Z",
                                "ticker": ticker,
                                "direction": "LONG",
                                "trigger_price": price,
                                "position_size_usd": trade_params["position_size_usd"],
                                "stop_loss": trade_params["stop_loss"],
                                "take_profit": trade_params["break_even_target"],
                                "metadata": {"playbook_combo": "Elite Trend Follower"},
                                "conditions_met": {"reason": "COMBO_1_TREND_FOLLOWER"}
                            }
                            
        # Combo 4: Structural Alignment Trend
        adx_1m = ind_1m.get("adx", {})
        adx_val = adx_1m.get("adx", [0])[-1] if isinstance(adx_1m, dict) and len(adx_1m.get("adx", [])) > 0 else 0
        if len(sma_fast_1m) > 1 and len(sma_slow_1m) > 1 and isinstance(cci_1m, list) and len(cci_1m) > 0:
            if sma_fast_1m[-2] <= sma_slow_1m[-2] and sma_fast_1m[-1] > sma_slow_1m[-1]:
                if adx_val > 25 or cci_1m[-1] > 100:
                    trade_params = self.risk_manager.calculate_trade_parameters(ticker, "LONG", price, is_a_plus_setup=False)
                    if trade_params:
                        return True, {
                            "timestamp": datetime.utcnow().isoformat() + "Z",
                            "ticker": ticker,
                            "direction": "LONG",
                            "trigger_price": price,
                            "position_size_usd": trade_params["position_size_usd"],
                            "stop_loss": trade_params["stop_loss"],
                            "take_profit": trade_params["take_profit"], # Run trend using main TP
                            "metadata": {"playbook_combo": "Structural Alignment Trend"},
                            "conditions_met": {"reason": "COMBO_4_STRUCTURAL_TREND_LONG"}
                        }
        
        # Combo 4: Structural Alignment Trend SHORT
        if len(sma_fast_1m) > 1 and len(sma_slow_1m) > 1 and isinstance(cci_1m, list) and len(cci_1m) > 0:
            if sma_fast_1m[-2] >= sma_slow_1m[-2] and sma_fast_1m[-1] < sma_slow_1m[-1]:
                if adx_val > 25 or cci_1m[-1] < -100:
                    trade_params = self.risk_manager.calculate_trade_parameters(ticker, "SHORT", price, is_a_plus_setup=False)
                    if trade_params:
                        return True, {
                            "timestamp": datetime.utcnow().isoformat() + "Z",
                            "ticker": ticker,
                            "direction": "SHORT",
                            "trigger_price": price,
                            "position_size_usd": trade_params["position_size_usd"],
                            "stop_loss": trade_params["stop_loss"],
                            "take_profit": trade_params["take_profit"], # Run trend using main TP
                            "metadata": {"playbook_combo": "Structural Alignment Trend"},
                            "conditions_met": {"reason": "COMBO_4_STRUCTURAL_TREND_SHORT"}
                        }

        # Combo 2: The Statistical Reversal (LSMA + Z-Score + RSI)
        # Entry: Hurst < 0.5 (choppy), Z-score hits -2.5, RSI < 25, Price < LSMA for extra stretch.
        hurst = state.get("macro_metrics", {}).get("hurst", 0.5)
        z_score = state.get("last_z_score", 0.0)
        rsi_1m = ind_1m.get("rsi", [])
        lsma_25 = ind_1m.get("lsma25", [])
        
        if isinstance(rsi_1m, dict):
             rsi_val = rsi_1m.get("rsi", [50])[-1] if len(rsi_1m.get("rsi", [])) > 0 else 50
        elif isinstance(rsi_1m, list) and len(rsi_1m) > 0:
             rsi_val = rsi_1m[-1]
        else:
             rsi_val = 50
             
        lsma_val = lsma_25[-1] if isinstance(lsma_25, list) and len(lsma_25) > 0 else price
             
        if hurst < 0.5 and z_score <= -2.5 and rsi_val < 25 and price < lsma_val:
             trade_params = self.risk_manager.calculate_trade_parameters(ticker, "LONG", price, is_a_plus_setup=True)
             if trade_params:
                 return True, {
                     "timestamp": datetime.utcnow().isoformat() + "Z",
                     "ticker": ticker,
                     "direction": "LONG",
                     "trigger_price": price,
                     "position_size_usd": trade_params["position_size_usd"],
                     "stop_loss": trade_params["stop_loss"],
                     "take_profit": trade_params["break_even_target"],
                     "metadata": {"playbook_combo": "Statistical Reversal", "z_score": z_score},
                     "conditions_met": {"reason": "COMBO_2_STATISTICAL_REVERSAL"}
                 }
                 
        if hurst < 0.5 and z_score >= 2.5 and rsi_val > 75 and price > lsma_val:
             trade_params = self.risk_manager.calculate_trade_parameters(ticker, "SHORT", price, is_a_plus_setup=True)
             if trade_params:
                 return True, {
                     "timestamp": datetime.utcnow().isoformat() + "Z",
                     "ticker": ticker,
                     "direction": "SHORT",
                     "trigger_price": price,
                     "position_size_usd": trade_params["position_size_usd"],
                     "stop_loss": trade_params["stop_loss"],
                     "take_profit": trade_params["break_even_target"],
                     "metadata": {"playbook_combo": "Statistical Reversal", "z_score": z_score},
                     "conditions_met": {"reason": "COMBO_2_STATISTICAL_REVERSAL_SHORT"}
                 }

        # Prompt 3 / Combo 5: Institutional Trend Follower
        if "market_data" in state and isinstance(state["market_data"], pd.DataFrame):
            df = state["market_data"]
            if len(df) > 50:
                signal_dir, atr_val = analyze_market_structure(df)
                if signal_dir in ["LONG", "SHORT"]:
                    trade_params = self.risk_manager.calculate_trade_parameters_strict(signal_dir, price, atr_val)
                    if trade_params:
                        return True, {
                            "timestamp": datetime.utcnow().isoformat() + "Z",
                            "ticker": ticker,
                            "direction": signal_dir,
                            "trigger_price": price,
                            "position_size_usd": trade_params["position_size_usd"],
                            "stop_loss": trade_params["stop_loss"],
                            "take_profit": trade_params["take_profit"],
                            "metadata": {"playbook_combo": "Institutional Trend Follower", "atr": atr_val},
                            "conditions_met": {"reason": f"PROMPT_3_INSTITUTIONAL_{signal_dir}"}
                        }

        # Combo 3: The SMC Institutional Trap
        is_liq_sweep_long = False
        dxy_z = state.get("macro_metrics", {}).get("dxy_z_score", 0.0)
        
        if dxy_z < 1.0: # Macro Correlation check (DXY is not pumping)
            if "swing_lows" in state and len(state["swing_lows"]) > 0:
                swing_item = state["swing_lows"][-1]
                if isinstance(swing_item, (tuple, list)) and len(swing_item) == 2:
                    last_low_t, last_low_p = swing_item
                else:
                    last_low_t, last_low_p = 0, float(swing_item)
                if price > last_low_p and price < last_low_p * 1.002: # swept but closed above
                     cvd_hist = list(state.get("cvd_history", []))
                     if len(cvd_hist) > 5 and cvd_hist[-1][1] > cvd_hist[-5][1]: # CVD absorbing
                         # Check HTF Support
                         sr_1h = state.get("latest_indicators", {}).get("1h", {}).get("sr", {})
                         supports_1h = sr_1h.get("supports", [])
                         near_htf_support = False
                         if isinstance(supports_1h, list) and len(supports_1h) > 0:
                             for sup in supports_1h:
                                 if abs(price - sup) / price < 0.005:
                                     near_htf_support = True
                                     break
                         
                         if near_htf_support or len(supports_1h) == 0:
                             is_liq_sweep_long = True
                     
        if is_liq_sweep_long:
             trade_params = self.risk_manager.calculate_trade_parameters(ticker, "LONG", price, is_a_plus_setup=True)
             if trade_params:
                 return True, {
                     "timestamp": datetime.utcnow().isoformat() + "Z",
                     "ticker": ticker,
                     "direction": "LONG",
                     "trigger_price": price,
                     "position_size_usd": trade_params["position_size_usd"],
                     "stop_loss": trade_params["stop_loss"],
                     "take_profit": trade_params["break_even_target"],
                     "metadata": {"playbook_combo": "SMC Institutional Trap"},
                     "conditions_met": {"reason": "COMBO_3_SMC_TRAP"}
                 }
                 
        # Combo 3 SHORT: The SMC Institutional Trap
        is_liq_sweep_short = False
        if dxy_z > -1.0: # Macro Correlation check (DXY is not crashing)
            if "swing_highs" in state and len(state["swing_highs"]) > 0:
                swing_item = state["swing_highs"][-1]
                if isinstance(swing_item, (tuple, list)) and len(swing_item) == 2:
                    last_high_t, last_high_p = swing_item
                else:
                    last_high_t, last_high_p = 0, float(swing_item)
                if price < last_high_p and price > last_high_p * 0.998: # swept but closed below
                     cvd_hist = list(state.get("cvd_history", []))
                     if len(cvd_hist) > 5 and cvd_hist[-1][1] < cvd_hist[-5][1]: # CVD absorbing (sellers)
                         # Check HTF Resistance
                         sr_1h = state.get("latest_indicators", {}).get("1h", {}).get("sr", {})
                         resistances_1h = sr_1h.get("resistances", [])
                         near_htf_res = False
                         if isinstance(resistances_1h, list) and len(resistances_1h) > 0:
                             for res in resistances_1h:
                                 if abs(price - res) / price < 0.005:
                                     near_htf_res = True
                                     break
                         
                         if near_htf_res or len(resistances_1h) == 0:
                             is_liq_sweep_short = True
                     
        if is_liq_sweep_short:
             trade_params = self.risk_manager.calculate_trade_parameters(ticker, "SHORT", price, is_a_plus_setup=True)
             if trade_params:
                 return True, {
                     "timestamp": datetime.utcnow().isoformat() + "Z",
                     "ticker": ticker,
                     "direction": "SHORT",
                     "trigger_price": price,
                     "position_size_usd": trade_params["position_size_usd"],
                     "stop_loss": trade_params["stop_loss"],
                     "take_profit": trade_params["break_even_target"],
                     "metadata": {"playbook_combo": "SMC Institutional Trap"},
                     "conditions_met": {"reason": "COMBO_3_SMC_TRAP_SHORT"}
                 }

        return False, None

    def evaluate_long_condition(self, options_bias: float = 0.0):
        now = time.time()
        state = self.market_state
        
        is_autopilot = getattr(self, "strategy_mode", "HYBRID") == "AUTOPILOT"
        conviction_met = False
        
        if is_autopilot:
            if self.get_mtf_conviction() >= getattr(self, "conviction_threshold", 85):
                conviction_met = True
            else:
                return False, None
                
        # 0. High-Priority Macro Governance Checks
        if state["macro_killswitch"] or state.get("macro_regime") in ["SHOCK", "CONTRACTION", "RISK_OFF"]:
            # Gate 1: If Macro is "SHOCK", "Risk Off", or "CONTRACTION", the Killswitch activates.
            # Block all Long breakout trades. Only allow mean-reversion.
            # We flag mean_reversion mode, or we just drop if not a mean reversion trigger.
            pass # we'll handle this in modifiers
            
        # HTF dictates Trend, LTF dictates Entry. Never take LTF signal that contradicts HTF.
        if self.signal_router.htf_bias == "BEARISH":
            return False, None

        ticker = state.get("ticker", "UNKNOWN")
        if self.risk_manager.is_vetoed(ticker):
            # Daily Drawdown hit limit or Sector Heat Limit reached
            return False, None

        # Get Regime Governor Parameters
        modifiers = self.get_regime_modifiers("LONG")
        if modifiers["weight"] == 0:
            return False, None
            
        mean_reversion_mode = modifiers["mean_reversion"]
        if state["macro_killswitch"] or state.get("macro_regime") in ["SHOCK", "CONTRACTION", "RISK_OFF"]:
            mean_reversion_mode = True # FORCE Mean Reversion only if regime is strict
            
        metrics = state.get("macro_metrics", {})
        hurst = metrics.get("hurst_exponent", 0.5)
        # Regime Filtering: If Hurst > 0.5 (Trending), Mean Reversion signals are mathematically vetoed and blocked entirely
        if mean_reversion_mode and hurst > 0.5:
            return False, None

        # 1. Cooldown check
        if now - self.last_signal_time < self.cooldown_seconds:
            return False, None

        price = state["current_price"]
        
        if price <= 0:
            return False, None
            
        # Lead/Lag Check (Nasdaq & DXY Correlation Drag)
        metrics = state.get("macro_metrics", {})
        ndx_corr = metrics.get("ndx_correlation", 0.0)
        ndx_mom = metrics.get("ndx_momentum", 0.0)
        dxy_corr = metrics.get("dxy_correlation", 0.0)
        dxy_z = metrics.get("dxy_z_score", 0.0)
        pcr = metrics.get("put_call_ratio", 1.0)

        # If NDX is highly correlated and crashing, block longs.
        if ndx_corr > 0.6 and ndx_mom < -1.5:
            return False, None
            
        # If DXY is highly inversely correlated and breaking out, block longs.
        if dxy_corr < -0.5 and dxy_z > 1.5:
            return False, None
            
        # PCR Filter
        if pcr > 1.5:
            return False, None # Extreme bearish options sentiment
            
        # Gamma Exposure (GEX) Execution gate
        if state.get("active_gamma_exposure") and state["active_gamma_exposure"]["hedge"] == "SELL_UNDERLYING":
            if now - state["active_gamma_exposure"]["timestamp"] < 5:
                return False, None # MMs are aggressively selling underlying to hedge

        # Phase 2: Extract Agent Inputs
        vol_climax = self.is_volume_climax()
        cvd_slope = self.get_cvd_slope()
        cmf = self.get_cmf()
        state_z_score = state.get("last_z_score", 0.0)

        divergence = self.get_delta_divergence()
        obv_div = self.get_obv_divergence()
        has_bullish_div = (divergence == "BULLISH")
        is_liq_sweep = self.is_liquidity_sweep("LONG")

        has_pro_edge = has_bullish_div or is_liq_sweep or (obv_div == "BULLISH")

        market_profile = state.get("market_profile", {"poc": 0.0, "vah": 0.0, "val": 0.0})
        is_near_val = (market_profile["val"] > 0) and (abs(price - market_profile["val"]) / price < 0.002)
        is_breakout_vah = (market_profile["vah"] > 0) and (price > market_profile["vah"])

        nearest_iceberg_dist = 1.0
        iceberg_support = False
        for ice in state["active_icebergs"]:
            dist = abs(price - ice["price"]) / price
            if dist < nearest_iceberg_dist: nearest_iceberg_dist = dist
            if ice.get("side") == "HIDDEN_BUYER" and dist <= (0.001 / modifiers["threshold_mult"]): iceberg_support = True
                
        call_sweep_active = False
        if state["last_call_sweep"]:
            if now - state["last_call_sweep"]["timestamp"] <= (5 / modifiers["threshold_mult"]):
                call_sweep_active = True
                
        # Parse Indicators and SMC
        bullish_ob_support = False
        is_near_bullish_ob = False
        proximity_threshold = 0.005 # 0.5% proximity to unmitigated order block
        
        if state.get("order_blocks", {}).get("bullish"):
            for ob in state["order_blocks"]["bullish"]:
                if ob[0] <= price <= ob[1]: # Price is inside OB
                    bullish_ob_support = True
                    is_near_bullish_ob = True
                    break
                else:
                    ob_center = (ob[0] + ob[1]) / 2.0
                    if abs(price - ob_center) / price <= proximity_threshold:
                        is_near_bullish_ob = True

        # Step 3: Proximity Sensor to shift into Armed Mode
        if is_liq_sweep or is_near_bullish_ob:
            self.smc_armed_state = "ARMED_LONG"
            self.smc_armed_time = now

        # Step 4: Confluence Expiration Timer (Disarm if stale)
        is_armed_long = False
        if self.smc_armed_state == "ARMED_LONG":
            if now - self.smc_armed_time > self.smc_armed_timer_limit:
                self.smc_armed_state = "SCANNING"
            else:
                is_armed_long = True

        if not is_armed_long:
            return False, None

        supertrend_dir = 0
        rsi_val = 50.0
        # By default use 1m context, fallback to any available if 1m is missing
        indicators_mtf = state.get("latest_indicators", {})
        indicators = indicators_mtf.get("1m", list(indicators_mtf.values())[0] if indicators_mtf else {})
        if indicators and "rsi1" in indicators:
            rsi_arr = indicators["rsi1"]
            if rsi_arr and len(rsi_arr) > 0 and rsi_arr[-1] is not None:
                rsi_val = rsi_arr[-1]
                
        if indicators and "supertrend1" in indicators:
            if isinstance(indicators["supertrend1"], dict):
                st_dir_arr = indicators["supertrend1"].get("direction")
                if st_dir_arr and len(st_dir_arr) > 0 and st_dir_arr[-1] is not None:
                    supertrend_dir = st_dir_arr[-1]

        # Spoofing Detection
        if state.get("active_spoofs"):
            for spoof in state["active_spoofs"]:
                if spoof.get("side") == "BID" and spoof.get("type") == "SPOOFING_DETECTED" and (price - spoof.get("price", 0)) / price < 0.005:
                    # Cancelled buy wall -> bearish intent -> block longs
                    return False, None

        # Phase 2: Agent Matrix Evaluation
        vol_sig = self.volume_agent.evaluate(state, cvd_slope, cmf, vol_climax, state_z_score, is_near_val, is_breakout_vah, False, False, has_pro_edge, modifiers["threshold_mult"], mean_reversion_mode)
        mom_sig = self.momentum_agent.evaluate(divergence, obv_div, False, False, supertrend_dir, rsi_val)
        smc_sig = self.smc_agent.evaluate(is_liq_sweep, False, iceberg_support, False, call_sweep_active, False, bullish_ob_support, False)

        # Flicker Filter Validation
        vol_valid = self.validator.validate(state["ticker"]+"_VOL_LONG", "LONG" if vol_sig == "LONG" else "NONE")
        mom_valid = self.validator.validate(state["ticker"]+"_MOM_LONG", "LONG" if mom_sig == "LONG" else "NONE")
        smc_valid = self.validator.validate(state["ticker"]+"_SMC_LONG", "LONG" if smc_sig == "LONG" else "NONE")

        agents_agreeing = sum([1 for x in [vol_valid, mom_valid, smc_valid] if x])
        if not conviction_met:
            if agents_agreeing < 2 and not (smc_valid and vol_valid):
                return False, None

        # A+ Setup: All 3 agents agree AND Macro Regime is strongly supportive
        is_a_plus = (agents_agreeing >= 2 and state.get("macro_regime") == "RISK_ON") or conviction_met or options_bias > 0.3

        # All conditions met
        self.last_signal_time = now
        self.smc_armed_state = "SCANNING" # Reset after successful signal generation
        
        leverage_multiplier = (self.get_leverage_multiplier() * modifiers["weight"]) + options_bias
        trade_params = self.risk_manager.calculate_trade_parameters(ticker, "LONG", price, is_a_plus_setup=is_a_plus)
        if not trade_params:
            return False, None
            
        payload = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "symbol": state["ticker"],
            "ticker": state["ticker"],
            "direction": "LONG",
            "trigger_price": price,
            "position_size_usd": trade_params["position_size_usd"],
            "stop_loss": trade_params["stop_loss"],
            "take_profit": trade_params["take_profit"],
            "break_even_target": trade_params["break_even_target"],
            "trailing_atr": trade_params["atr_proxy"],
            "leverage_multiplier": round(leverage_multiplier, 2),
            "conditions_met": {
                "cvd_slope": round(cvd_slope, 4),
                "cmf_accumulation": round(cmf, 4),
                "divergence": divergence,
                "obv_divergence": obv_div,
                "liq_sweep": is_liq_sweep,
                "ndx_momentum": round(ndx_mom, 2),
                "z_score": round(state["last_z_score"], 2),
                "iceberg_distance": round(nearest_iceberg_dist * 100, 4), # in %
                "call_sweep_value": state["last_call_sweep"]["usd_value"] if state.get("last_call_sweep") else 0,
                "session": modifiers.get("session", "UNKNOWN")
            }
        }
        
        return True, payload

    def evaluate_short_condition(self, options_bias: float = 0.0):
        """
        Companion to evaluate_long_condition.
        Evaluates bearish setups, using inverted checks and 'SHORT' regime modifiers.
        """
        now = time.time()
        state = self.market_state
        
        is_autopilot = getattr(self, "strategy_mode", "HYBRID") == "AUTOPILOT"
        conviction_met = False
        
        if is_autopilot:
            if self.get_mtf_conviction() <= (100 - getattr(self, "conviction_threshold", 85)):
                conviction_met = True
            else:
                return False, None
        
        if state["macro_killswitch"] or state.get("macro_regime") == "SHOCK":
            return False, None
            
        if self.signal_router.htf_bias == "BULLISH":
            return False, None

        ticker = state.get("ticker", "UNKNOWN")
        if self.risk_manager.is_vetoed(ticker):
            return False, None

        modifiers = self.get_regime_modifiers("SHORT")
        if modifiers["weight"] == 0:
            return False, None
            
        mean_reversion_mode = modifiers["mean_reversion"]
        metrics = state.get("macro_metrics", {})
        hurst = metrics.get("hurst_exponent", 0.5)
        # Regime Filtering: If Hurst > 0.5 (Trending), Mean Reversion signals are mathematically vetoed
        if mean_reversion_mode and hurst > 0.5:
            return False, None

        if now - self.last_signal_time < self.cooldown_seconds:
            return False, None

        price = state["current_price"]
        if price <= 0:
            return False, None
            
        metrics = state.get("macro_metrics", {})
        ndx_corr = metrics.get("ndx_correlation", 0.0)
        ndx_mom = metrics.get("ndx_momentum", 0.0)
        dxy_corr = metrics.get("dxy_correlation", 0.0)
        dxy_z = metrics.get("dxy_z_score", 0.0)
        pcr = metrics.get("put_call_ratio", 1.0)

        # If NDX is highly correlated and pumping, block shorts.
        if ndx_corr > 0.6 and ndx_mom > 1.5:
            return False, None

        # If DXY is highly inversely correlated and crashing, block shorts.
        if dxy_corr < -0.5 and dxy_z < -1.5:
            return False, None
            
        # PCR Filter 
        if pcr < 0.5:
            return False, None # Extreme bullish options sentiment
            
        # Gamma Exposure (GEX) Execution gate
        if state.get("active_gamma_exposure") and state["active_gamma_exposure"]["hedge"] == "BUY_UNDERLYING":
            if now - state["active_gamma_exposure"]["timestamp"] < 5:
                return False, None # MMs are aggressively buying underlying to hedge

        # Phase 2: Extract Agent Inputs
        vol_climax = self.is_volume_climax()
        cvd_slope = self.get_cvd_slope()
        cmf = self.get_cmf()
        state_z_score = state.get("last_z_score", 0.0)

        divergence = self.get_delta_divergence()
        obv_div = self.get_obv_divergence()
        has_bearish_div = (divergence == "BEARISH")
        is_liq_sweep = self.is_liquidity_sweep("SHORT")

        has_pro_edge = has_bearish_div or is_liq_sweep or (obv_div == "BEARISH")

        market_profile = state.get("market_profile", {"poc": 0.0, "vah": 0.0, "val": 0.0})
        is_near_vah = (market_profile["vah"] > 0) and (abs(price - market_profile["vah"]) / price < 0.002)
        is_breakout_val = (market_profile["val"] > 0) and (price < market_profile["val"])

        nearest_iceberg_dist = 1.0
        iceberg_resistance = False
        for ice in state["active_icebergs"]:
            dist = abs(price - ice["price"]) / price
            if dist < nearest_iceberg_dist: nearest_iceberg_dist = dist
            if ice.get("side") == "HIDDEN_SELLER" and dist <= (0.001 / modifiers["threshold_mult"]): iceberg_resistance = True
                
        put_sweep_active = False
        if state.get("last_put_sweep"):
            if now - state["last_put_sweep"]["timestamp"] <= (5 / modifiers["threshold_mult"]):
                put_sweep_active = True
                
        # Parse Indicators and SMC
        bearish_ob_resistance = False
        is_near_bearish_ob = False
        proximity_threshold = 0.005 # 0.5% proximity to unmitigated order block

        if state.get("order_blocks", {}).get("bearish"):
            for ob in state["order_blocks"]["bearish"]:
                if ob[0] <= price <= ob[1]: # Price is inside Bearish OB
                    bearish_ob_resistance = True
                    is_near_bearish_ob = True
                    break
                else:
                    ob_center = (ob[0] + ob[1]) / 2.0
                    if abs(price - ob_center) / price <= proximity_threshold:
                        is_near_bearish_ob = True

        # Step 3: Proximity Sensor to shift into Armed Mode
        if is_liq_sweep or is_near_bearish_ob:
            self.smc_armed_state = "ARMED_SHORT"
            self.smc_armed_time = now

        # Step 4: Confluence Expiration Timer (Disarm if stale)
        is_armed_short = False
        if self.smc_armed_state == "ARMED_SHORT":
            if now - self.smc_armed_time > self.smc_armed_timer_limit:
                self.smc_armed_state = "SCANNING"
            else:
                is_armed_short = True

        if not is_armed_short:
            return False, None

        supertrend_dir = 0
        rsi_val = 50.0
        # By default use 1m context, fallback to any available if 1m is missing
        indicators_mtf = state.get("latest_indicators", {})
        indicators = indicators_mtf.get("1m", list(indicators_mtf.values())[0] if indicators_mtf else {})
        if indicators and "rsi1" in indicators:
            rsi_arr = indicators["rsi1"]
            if rsi_arr and len(rsi_arr) > 0 and rsi_arr[-1] is not None:
                rsi_val = rsi_arr[-1]
                
        if indicators and "supertrend1" in indicators:
            if isinstance(indicators["supertrend1"], dict):
                st_dir_arr = indicators["supertrend1"].get("direction")
                if st_dir_arr and len(st_dir_arr) > 0 and st_dir_arr[-1] is not None:
                    supertrend_dir = st_dir_arr[-1]

        # Spoofing Detection
        if state.get("active_spoofs"):
            for spoof in state["active_spoofs"]:
                if spoof.get("side") == "ASK" and spoof.get("type") == "SPOOFING_DETECTED" and (price - spoof.get("price", 0)) / price > -0.005:
                    # Cancelled sell wall -> bullish intent -> block shorts
                    return False, None

        # Phase 2: Agent Matrix Evaluation
        vol_sig = self.volume_agent.evaluate(state, cvd_slope, cmf, vol_climax, state_z_score, False, False, is_near_vah, is_breakout_val, has_pro_edge, modifiers["threshold_mult"], modifiers["mean_reversion"])
        mom_sig = self.momentum_agent.evaluate(divergence, obv_div, False, False, supertrend_dir, rsi_val)
        smc_sig = self.smc_agent.evaluate(False, is_liq_sweep, False, iceberg_resistance, False, put_sweep_active, False, bearish_ob_resistance)

        # Flicker Filter Validation
        vol_valid = self.validator.validate(state["ticker"]+"_VOL_SHORT", "SHORT" if vol_sig == "SHORT" else "NONE")
        mom_valid = self.validator.validate(state["ticker"]+"_MOM_SHORT", "SHORT" if mom_sig == "SHORT" else "NONE")
        smc_valid = self.validator.validate(state["ticker"]+"_SMC_SHORT", "SHORT" if smc_sig == "SHORT" else "NONE")

        agents_agreeing = sum([1 for x in [vol_valid, mom_valid, smc_valid] if x])
        if not conviction_met:
            if agents_agreeing < 2 and not (smc_valid and vol_valid):
                return False, None

        # A+ Setup: All 3 agents agree AND Macro Regime is strongly supportive
        is_a_plus = (agents_agreeing >= 2 and state.get("macro_regime") == "RISK_OFF") or conviction_met or options_bias < -0.3

        self.last_signal_time = now
        self.smc_armed_state = "SCANNING" # Reset after successful signal generation
        leverage_multiplier = (self.get_leverage_multiplier() * modifiers["weight"]) - options_bias
        trade_params = self.risk_manager.calculate_trade_parameters(ticker, "SHORT", price, is_a_plus_setup=is_a_plus)
        if not trade_params:
            return False, None
            
        payload = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "symbol": state["ticker"],
            "ticker": state["ticker"],
            "direction": "SHORT",
            "trigger_price": price,
            "position_size_usd": trade_params["position_size_usd"],
            "stop_loss": trade_params["stop_loss"],
            "take_profit": trade_params["take_profit"],
            "break_even_target": trade_params["break_even_target"],
            "trailing_atr": trade_params["atr_proxy"],
            "leverage_multiplier": round(leverage_multiplier, 2),
            "conditions_met": {
                "cvd_slope": round(cvd_slope, 4),
                "cmf_distribution": round(cmf, 4),
                "divergence": divergence,
                "obv_divergence": obv_div,
                "liq_sweep": is_liq_sweep,
                "ndx_momentum": round(ndx_mom, 2) if 'ndx_mom' in locals() else 0.0,
                "z_score": round(state["last_z_score"], 2),
                "iceberg_distance": round(nearest_iceberg_dist * 100, 4),
                "put_sweep_value": state["last_put_sweep"]["usd_value"] if state.get("last_put_sweep") else 0,
                "session": modifiers.get("session", "UNKNOWN")
            }
        }
        
        return True, payload

    def publish_signal(self, payload):
        """
        Publishes the signal to the internal broker (stdout).
        Strictly validates the core payload structure before publishing.
        """
        try:
            # Core validation check
            required_fields = ["timestamp", "ticker", "direction", "trigger_price", "conditions_met"]
            if not all(field in payload for field in required_fields):
                raise ValueError(f"Missing required fields in payload: {payload}")
            
            signal_type = "STRATEGY_SIGNAL" if getattr(self, "strategy_mode", "HYBRID") == "AUTOPILOT" else "ALPHA_SIGNAL"
            
            # Publish JSON string to stdout (our high-speed pipe to Node.js)
            base_payload = {
                "type": signal_type,
                "symbol": payload["ticker"],
                "data": payload
            }
            if getattr(self, 'is_shadow', False):
                base_payload["isShadow"] = True
                
            if hasattr(self, 'publish_callback') and self.publish_callback:
                self.publish_callback(base_payload)
            else:
                print(json.dumps(base_payload), flush=True)
            
            # Also emit an Autopilot log if we executed automatically
            if signal_type == "STRATEGY_SIGNAL":
                log_payload = {
                    "type": "AUTOPILOT_LOG",
                    "symbol": payload["ticker"],
                    "message": f"Autonomous {payload['direction']} Order Dispatched via Confluence Engine. Setup: A+",
                }
                if hasattr(self, 'publish_callback') and self.publish_callback:
                    self.publish_callback(log_payload)
                else:
                    print(json.dumps(log_payload), flush=True)
            
        except Exception as e:
            if hasattr(self, 'publish_callback') and self.publish_callback:
                self.publish_callback({"type": "ERROR", "message": f"Signal publishing failed: {str(e)}"})
            else:
                print(json.dumps({"type": "ERROR", "message": f"Signal publishing failed: {str(e)}"}), flush=True)

    def run_cycle(self):
        """
        Evaluates the engine. Should be called frequently (e.g. every 100ms).
        """
        now = time.time()
        if now - self.last_eval_time >= self.eval_interval:
            self.last_eval_time = now
            
            ticker = self.market_state.get("ticker", "UNKNOWN")
            price = self.market_state["current_price"]
            
            # Asset-Specific HTF Bias Continuous Calculation
            mtf_bias_mapped = {"LONG": "BULLISH", "SHORT": "BEARISH"}.get(self.get_mtf_bias(), "NEUTRAL")
            if self.market_state.get("macro_regime") == "RISK_ON":
                self.signal_router.update_htf_bias("BULLISH" if mtf_bias_mapped != "BEARISH" else "NEUTRAL")
            elif self.market_state.get("macro_regime") == "RISK_OFF":
                self.signal_router.update_htf_bias("BEARISH" if mtf_bias_mapped != "BULLISH" else "NEUTRAL")
            else:
                self.signal_router.update_htf_bias(mtf_bias_mapped)
            
            # 0. Global Veto Check
            if self.risk_manager and self.risk_manager.is_vetoed(ticker):
                return None
                
            # 0.1 Toxicity & Intel Gatekeeping (Spoofing Filter & Imbalance)
            if self.market_state.get("active_spoofs"):
                active_spoofing = any(e.get("type", "") == "SPOOFING_DETECTED" and e.get("severity") in ["HIGH", "MEDIUM"] for e in self.market_state["active_spoofs"])
                if active_spoofing:
                    # Toxic orderflow environment detected. We block execution.
                    # Send a debug log if desired.
                    # print(f"[{ticker}] SIGNAL BLOCKED: Massive Order Book spoofing detected.", flush=True)
                    return None
            
            # Check for excessive book imbalance
            if hasattr(self, "market_state") and getattr(self.market_state, "get", None):
                book_imbalance = self.market_state.get("last_book_imbalance")
                if book_imbalance is not None and abs(book_imbalance) > 0.8:
                    return None


            # Manage Active Trades
            if self.risk_manager:
                # We need to iterate over trade IDs that belong to this symbol
                for tid in list(self.risk_manager.active_trades.keys()):
                    if self.risk_manager.active_trades[tid].get("symbol") == ticker:
                        action = self.risk_manager.manage_active_trade(tid, price)
                        if action:
                            # Publish risk management action (e.g. SL moved, stagnant exit)
                            direction = "CLOSE_LONG" if self.risk_manager.active_trades[tid]["direction"] == "LONG" else "CLOSE_SHORT"
                            if action["event"] in ["STAGNANT_EXIT", "TRAIL_STOP_EXIT", "TAKE_PROFIT_EXIT"]:
                                trade_info = self.risk_manager.active_trades[tid]
                                sig = {
                                    "timestamp": datetime.utcnow().isoformat() + "Z",
                                    "ticker": ticker,
                                    "direction": direction,
                                    "trigger_price": price,
                                    "position_size_usd": trade_info.get("position_size_usd", 0),
                                    "conditions_met": {"reason": action["event"]}
                                }
                                self.publish_signal(sig)
                                self.risk_manager.remove_trade(tid, exit_price=price)
                            elif action["event"] == "SCALE_OUT":
                                trade_info = self.risk_manager.active_trades[tid]
                                orig_usd = trade_info.get("position_size_usd", 0)
                                scale_pct = action.get("scale_pct", 0.50)
                                scaled_usd = orig_usd * scale_pct
                                
                                # Update remaining size in risk manager
                                trade_info["position_size_usd"] = orig_usd - scaled_usd
                                
                                sig = {
                                    "timestamp": datetime.utcnow().isoformat() + "Z",
                                    "ticker": ticker,
                                    "direction": direction, # Close direction
                                    "trigger_price": price,
                                    "metadata": {
                                        "scale_pct": scale_pct, 
                                        "action": "SCALE_OUT", 
                                        "new_sl": action.get("new_sl"),
                                        "original_position_size_usd": orig_usd
                                    },
                                    "conditions_met": {"reason": "SCALE_OUT"}
                                }
                                self.publish_signal(sig)
                            elif action["event"] == "SCALE_IN":
                                # Scale in uses the origin direction (LONG or SHORT)
                                orig_dir = self.risk_manager.active_trades[tid]["direction"]
                                sig = {
                                    "timestamp": datetime.utcnow().isoformat() + "Z",
                                    "ticker": ticker,
                                    "direction": orig_dir,
                                    "trigger_price": price,
                                    "metadata": {
                                        "scale_pct": action.get("scale_pct", 0.50), 
                                        "action": "SCALE_IN", 
                                        "new_sl": action.get("new_sl"),
                                        "position_size_usd": action.get("position_size_usd", 0)
                                    },
                                    "conditions_met": {"reason": "SCALE_IN"}
                                }
                                self.publish_signal(sig)
                            elif action["event"] in ["TRAIL_STOP", "MOVE_TO_BREAKEVEN"]:
                                # Publish an UPDATE_RISK event so Node can update OCO orders
                                base_payload = {
                                    "type": "UPDATE_RISK",
                                    "symbol": ticker,
                                    "data": {
                                        "direction": self.risk_manager.active_trades[tid]["direction"],
                                        "new_sl": action["new_sl"],
                                        "event": action["event"]
                                    }
                                }
                                if getattr(self, 'is_shadow', False):
                                    base_payload["isShadow"] = True
                                if hasattr(self, 'publish_callback') and self.publish_callback:
                                    self.publish_callback(base_payload)
                                else:
                                    print(json.dumps(base_payload), flush=True)

            # Evaluate Options Flow / Gamma Hedging Probability Reweighting
            options_bias = 0
            if "active_gamma_exposure" in self.market_state:
                gamma_alert = self.market_state["active_gamma_exposure"]
                if time.time() - gamma_alert.get("timestamp", 0) < 300: # 5 mins
                    hedge_action = gamma_alert.get("hedge_action", "")
                    if hedge_action == "BUY_UNDERLYING":
                        options_bias = 0.5 # Adds 50% probability weighting to L
                        print(f"[{ticker}] Delta-Neutral Hedging detected: Market Makers forced to BUY. Re-weighting LONG probability.", flush=True)
                    elif hedge_action == "SELL_UNDERLYING":
                        options_bias = -0.5
                        print(f"[{ticker}] Delta-Neutral Hedging detected: Market Makers forced to SELL. Re-weighting SHORT probability.", flush=True)

            # Evaluate Custom Execution Rules
            custom_rule = self.evaluate_custom_rules()
            if custom_rule:
                custom_action = custom_rule.get("action", "LONG")
                # Avoid rapid sequential firing
                if now - self.last_signal_time > self.cooldown_seconds:
                    self.last_signal_time = now
                    sig = {
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                        "ticker": ticker,
                        "direction": custom_action,
                        "trigger_price": price,
                        "metadata": {"rule": custom_rule},
                        "conditions_met": {"reason": "CUSTOM_EXECUTION_RULE"}
                    }
                    if custom_action in ["LONG", "SHORT"]:
                        trade_params = self.risk_manager.calculate_trade_parameters(ticker, custom_action, price, is_a_plus_setup=True)
                        if trade_params:
                            sig["position_size_usd"] = trade_params["position_size_usd"]
                            sig["stop_loss"] = trade_params["stop_loss"]
                            sig["take_profit"] = trade_params["break_even_target"]
                            self.publish_signal(sig)
                            return sig
                    elif custom_action in ["CLOSE_LONG", "CLOSE_SHORT"]:
                        dir_search = "LONG" if custom_action == "CLOSE_LONG" else "SHORT"
                        pos_size = 0
                        if self.risk_manager:
                            tids_to_close = [tid for tid, trade in self.risk_manager.active_trades.items() if trade.get("symbol") == ticker and trade.get("direction") == dir_search]
                            for tid in tids_to_close:
                                pos_size += self.risk_manager.active_trades[tid].get("position_size_usd", 0)
                                self.risk_manager.remove_trade(tid, exit_price=price)
                        if pos_size > 0:
                            sig["position_size_usd"] = pos_size
                            
                        self.publish_signal(sig)
                        return sig

            # Evaluate Combos (High-Win-Rate Playbook)
            success_combo, payload_combo = self.evaluate_playbook_combos()
            if success_combo and payload_combo:
                self.last_signal_time = now
                self.publish_signal(payload_combo)
                return payload_combo
                
            # Evaluate Long Condition (MTF Alignment Filter)
            if mtf_bias_mapped != "BEARISH":
                success_long, payload_long = self.evaluate_long_condition(options_bias)
                if success_long:
                    routed = self.signal_router.process_ltf_signal(payload_long, self.market_state.get("last_z_score", 0.0), self.get_mtf_bias(), self.market_state)
                    if routed:
                        self.publish_signal(routed)
                        return routed

            # Evaluate Short Condition (MTF Alignment Filter)
            if mtf_bias_mapped != "BULLISH":
                success_short, payload_short = self.evaluate_short_condition(options_bias)
                if success_short:
                    routed = self.signal_router.process_ltf_signal(payload_short, self.market_state.get("last_z_score", 0.0), self.get_mtf_bias(), self.market_state)
                    if routed:
                        self.publish_signal(routed)
                        return routed
                
        return None

class MicroGearsEngine(ConfluenceEngine):
    """
    Alternative 1: Pure 'Gears' Execution (Micro-Focused).
    Trades strictly on Gamma and Liquidity. Drops all macro payloads.
    """
    def __init__(self, cooldown_seconds: int = 5, risk_manager = None):
        super().__init__(cooldown_seconds, risk_manager)
    
    def evaluate_long_condition(self, options_bias: float = 0.0):
        now = time.time()
        state = self.market_state
        
        if getattr(self, "strategy_mode", "HYBRID") == "AUTOPILOT":
            if getattr(self, "get_mtf_conviction", lambda: 85)() < getattr(self, "conviction_threshold", 85):
                return False, None
                
        # 0. Macro blind except for the absolute killswitch
        if state.get("macro_killswitch"):
            return False, None
            
        if self.signal_router.htf_bias == "BEARISH":
            return False, None
            
        ticker = state.get("ticker", "UNKNOWN")
        if self.risk_manager.is_vetoed(ticker):
            return False, None

        if now - self.last_signal_time < self.cooldown_seconds:
            return False, None

        price = state["current_price"]
        if price <= 0:
            return False, None
            
        # Flow toxicity / Spoofing Guard
        if state.get("active_spoofs"):
            for spoof in state["active_spoofs"]:
                # If someone is spoofing the BID (fake buy walls), LONG is toxic
                if spoof.get("side") == "BID" and spoof.get("type") == "SPOOFING_DETECTED":
                    return False, None

        cvd_slope = self.get_cvd_slope()
        
        # Pure Momentum/Liquidity Scalp Entry Check
        if cvd_slope <= 0.0:
            return False, None

        # Requires immediate local volume spike
        if state["last_z_score"] <= 2.5:
            return False, None

        # Check DOM Wall Support within tight boundary
        # Long uses resting BIDs (aggressor "SELL")
        iceberg_support = False
        nearest_iceberg_dist = 1.0 # 100%
        for ice in state["active_icebergs"]:
            dist = abs(price - ice["price"]) / price
            if dist < nearest_iceberg_dist:
                nearest_iceberg_dist = dist
            # Very tight iceberg proximity
            if ice.get("side") == "HIDDEN_BUYER" and dist <= 0.0005:
                iceberg_support = True
                break
        
        # Must have EITHER Iceberg support OR massive Call Sweep momentum
        call_sweep_active = False
        if state.get("last_call_sweep"):
            if now - state["last_call_sweep"]["timestamp"] <= 3:
                call_sweep_active = True
                
        if not iceberg_support and not call_sweep_active:
             return False, None

        is_a_plus = (iceberg_support and call_sweep_active and state.get("macro_regime") == "RISK_ON") or options_bias > 0.3
        
        self.last_signal_time = now
        leverage_multiplier = 1.5 + options_bias # aggressive scaling plus bias
        
        trade_params = self.risk_manager.calculate_trade_parameters(ticker, "LONG", price, is_a_plus_setup=is_a_plus)
        if not trade_params:
            return False, None
        session_data = self.session_manager.update()
        
        payload = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "symbol": ticker,
            "ticker": ticker,
            "direction": "LONG",
            "trigger_price": price,
            "position_size_usd": trade_params["position_size_usd"],
            "stop_loss": trade_params["stop_loss"],
            "take_profit": trade_params["take_profit"],
            "break_even_target": trade_params["break_even_target"],
            "trailing_atr": trade_params["atr_proxy"],
            "leverage_multiplier": round(leverage_multiplier, 2),
            "conditions_met": {
                "cvd_slope": round(cvd_slope, 4),
                "z_score": round(state["last_z_score"], 2),
                "iceberg_distance": round(nearest_iceberg_dist * 100, 4), # in %
                "call_sweep_value": state["last_call_sweep"]["usd_value"] if state.get("last_call_sweep") else 0,
                "session": session_data["session"]
            }
        }
        
        return True, payload

    def evaluate_short_condition(self, options_bias: float = 0.0):
        now = time.time()
        state = self.market_state
        
        if getattr(self, "strategy_mode", "HYBRID") == "AUTOPILOT":
            if getattr(self, "get_mtf_conviction", lambda: 85)() > (100 - getattr(self, "conviction_threshold", 85)):
                return False, None
                
        ticker = state.get("ticker", "UNKNOWN")
        
        if state.get("macro_killswitch"):
            return False, None
            
        if self.signal_router.htf_bias == "BULLISH":
            return False, None
            
        if self.risk_manager and self.risk_manager.is_vetoed(ticker):
            return False, None
            
        if now - self.last_signal_time < self.cooldown_seconds:
            return False, None

        price = state["current_price"]
        if price <= 0:
            return False, None
            
        # Flow toxicity / Spoofing Guard
        if state.get("active_spoofs"):
            for spoof in state["active_spoofs"]:
                # If someone is spoofing the ASK (fake sell walls), SHORT is toxic
                if spoof.get("side") == "ASK" and spoof.get("type") == "SPOOFING_DETECTED":
                    return False, None

        cvd_slope = self.get_cvd_slope()
        
        if cvd_slope >= 0.0:
            return False, None

        # Requires immediate local volume spike (Z-score computed on volume magnitude)
        if state["last_z_score"] <= 2.5:
            return False, None

        # Short uses resting ASKs (aggressor "BUY")
        iceberg_resistance = False
        nearest_iceberg_dist = 1.0 # 100%
        for ice in state["active_icebergs"]:
            dist = abs(price - ice["price"]) / price
            if dist < nearest_iceberg_dist:
                nearest_iceberg_dist = dist
            if ice.get("side") == "HIDDEN_SELLER" and dist <= 0.0005:
                iceberg_resistance = True
                break
        
        put_sweep_active = False
        if state.get("last_put_sweep"):
            if now - state["last_put_sweep"]["timestamp"] <= 3:
                put_sweep_active = True
                
        if not iceberg_resistance and not put_sweep_active:
             return False, None

        is_a_plus = (iceberg_resistance and put_sweep_active and state.get("macro_regime") == "RISK_OFF") or options_bias < -0.3
        
        self.last_signal_time = now
        leverage_multiplier = 1.5 - options_bias
        
        trade_params = self.risk_manager.calculate_trade_parameters(ticker, "SHORT", price, is_a_plus_setup=is_a_plus)
        if not trade_params:
            return False, None
        session_data = self.session_manager.update()
        
        payload = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "symbol": ticker,
            "ticker": ticker,
            "direction": "SHORT",
            "trigger_price": price,
            "position_size_usd": trade_params["position_size_usd"],
            "stop_loss": trade_params["stop_loss"],
            "take_profit": trade_params["take_profit"],
            "break_even_target": trade_params["break_even_target"],
            "trailing_atr": trade_params["atr_proxy"],
            "leverage_multiplier": round(leverage_multiplier, 2),
            "conditions_met": {
                "cvd_slope": round(cvd_slope, 4),
                "z_score": round(state["last_z_score"], 2),
                "iceberg_distance": round(nearest_iceberg_dist * 100, 4),
                "call_sweep_value": state["last_put_sweep"]["usd_value"] if state.get("last_put_sweep") else 0,
                "session": session_data["session"]
            }
        }
        
        return True, payload

    def publish_signal(self, payload):
        try:
            required_fields = ["timestamp", "ticker", "direction", "trigger_price", "conditions_met"]
            if not all(field in payload for field in required_fields):
                raise ValueError(f"Missing required fields in payload: {payload}")
            
            signal_type = "STRATEGY_SIGNAL" if getattr(self, "strategy_mode", "HYBRID") == "AUTOPILOT" else "ALPHA_SIGNAL"
            
            # Publish JSON string to stdout with MICRO_GEARS origin
            base_payload = {
                "type": signal_type,
                "symbol": payload["ticker"],
                "strategy_id": "MICRO_GEARS",
                "data": payload
            }
            if getattr(self, 'is_shadow', False):
                base_payload["isShadow"] = True
                
            if hasattr(self, 'publish_callback') and self.publish_callback:
                self.publish_callback(base_payload)
            else:
                print(json.dumps(base_payload), flush=True)
            
            if signal_type == "STRATEGY_SIGNAL":
                log_payload = {
                    "type": "AUTOPILOT_LOG",
                    "symbol": payload["ticker"],
                    "message": f"Autonomous {payload['direction']} Order Dispatched via MICRO GEARS. Setup: A+",
                }
                if hasattr(self, 'publish_callback') and self.publish_callback:
                    self.publish_callback(log_payload)
                else:
                    print(json.dumps(log_payload), flush=True)
            
        except Exception as e:
            if hasattr(self, 'publish_callback') and self.publish_callback:
                self.publish_callback({"type": "ERROR", "message": f"Signal publishing failed: {str(e)}"})
            else:
                print(json.dumps({"type": "ERROR", "message": f"Signal publishing failed: {str(e)}"}), flush=True)

class MacroTrendEngine(ConfluenceEngine):
    """
    Alternative 2: Pure Macro-Trend (Macro-Focused).
    Trades strictly on DXY + r^2 correlation paired with Perp Funding Rates.
    Ignores orderbook, CVD, and short-term Options.
    """
    def __init__(self, cooldown_seconds: int = 3600, risk_manager = None): # 1 hour cooldown, this is a swing trader
        super().__init__(cooldown_seconds, risk_manager)
    
    def evaluate_long_condition(self, options_bias: float = 0.0):
        now = time.time()
        state = self.market_state
        
        if getattr(self, "strategy_mode", "HYBRID") == "AUTOPILOT":
            if getattr(self, "get_mtf_conviction", lambda: 85)() < getattr(self, "conviction_threshold", 85):
                return False, None
        
        if state.get("macro_killswitch"):
            return False, None
            
        ticker = state.get("ticker", "UNKNOWN")
        if self.risk_manager.is_vetoed(ticker):
            return False, None

        if now - self.last_signal_time < self.cooldown_seconds:
            return False, None

        price = state["current_price"]
        if price <= 0:
            return False, None

        metrics = state.get("macro_metrics", {})
        funding_rate = metrics.get("funding_rate", 0.0)
        dxy_correlation = metrics.get("dxy_correlation", 0.0)
        cot_ratio = metrics.get("cot_long_short_ratio", 1.0)
        dxy_z_score = metrics.get("dxy_z_score", 0.0)

        # 1. Negative Funding Rate (Uncrowded / Capitulation)
        if funding_rate >= 0.0:
            return False, None

        # 2. Strong DXY Negative Correlation OR DXY Crashing
        # If DXY is crashing (z-score < -1.5) and correlation is negative or neutral
        # OR if correlation is extremely negative and DXY is just going down
        if dxy_z_score > -1.0 and dxy_correlation > -0.5:
             return False, None

        # 3. Institutional accumulation (COT > 1.2)
        if cot_ratio <= 1.2:
            return False, None

        is_a_plus = (cot_ratio >= 1.5 and dxy_correlation < -0.8 and funding_rate < -0.001) or options_bias > 0.3
        
        self.last_signal_time = now
        leverage_multiplier = 0.5 + options_bias / 2 # Low leverage with slight boost from options
        
        trade_params = self.risk_manager.calculate_trade_parameters(ticker, "LONG", price, is_a_plus_setup=is_a_plus)
        if not trade_params:
            return False, None
        session_data = self.session_manager.update()
        
        payload = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "symbol": ticker,
            "ticker": ticker,
            "direction": "LONG",
            "trigger_price": price,
            "position_size_usd": trade_params["position_size_usd"],
            "stop_loss": trade_params["stop_loss"],
            "take_profit": trade_params["take_profit"],
            "break_even_target": trade_params["break_even_target"],
            "trailing_atr": trade_params["atr_proxy"],
            "leverage_multiplier": round(leverage_multiplier, 2),
            "conditions_met": {
                "cvd_slope": 0.0, # Ignored
                "z_score": 0.0, # Ignored
                "iceberg_distance": 0.0, # Ignored
                "call_sweep_value": 0.0, # Ignored
                "session": session_data["session"],
                "macro_funding_rate": funding_rate,
                "macro_dxy_corr": dxy_correlation,
                "macro_cot": cot_ratio
            }
        }
        
        return True, payload

    def evaluate_short_condition(self, options_bias: float = 0.0):
        now = time.time()
        state = self.market_state
        
        if getattr(self, "strategy_mode", "HYBRID") == "AUTOPILOT":
            if getattr(self, "get_mtf_conviction", lambda: 85)() > (100 - getattr(self, "conviction_threshold", 85)):
                return False, None
        
        if state.get("macro_killswitch"):
            return False, None
            
        ticker = state.get("ticker", "UNKNOWN")
        if self.risk_manager and self.risk_manager.is_vetoed(ticker):
            return False, None

        if now - self.last_signal_time < self.cooldown_seconds:
            return False, None

        price = state["current_price"]
        if price <= 0:
            return False, None

        metrics = state.get("macro_metrics", {})
        funding_rate = metrics.get("funding_rate", 0.0)
        dxy_correlation = metrics.get("dxy_correlation", 0.0)
        cot_ratio = metrics.get("cot_long_short_ratio", 1.0)
        dxy_z_score = metrics.get("dxy_z_score", 0.0)

        # 1. Highly positive funding (Extreme Crowded Longs)
        if funding_rate <= 0.0005: # Needs to be significantly positive to short
            return False, None

        # 2. DXY Spiking + Strong Negative Correlation (Capital flight to USD)
        if dxy_z_score < 1.5:
            return False, None
            
        if dxy_correlation > -0.5:
            return False, None

        # 3. Institutional unloading (COT < 0.8)
        if cot_ratio >= 0.8:
            return False, None

        is_a_plus = (cot_ratio <= 0.5 and dxy_z_score > 2.0 and dxy_correlation < -0.8 and funding_rate > 0.001) or options_bias < -0.3

        self.last_signal_time = now
        leverage_multiplier = 0.5 - options_bias / 2
        
        trade_params = self.risk_manager.calculate_trade_parameters(ticker, "SHORT", price, is_a_plus_setup=is_a_plus)
        if not trade_params:
            return False, None
        session_data = self.session_manager.update()
        
        payload = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "symbol": ticker,
            "ticker": ticker,
            "direction": "SHORT",
            "trigger_price": price,
            "position_size_usd": trade_params["position_size_usd"],
            "stop_loss": trade_params["stop_loss"],
            "take_profit": trade_params["take_profit"],
            "break_even_target": trade_params["break_even_target"],
            "trailing_atr": trade_params["atr_proxy"],
            "leverage_multiplier": round(leverage_multiplier, 2),
            "conditions_met": {
                "cvd_slope": 0.0,
                "z_score": 0.0,
                "iceberg_distance": 0.0,
                "call_sweep_value": 0.0,
                "session": session_data["session"],
                "macro_funding_rate": funding_rate,
                "macro_dxy_corr": dxy_correlation,
                "macro_cot": cot_ratio
            }
        }
        
        return True, payload

    def publish_signal(self, payload):
        try:
            required_fields = ["timestamp", "ticker", "direction", "trigger_price", "conditions_met"]
            if not all(field in payload for field in required_fields):
                raise ValueError(f"Missing required fields in payload: {payload}")
            
            signal_type = "STRATEGY_SIGNAL" if getattr(self, "strategy_mode", "HYBRID") == "AUTOPILOT" else "ALPHA_SIGNAL"
            
            base_payload = {
                "type": signal_type,
                "symbol": payload["ticker"],
                "strategy_id": "MACRO_TREND",
                "data": payload
            }
            if getattr(self, 'is_shadow', False):
                base_payload["isShadow"] = True
                
            if hasattr(self, 'publish_callback') and self.publish_callback:
                self.publish_callback(base_payload)
            else:
                print(json.dumps(base_payload), flush=True)

            if signal_type == "STRATEGY_SIGNAL":
                log_payload = {
                    "type": "AUTOPILOT_LOG",
                    "symbol": payload["ticker"],
                    "message": f"Autonomous {payload['direction']} Order Dispatched via MACRO TREND. Setup: A+",
                }
                if hasattr(self, 'publish_callback') and self.publish_callback:
                    self.publish_callback(log_payload)
                else:
                    print(json.dumps(log_payload), flush=True)
            
        except Exception as e:
            if hasattr(self, 'publish_callback') and self.publish_callback:
                self.publish_callback({"type": "ERROR", "message": f"Signal publishing failed: {str(e)}"})
            else:
                print(json.dumps({"type": "ERROR", "message": f"Signal publishing failed: {str(e)}"}), flush=True)

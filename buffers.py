import numpy as np
import time
import datetime
from collections import deque
from typing import Dict, Any, List, Optional

class OrderBookRingBuffer:
    """
    High-Frequency Ring Buffer for Orderbook Snapshots.
    Uses collections.deque (implemented in C) for O(1) append/pop performance.
    Strictly prevents memory leaks by automatically dropping oldest items when maxlen is reached.
    """
    def __init__(self, max_size: int = 1000):
        # deque with maxlen automatically evicts the oldest item when full
        self.snapshots: deque = deque(maxlen=max_size)

    def append(self, snapshot: Dict[str, Any]):
        """Append a new orderbook snapshot."""
        self.snapshots.append(snapshot)

    def get_latest(self) -> Optional[Dict[str, Any]]:
        """Get the most recent snapshot."""
        if not self.snapshots:
            return None
        return self.snapshots[-1]

    def get_all(self) -> List[Dict[str, Any]]:
        """Return all snapshots in chronological order."""
        return list(self.snapshots)

    def __len__(self):
        return len(self.snapshots)


class TickDataRingBuffer:
    """
    High-Frequency Ring Buffer for Tick Data (Price/Volume).
    Uses pre-allocated NumPy arrays to strictly prevent memory leaks and 
    allow instant vectorized math calculations (e.g., for indicators).
    """
    def __init__(self, capacity: int = 10000):
        self.capacity = capacity
        # Pre-allocate memory to prevent leaks and reallocation overhead
        self.prices = np.zeros(capacity, dtype=np.float64)
        self.volumes = np.zeros(capacity, dtype=np.float64)
        self.ask_volumes = np.zeros(capacity, dtype=np.float64)
        self.bid_volumes = np.zeros(capacity, dtype=np.float64)
        self.timestamps = np.zeros(capacity, dtype=np.int64)
        
        self.index = 0
        self.is_full = False
        
        # O(1) Running Metrics for instant math calculations
        self.running_sum_pv = 0.0
        self.running_sum_v = 0.0
        self.running_sum_v_sq = 0.0
        self.append_count = 0
        self.recalc_interval = 100000 # Recalculate every 100k ticks

    def append(self, price: float, volume: float, timestamp: int, is_buyer_maker: bool = False):
        """Insert new tick data at the current index, wrapping around if necessary."""
        # If overwriting old data, remove its contribution from running sums
        if self.is_full:
            old_p = self.prices[self.index]
            old_v = self.volumes[self.index]
            self.running_sum_pv -= (old_p * old_v)
            self.running_sum_v -= old_v
            self.running_sum_v_sq -= (old_v * old_v)

        # Insert new data
        self.prices[self.index] = price
        self.volumes[self.index] = volume
        self.ask_volumes[self.index] = volume if not is_buyer_maker else 0.0
        self.bid_volumes[self.index] = volume if is_buyer_maker else 0.0
        self.timestamps[self.index] = timestamp
        
        # Add new contribution to running sums
        self.running_sum_pv += (price * volume)
        self.running_sum_v += volume
        self.running_sum_v_sq += (volume * volume)
        
        self.index += 1
        self.append_count += 1
        
        # Periodically recalculate to prevent floating point drift
        if self.append_count >= self.recalc_interval:
            self._recalculate_sums()
            self.append_count = 0

        if self.index >= self.capacity:
            self.index = 0
            self.is_full = True

    def _recalculate_sums(self):
        """Recalculate running sums from the entire buffer to reset floating point drift."""
        if not self.is_full:
            self.running_sum_pv = np.sum(self.prices[:self.index] * self.volumes[:self.index])
            self.running_sum_v = np.sum(self.volumes[:self.index])
            self.running_sum_v_sq = np.sum(self.volumes[:self.index] ** 2)
        else:
            self.running_sum_pv = np.sum(self.prices * self.volumes)
            self.running_sum_v = np.sum(self.volumes)
            self.running_sum_v_sq = np.sum(self.volumes ** 2)

    def get_prices(self) -> np.ndarray:
        """
        Returns the price array in chronological order.
        Note: This creates a copy/concatenation. Use running metrics for O(1) performance where possible.
        """
        if not self.is_full:
            return self.prices[:self.index]
        # Concatenate the older part (from index to end) with the newer part (from 0 to index)
        return np.concatenate((self.prices[self.index:], self.prices[:self.index]))

    def get_volumes(self) -> np.ndarray:
        """Returns the volume array in chronological order."""
        if not self.is_full:
            return self.volumes[:self.index]
        return np.concatenate((self.volumes[self.index:], self.volumes[:self.index]))
        
    def get_ask_volumes(self) -> np.ndarray:
        """Returns the ask_volume array in chronological order."""
        if not self.is_full:
            return self.ask_volumes[:self.index]
        return np.concatenate((self.ask_volumes[self.index:], self.ask_volumes[:self.index]))

    def get_bid_volumes(self) -> np.ndarray:
        """Returns the bid_volume array in chronological order."""
        if not self.is_full:
            return self.bid_volumes[:self.index]
        return np.concatenate((self.bid_volumes[self.index:], self.bid_volumes[:self.index]))
        
    def get_timestamps(self) -> np.ndarray:
        """Returns the timestamp array in chronological order."""
        if not self.is_full:
            return self.timestamps[:self.index]
        return np.concatenate((self.timestamps[self.index:], self.timestamps[:self.index]))

    def get_last_price(self) -> float:
        if self.index == 0 and not self.is_full:
            return 0.0
        idx = self.index - 1 if self.index > 0 else self.capacity - 1
        return float(self.prices[idx])

    def get_vwap(self) -> float:
        """
        O(1) Math Calculation: Volume Weighted Average Price over the buffer.
        Calculated instantly using running sums without O(N) array copying.
        """
        if self.running_sum_v == 0:
            return 0.0
        return self.running_sum_pv / self.running_sum_v

    def get_z_score(self, current_qty: float) -> float:
        """
        Calculate Z-Score of the current quantity against the buffer.
        """
        n = self.capacity if self.is_full else self.index
        if n < 20:
            return 0.0
        
        # O(1) Math: Var = E[X^2] - (E[X])^2
        mean = self.running_sum_v / n
        variance = (self.running_sum_v_sq / n) - (mean ** 2)
        
        # Catch negative variance due to floating point precision errors
        std = np.sqrt(max(0, variance))
        
        if std == 0: return 0.0
        return (current_qty - mean) / std

    def get_market_profile(self, bins: int = 50) -> Dict[str, float]:
        """
        Calculates the Volume Profile (TPO), Point of Control (POC), and Value Area (VAH/VAL).
        Returns a dictionary with POC, VAH, VAL.
        """
        n = self.capacity if self.is_full else self.index
        if n < 50:
            return {"poc": 0.0, "vah": 0.0, "val": 0.0}
            
        active_prices = self.prices[:self.index] if not self.is_full else self.prices
        active_volumes = self.volumes[:self.index] if not self.is_full else self.volumes
        
        if len(active_prices) == 0:
            return {"poc": 0.0, "vah": 0.0, "val": 0.0}
            
        min_p = np.min(active_prices)
        max_p = np.max(active_prices)
        
        if min_p == max_p or np.isnan(min_p) or np.isnan(max_p):
            return {"poc": float(min_p), "vah": float(min_p), "val": float(min_p)}
            
        # numpy histogram with weights is highly optimized
        hist, bin_edges = np.histogram(active_prices, bins=bins, weights=active_volumes)
        
        # Find POC
        poc_idx = int(np.argmax(hist))
        poc = (bin_edges[poc_idx] + bin_edges[poc_idx + 1]) / 2.0
        
        # Find VAH and VAL (70% Value Area)
        total_vol = np.sum(hist)
        target_vol = total_vol * 0.70
        current_vol = hist[poc_idx]
        
        up_idx = poc_idx + 1
        down_idx = poc_idx - 1
        
        val_idx = poc_idx
        vah_idx = poc_idx
        
        while current_vol < target_vol:
            up_vol = hist[up_idx] if up_idx < len(hist) else -1
            down_vol = hist[down_idx] if down_idx >= 0 else -1
            
            if up_vol == -1 and down_vol == -1:
                break
                
            if up_vol > down_vol:
                current_vol += up_vol
                vah_idx = up_idx
                up_idx += 1
            else:
                current_vol += down_vol
                val_idx = down_idx
                down_idx -= 1
                
        vah = (bin_edges[vah_idx] + bin_edges[vah_idx + 1]) / 2.0
        val = (bin_edges[val_idx] + bin_edges[val_idx + 1]) / 2.0
        
        return {"poc": float(poc), "vah": float(max(vah, poc)), "val": float(min(val, poc))}

    def __len__(self):
        return self.capacity if self.is_full else self.index


import datetime

class OptionsAnalyzer:
    """
    Analyzes options flow to detect massive sweeps and estimate dealer gamma exposure.
    """
    def __init__(self):
        self.sweep_threshold_usd = 50000  # $50k for a massive sweep in crypto options
        
    def analyze(self, options_data: dict, current_underlying_price: float) -> list:
        usd_value = options_data.get("usd_value", 0)
        side = options_data.get("side", "")
        opt_type = options_data.get("type", "")
        strike = options_data.get("strike", 0)
        expiry = options_data.get("expiry", "")
        symbol = options_data.get("symbol", "")
        
        results = []
        
        # Check for short expiration (<= 7 days)
        is_short_expiry = False
        if expiry and len(expiry) == 6:
            try:
                # expiry format: YYMMDD
                exp_date = datetime.datetime.strptime(expiry, "%y%m%d")
                # Use a more robust now() and handle same-day expirations
                now = datetime.datetime.utcnow()
                # If it expires today, days_to_expiry might be 0 or -1 depending on time
                # We consider anything expiring within the next 7 days as "short"
                diff = exp_date - now
                days_to_expiry = diff.days + (1 if diff.seconds > 0 else 0)
                
                if -1 <= days_to_expiry <= 7:
                    is_short_expiry = True
            except:
                pass
        
        # 1. Detect Massive Aggressive Sweeps (executed at Ask with short expiry)
        if usd_value >= self.sweep_threshold_usd and side == "BUY" and is_short_expiry:
            results.append({
                "type": "OPTIONS_SWEEP_DETECTED",
                "symbol": symbol,
                "option_type": opt_type,
                "strike": strike,
                "expiry": expiry,
                "usd_value": usd_value,
                "message": f"Massive {opt_type} Sweep: ${usd_value:,.0f}"
            })
            
        # 2. Estimate Dealer Gamma Exposure
        if current_underlying_price > 0 and usd_value >= 25000:
            moneyness = "ATM"
            # Define ITM/OTM/ATM bands
            itm_threshold = 0.95
            otm_threshold = 1.05
            
            if opt_type == "CALL":
                if strike < current_underlying_price * itm_threshold:
                    moneyness = "DEEP_ITM"
                elif strike > current_underlying_price * otm_threshold:
                    moneyness = "OTM"
            else: # PUT
                if strike > current_underlying_price * otm_threshold:
                    moneyness = "DEEP_ITM"
                elif strike < current_underlying_price * itm_threshold:
                    moneyness = "OTM"
                    
            # Gamma is highest ATM and ITM for dealers
            if moneyness in ["DEEP_ITM", "ATM"]:
                # Customer BUY CALL -> MM Short Call (Short Delta) -> MM BUYS Underlying to hedge
                # Customer BUY PUT -> MM Short Put (Long Delta) -> MM SELLS Underlying to hedge
                if side == "BUY":
                    hedge_action = "BUY_UNDERLYING" if opt_type == "CALL" else "SELL_UNDERLYING"
                else:
                    hedge_action = "SELL_UNDERLYING" if opt_type == "CALL" else "BUY_UNDERLYING"
                    
                results.append({
                    "type": "GAMMA_EXPOSURE_ALERT",
                    "symbol": symbol,
                    "option_type": opt_type,
                    "moneyness": moneyness,
                    "estimated_hedge": hedge_action,
                    "usd_value": usd_value,
                    "message": f"Dealer Gamma Risk: MM likely to {hedge_action} due to {side} of {moneyness} {opt_type}"
                })
                
        return results

class SmartTape:
    """
    Reconstructs fragmented orders into "Whale" orders.
    Merges sequential trades at the exact same millisecond and price.
    """
    def __init__(self, flush_interval_ms: int = 50):
        self.last_trade = None # {price, qty, ts, side}
        self.aggregated_trades = []
        self.cvd = 0.0
        self.cvd_high = 0.0
        self.cvd_low = 0.0

    def process_trade(self, price: float, qty: float, ts: int, is_buyer_maker: bool) -> Optional[Dict[str, Any]]:
        """
        Aggregate trades. Returns a merged trade if the sequence breaks, else None.
        """
        side = "SELL" if is_buyer_maker else "BUY"
        delta = -qty if is_buyer_maker else qty
        self.cvd += delta
        self.cvd_high = max(self.cvd_high, self.cvd)
        self.cvd_low = min(self.cvd_low, self.cvd)

        if self.last_trade and \
           self.last_trade['price'] == price and \
           self.last_trade['ts'] == ts and \
           self.last_trade['side'] == side:
            # Merge sequential trade
            self.last_trade['qty'] += qty
            return None
        else:
            # Sequence broke, return the previous aggregated trade and start new one
            prev_trade = self.last_trade
            self.last_trade = {
                "price": price,
                "qty": qty,
                "ts": ts,
                "side": side,
                "cvd": self.cvd,
                "cvd_high": self.cvd_high,
                "cvd_low": self.cvd_low
            }
            return prev_trade


class DOMTracker:
    """
    Real-time Depth of Market (DOM) Tracker.
    Maintains active NumPy arrays for the top N levels of bids and asks.
    Enables instant liquidity analysis and wall detection.
    """
    def __init__(self, depth_levels: int = 20):
        self.depth_levels = depth_levels
        # Pre-allocate arrays for bids and asks [Price, Quantity]
        self.bids = np.zeros((depth_levels, 2), dtype=np.float64)
        self.asks = np.zeros((depth_levels, 2), dtype=np.float64)
        self.last_update_ts = 0

    def update(self, snapshot: Dict[str, Any]):
        """Update the active DOM arrays from a raw orderbook snapshot."""
        # snapshot format expected: {"bids": [[p, q], ...] or [{"p": p, "q": q}, ...], "asks": ..., "timestamp": ts}
        raw_bids = snapshot.get("bids", [])
        raw_asks = snapshot.get("asks", [])
        
        # Reset arrays to handle cases where snapshot has fewer levels than depth_levels
        self.bids.fill(0)
        self.asks.fill(0)
        
        # Fill bids array (up to depth_levels)
        for i in range(min(len(raw_bids), self.depth_levels)):
            bid = raw_bids[i]
            if isinstance(bid, dict):
                self.bids[i] = [float(bid.get("p", 0)), float(bid.get("q", 0))]
            else:
                self.bids[i] = [float(bid[0]), float(bid[1])]
            
        # Fill asks array (up to depth_levels)
        for i in range(min(len(raw_asks), self.depth_levels)):
            ask = raw_asks[i]
            if isinstance(ask, dict):
                self.asks[i] = [float(ask.get("p", 0)), float(ask.get("q", 0))]
            else:
                self.asks[i] = [float(ask[0]), float(ask[1])]
            
        self.last_update_ts = snapshot.get("timestamp", 0)

    def get_weighted_imbalance(self, decay: float = 0.1) -> float:
        """
        Calculate Weighted Order Book Imbalance (OBI).
        Weights liquidity closest to the spread heavier using exponential decay.
        Formula: Sum(Qty_i * e^(-decay * i))
        """
        # Create weight vector: [e^0, e^-decay, e^(-2*decay), ...]
        indices = np.arange(self.depth_levels)
        weights = np.exp(-decay * indices)
        
        # Apply weights to quantities (column 1 of bids/asks)
        weighted_bid_vol = np.sum(self.bids[:, 1] * weights)
        weighted_ask_vol = np.sum(self.asks[:, 1] * weights)
        
        total = weighted_bid_vol + weighted_ask_vol
        if total == 0: return 0.0
        
        # Result ranges from -1.0 (pure sell pressure) to +1.0 (pure buy pressure)
        return (weighted_bid_vol - weighted_ask_vol) / total

    def get_imbalance(self) -> float:
        """Legacy unweighted imbalance for backward compatibility."""
        return self.get_weighted_imbalance(decay=0.0)

    def detect_walls(self, multiplier: float = 5.0) -> Dict[str, List[Dict[str, float]]]:
        """Detect liquidity walls (orders significantly larger than the average level)."""
        # Only calculate mean for non-zero levels to avoid skewing from padding
        bid_qtys = self.bids[self.bids[:, 1] > 0, 1]
        ask_qtys = self.asks[self.asks[:, 1] > 0, 1]
        
        avg_bid_vol = np.mean(bid_qtys) if bid_qtys.size > 0 else 0
        avg_ask_vol = np.mean(ask_qtys) if ask_qtys.size > 0 else 0
        
        walls = {"bids": [], "asks": []}
        
        if avg_bid_vol > 0:
            for i in range(self.depth_levels):
                if self.bids[i, 1] > avg_bid_vol * multiplier:
                    walls["bids"].append({"price": self.bids[i, 0], "quantity": self.bids[i, 1]})
        
        if avg_ask_vol > 0:
            for i in range(self.depth_levels):
                if self.asks[i, 1] > avg_ask_vol * multiplier:
                    walls["asks"].append({"price": self.asks[i, 0], "quantity": self.asks[i, 1]})
                
        return walls


class SpoofingFilter:
    """
    Detects Order Pulling & Stacking (Spoofing) at massive limit levels.
    Flags walls that disappear as price approaches (Spoofing) 
    vs. walls that reload/stay (True Support/Resistance).
    """
    def __init__(self, proximity_threshold: float = 0.002, memory_ttl_ms: int = 5000):
        self.previous_walls = {"bids": {}, "asks": {}} # Price -> Quantity
        self.recent_pulls = {"bids": {}, "asks": {}} # Price -> {"qty": float, "ts": int}
        self.proximity_threshold = proximity_threshold # 0.2% proximity
        self.memory_ttl_ms = memory_ttl_ms # 5 seconds memory for reloads

    def analyze(self, current_walls: Dict[str, List[Dict[str, float]]], current_price: float) -> List[Dict[str, Any]]:
        """
        Compare current walls with previous state to detect pulling/stacking.
        """
        events = []
        now = int(time.time() * 1000)
        
        # Cleanup stale memory
        for side in ["bids", "asks"]:
            self.recent_pulls[side] = {p: v for p, v in self.recent_pulls[side].items() if now - v["ts"] < self.memory_ttl_ms}
        
        # Convert current walls to maps for O(1) lookup
        curr_bids = {w['price']: w['quantity'] for w in current_walls['bids']}
        curr_asks = {w['price']: w['quantity'] for w in current_walls['asks']}
        
        # 1. Analyze Bids (Support Walls)
        for price, prev_qty in self.previous_walls['bids'].items():
            if price not in curr_bids:
                # Wall disappeared
                proximity = abs(current_price - price) / current_price if current_price > 0 else 1.0
                if proximity <= self.proximity_threshold:
                    events.append({
                        "type": "SPOOFING_DETECTED",
                        "side": "BID",
                        "price": price,
                        "action": "PULLED_ON_APPROACH",
                        "severity": "HIGH" if proximity < 0.0005 else "MEDIUM"
                    })
                else:
                    events.append({
                        "type": "LIQUIDITY_REMOVED",
                        "side": "BID",
                        "price": price,
                        "action": "CANCELLED"
                    })
                # Store in memory to detect reloads later
                self.recent_pulls["bids"][price] = {"qty": prev_qty, "ts": now}
            elif curr_bids[price] < prev_qty * 0.4:
                events.append({
                    "type": "WALL_WEAKENING",
                    "side": "BID",
                    "price": price,
                    "reduction": f"{((1 - curr_bids[price]/prev_qty)*100):.1f}%"
                })

        for price, curr_qty in curr_bids.items():
            if price not in self.previous_walls['bids']:
                # Check if it's a reload
                if price in self.recent_pulls["bids"]:
                    events.append({
                        "type": "TRUE_SUPPORT_RELOAD",
                        "side": "BID",
                        "price": price,
                        "new_qty": curr_qty,
                        "was_pulled": True
                    })
                    del self.recent_pulls["bids"][price]
                else:
                    events.append({
                        "type": "WALL_STACKED",
                        "side": "BID",
                        "price": price,
                        "qty": curr_qty
                    })
            elif curr_qty > self.previous_walls['bids'][price] * 1.5:
                events.append({
                    "type": "TRUE_SUPPORT_RELOAD",
                    "side": "BID",
                    "price": price,
                    "new_qty": curr_qty,
                    "was_pulled": False
                })

        # 2. Analyze Asks (Resistance Walls)
        for price, prev_qty in self.previous_walls['asks'].items():
            if price not in curr_asks:
                proximity = abs(current_price - price) / current_price if current_price > 0 else 1.0
                if proximity <= self.proximity_threshold:
                    events.append({
                        "type": "SPOOFING_DETECTED",
                        "side": "ASK",
                        "price": price,
                        "action": "PULLED_ON_APPROACH",
                        "severity": "HIGH" if proximity < 0.0005 else "MEDIUM"
                    })
                else:
                    events.append({
                        "type": "LIQUIDITY_REMOVED",
                        "side": "ASK",
                        "price": price,
                        "action": "CANCELLED"
                    })
                self.recent_pulls["asks"][price] = {"qty": prev_qty, "ts": now}
            elif curr_asks[price] < prev_qty * 0.4:
                events.append({
                    "type": "WALL_WEAKENING",
                    "side": "ASK",
                    "price": price,
                    "reduction": f"{((1 - curr_asks[price]/prev_qty)*100):.1f}%"
                })

        for price, curr_qty in curr_asks.items():
            if price not in self.previous_walls['asks']:
                if price in self.recent_pulls["asks"]:
                    events.append({
                        "type": "TRUE_RESISTANCE_RELOAD",
                        "side": "ASK",
                        "price": price,
                        "new_qty": curr_qty,
                        "was_pulled": True
                    })
                    del self.recent_pulls["asks"][price]
                else:
                    events.append({
                        "type": "WALL_STACKED",
                        "side": "ASK",
                        "price": price,
                        "qty": curr_qty
                    })
            elif curr_qty > self.previous_walls['asks'][price] * 1.5:
                events.append({
                    "type": "TRUE_RESISTANCE_RELOAD",
                    "side": "ASK",
                    "price": price,
                    "new_qty": curr_qty,
                    "was_pulled": False
                })

        # Update state for next analysis
        self.previous_walls['bids'] = curr_bids
        self.previous_walls['asks'] = curr_asks
        
        return events


class IcebergDetector:
    """
    Detects Hidden (Iceberg) orders by cross-referencing the Smart Tape with the DOM.
    If traded volume at a price level significantly exceeds the displayed liquidity 
    without the price moving, it flags a hidden institutional participant.
    """
    def __init__(self, threshold_multiplier: float = 3.0):
        self.threshold_multiplier = threshold_multiplier
        # price -> {"total_qty": float, "displayed_qty": float, "side": str, "last_ts": int, "detected": bool}
        self.active_levels = {} 

    def analyze(self, price: float, qty: float, side: str, dom: DOMTracker, ts: int) -> Optional[Dict[str, Any]]:
        # Cleanup old levels (older than 10 seconds)
        self.active_levels = {p: v for p, v in self.active_levels.items() if ts - v["last_ts"] < 10000}

        if price not in self.active_levels:
            # Find displayed liquidity at this price in the DOM
            displayed_qty = 0.0
            if side == "BUY":
                # Aggressive BUY hits ASKS
                for p, q in dom.asks:
                    if np.isclose(p, price, atol=1e-8):
                        displayed_qty = q
                        break
            else:
                # Aggressive SELL hits BIDS
                for p, q in dom.bids:
                    if np.isclose(p, price, atol=1e-8):
                        displayed_qty = q
                        break
            
            if displayed_qty <= 0:
                return None # Not a visible limit level
            
            self.active_levels[price] = {
                "total_qty": 0.0,
                "displayed_qty": displayed_qty,
                "side": side,
                "last_ts": ts,
                "detected": False
            }

        level = self.active_levels[price]
        level["total_qty"] += qty
        level["last_ts"] = ts

        # Check if threshold exceeded and not already flagged for this specific level instance
        if not level["detected"] and level["total_qty"] > level["displayed_qty"] * self.threshold_multiplier:
            level["detected"] = True
            return {
                "type": "ICEBERG_DETECTED",
                "price": price,
                "total_traded": level["total_qty"],
                "displayed_qty": level["displayed_qty"],
                "side": "HIDDEN_BUYER" if side == "SELL" else "HIDDEN_SELLER",
                "timestamp": ts
            }
        
        return None


class CandleAggregator:
    """
    Multi-Timeframe Candle Aggregator.
    Builds OHLCV candles from raw ticks for various timeframes (e.g. 1m, 5m, 15m).
    """
    def __init__(self, timeframes: List[str] = ["1m", "5m", "15m"], max_history: int = 200):
        self.timeframes = timeframes
        self.max_history = max_history
        # timeframe -> list of candles [{"o", "h", "l", "c", "v", "ts"}]
        self.candles: Dict[str, List[Dict[str, float]]] = {tf: [] for tf in timeframes}
        # timeframe -> current building candle
        self.current_candle: Dict[str, Optional[Dict[str, float]]] = {tf: None for tf in timeframes}

    def _get_interval_ms(self, timeframe: str) -> int:
        unit = timeframe[-1]
        val = int(timeframe[:-1])
        if unit == 'm': return val * 60 * 1000
        if unit == 'h': return val * 60 * 60 * 1000
        if unit == 'd': return val * 24 * 60 * 60 * 1000
        return 60 * 1000 # Default 1m

    def process_tick(self, price: float, qty: float, ts: int) -> Dict[str, Any]:
        """
        Updates internal candle state. Returns a list of newly closed candles.
        """
        closed_candles = []
        
        for tf in self.timeframes:
            interval_ms = self._get_interval_ms(tf)
            candle_start_ts = (ts // interval_ms) * interval_ms
            
            curr = self.current_candle[tf]
            
            if curr is None:
                # First tick for this timeframe
                self.current_candle[tf] = {
                    "o": price, "h": price, "l": price, "c": price, "v": qty, "ts": candle_start_ts, "tf": tf
                }
            elif candle_start_ts > curr["ts"]:
                # Candle closed
                full_candle = curr.copy()
                self.candles[tf].append(full_candle)
                if len(self.candles[tf]) > self.max_history:
                    self.candles[tf].pop(0)
                
                closed_candles.append(full_candle)
                
                # Start new candle
                self.current_candle[tf] = {
                    "o": price, "h": price, "l": price, "c": price, "v": qty, "ts": candle_start_ts, "tf": tf
                }
            else:
                # Update current candle
                curr["h"] = max(curr["h"], price)
                curr["l"] = min(curr["l"], price)
                curr["c"] = price
                curr["v"] += qty
                
        return closed_candles

    def get_candles(self, timeframe: str) -> List[Dict[str, float]]:
        return self.candles.get(timeframe, [])

    def seed_candles(self, timeframe: str, new_candles: List[Dict[str, float]]):
        if timeframe in self.candles and new_candles:
            # The last candle might be incomplete (still forming)
            # So we set it as the current_candle
            completed_candles = new_candles[:-1]
            current_candle = new_candles[-1]
            
            self.candles[timeframe].extend(completed_candles)
            if len(self.candles[timeframe]) > self.max_history:
                self.candles[timeframe] = self.candles[timeframe][-self.max_history:]
                
            self.current_candle[timeframe] = current_candle

    def get_current_candle(self, timeframe: str) -> Optional[Dict[str, float]]:
        return self.current_candle.get(timeframe)

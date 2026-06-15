import collections
import time
import json
from datetime import datetime
import numpy as np

class RiskManager:
    """
    Survival Infrastructure: Handles strict risk capital rules,
    daily drawdown limits, and volatility-based stop losses.
    """
    def __init__(self, account_size: float = 10000.0, max_risk_per_trade: float = 0.02, max_daily_drawdown: float = 0.05, max_portfolio_heat: float = 0.20):
        self.initial_account_size = account_size
        self.account_size = account_size
        self.max_risk_per_trade = max_risk_per_trade # 1.5% default (within 1-2% rule)
        self.max_daily_drawdown = max_daily_drawdown # 5% default
        self.max_portfolio_heat = max_portfolio_heat # 10% max total risk across all trades
        self.daily_high_water_mark = account_size
        self.risk_overrides = {
            "BTC/USDT": {"maxHeat": 0.05, "maxRisk": 0.02},
            "BTCUSDT": {"maxHeat": 0.05, "maxRisk": 0.02},
            "ETH/USDT": {"maxHeat": 0.05, "maxRisk": 0.02},
            "ETHUSDT": {"maxHeat": 0.05, "maxRisk": 0.02},
            "DOGE/USDT": {"maxHeat": 0.005, "maxRisk": 0.002},
            "DOGEUSDT": {"maxHeat": 0.005, "maxRisk": 0.002},
            "SHIB/USDT": {"maxHeat": 0.005, "maxRisk": 0.002},
            "SHIBUSDT": {"maxHeat": 0.005, "maxRisk": 0.002},
            "PEPE/USDT": {"maxHeat": 0.005, "maxRisk": 0.002},
            "PEPEUSDT": {"maxHeat": 0.005, "maxRisk": 0.002},
            "SOL/USDT": {"maxHeat": 0.02, "maxRisk": 0.01},
            "SOLUSDT": {"maxHeat": 0.02, "maxRisk": 0.01},
        }
        
        # Track trades
        self.active_trades = {}
        self.last_reset_day = datetime.utcnow().day
        self.current_regime = "NEUTRAL"
        self.killswitch_active = False
        self.killswitch_expiry = 0
        
        self.halted_symbols = {} # symbol -> expiry_time
        self.portfolio_halt_expiry = 0
        
        # Sector Mapping (Simple Alternative to Correlation Matrix)
        self.symbol_sectors = {
            "BTC/USDT": "CRYPTO_MAJOR",
            "ETH/USDT": "CRYPTO_MAJOR",
            "BNB/USDT": "CRYPTO_MAJOR",
            "SOL/USDT": "CRYPTO_MAJOR",
            "ARB/USDT": "CRYPTO_L2",
            "OP/USDT": "CRYPTO_L2",
            "MATIC/USDT": "CRYPTO_L2",
            "LINK/USDT": "DEFI",
            "UNI/USDT": "DEFI",
            "AAVE/USDT": "DEFI",
            "USDC/USDT": "STABLE",
            "GOLD/USDT": "STABLE_COMMODITY"
        }
        self.active_sectors = set() # Track sectors with open trades
        self.recent_outcomes = collections.deque(maxlen=20) # 1 for WIN, 0 for LOSS
        self.symbol_pnl_history = {} # Track exact PNL percentage returns per symbol
        
        # Approximate ATR tracking using standard deviation of tick prices for fast HFT logic
        self.recent_prices = collections.deque(maxlen=100)
    
    def log_trade_outcome(self, profit_usd: float, symbol: str = None, risk_amount: float = None):
        if profit_usd > 0:
            self.recent_outcomes.append(1)
        else:
            self.recent_outcomes.append(0)
            
        if symbol and risk_amount and risk_amount > 0:
            if symbol not in self.symbol_pnl_history:
                self.symbol_pnl_history[symbol] = collections.deque(maxlen=20)
            # Log exact returns mapped against risk initially allocated
            return_pct = profit_usd / risk_amount
            self.symbol_pnl_history[symbol].append(return_pct)

    def is_symbol_halted(self, symbol: str) -> bool:
        current_time = time.time()
        
        # Check portfolio halt
        if current_time < self.portfolio_halt_expiry:
            return True
            
        # Check symbol halt
        if symbol in self.halted_symbols and current_time < self.halted_symbols[symbol]:
            return True

        if symbol not in self.symbol_pnl_history or len(self.symbol_pnl_history[symbol]) < 5:
            return False
            
        returns = list(self.symbol_pnl_history[symbol])
        if len(returns) == 0:
            return False
            
        mean_return = np.mean(returns)
        std_dev = np.std(returns)
        
        # Calculate Rolling Sharpe (assuming risk-free near 0 for these short HFT durations)
        sharpe = (mean_return / std_dev) if std_dev > 0.0001 else 0
        
        # Calculate Sortino
        downside_returns = [r for r in returns if r < 0]
        downside_std = np.std(downside_returns) if len(downside_returns) > 0 else 0
        sortino = (mean_return / downside_std) if downside_std > 0.0001 else 0

        win_rate = len([r for r in returns if r > 0]) / len(returns)

        # Halt if execution structurally degrades below random chop threshold
        if win_rate < 0.3 or sortino < -0.5:
            self.halted_symbols[symbol] = current_time + 3600 # 1 hour halt
            print(f"[Circuit Breaker] Asset {symbol} halted for 1 hour! Win Rate: {win_rate:.2f}, Sortino: {sortino:.2f}", flush=True)
            return True
            
        # Check overall portfolio health
        if len(self.recent_outcomes) >= 10:
            global_win_rate = sum(list(self.recent_outcomes)[-10:]) / 10
            if global_win_rate <= 0.2:
                self.portfolio_halt_expiry = current_time + 7200 # 2 hour halt
                print(f"[Circuit Breaker] Global Portfolio halted for 2 hours! Recent Win Rate: {global_win_rate:.2f}", flush=True)
                return True
                
        return False

    def get_equity_curve_multiplier(self) -> float:
        if len(self.recent_outcomes) < 5:
            return 1.0
        
        win_rate = sum(self.recent_outcomes) / len(self.recent_outcomes)
        if win_rate < 0.4:
            # Under 40% win rate -> aggressively reduce position sizing
            return 0.25 # Only risk 25% of standard sizing
        elif win_rate < 0.5:
            return 0.50
        elif win_rate > 0.7:
            return 1.25 # Slightly reward hot streaks
        return 1.0

    def update_price(self, price: float):
        self.recent_prices.append(price)

    def update_regime(self, regime: str, killswitch: bool = False):
        self.current_regime = regime
        if killswitch and not self.killswitch_active:
            self.trigger_killswitch("MACRO_SHOCK")
        elif not killswitch and self.killswitch_active:
            if time.time() > self.killswitch_expiry:
                self.killswitch_active = False

    def trigger_killswitch(self, reason: str):
        """Hard Kill-Switching: Flatten and Pause for 24 hours."""
        self.killswitch_active = True
        self.killswitch_expiry = time.time() + (24 * 3600)
        # Flush active trades (Logic to flatten should be handled in signal router)
        self.active_trades.clear()
        self.active_sectors.clear()

    def get_atr_proxy(self, price: float) -> float:
        """Approximates Average True Range using recent price volatility."""
        if len(self.recent_prices) < 20:
            return price * 0.005 # Default to 0.5% if not enough data
        # Calculate standard deviation as a proxy for volatility
        std_dev = np.std(self.recent_prices)
        return float(max(std_dev * 1.5, price * 0.001)) # Scale to approximate true range

    def update_account_size(self, new_size: float):
        """Updates account size from broker."""
        if new_size <= 0: return
        self.account_size = new_size
        if self.account_size > self.daily_high_water_mark:
            self.daily_high_water_mark = self.account_size

    def is_vetoed(self, symbol: str = None) -> bool:
        """Vetoes trades if the daily drawdown limit is reached, heat limit is breached, or regime is toxic."""
        if symbol and self.is_symbol_halted(symbol):
            print(f"[Risk Manager] VETO: Structural Degradation Halt triggered for {symbol}.")
            return True

        if self.killswitch_active:
            if time.time() > self.killswitch_expiry:
                self.killswitch_active = False
            else:
                return True

        # Removed 'SHOCK' and 'CONTRACTION' from hard vetoes here, because shorting and mean-reversion 
        # might still be allowed. This logic is shifted (Gate 1) directly into the strategy evaluations.
        if self.current_regime in ["TOXIC"]:
            return True # Hard Kill-Switching based on macro regime
            
        current_day = datetime.utcnow().day
        if current_day != self.last_reset_day:
            self.last_reset_day = current_day
            self.daily_high_water_mark = self.account_size
            
        if self.account_size > self.daily_high_water_mark:
            self.daily_high_water_mark = self.account_size
            
        drawdown = (self.daily_high_water_mark - self.account_size) / self.daily_high_water_mark
        if drawdown >= self.max_daily_drawdown:
            return True # Hard Daily Halt

        # Portfolio Heat Limit check (Percentage based)
        total_risk = 0
        symbol_risk = 0
        for tid, tdata in self.active_trades.items():
            trade_risk = tdata.get("risk_amount", 0)
            total_risk += trade_risk
            if symbol and tdata.get("symbol") == symbol:
                symbol_risk += trade_risk
        
        current_heat = total_risk / self.account_size if self.account_size > 0 else 1.0
        sym_heat = symbol_risk / self.account_size if self.account_size > 0 else 1.0
        
        global_allowed_heat = self.max_portfolio_heat
        if current_heat >= global_allowed_heat:
            return True

        # Localized per-asset heat limit
        if symbol and symbol in self.risk_overrides and "maxHeat" in self.risk_overrides[symbol]:
            allowed_symbol_heat = self.risk_overrides[symbol]["maxHeat"]
            if sym_heat >= allowed_symbol_heat:
                return True

        # Sector Limit check (Simple Alternative to Correlation Matrix)
        # Allows for multiple trades in the same sector if the setup is clean
        # (Disabled Max 1 trade per sector to prevent limiting profitable setups)
        if symbol:
            sector = self.symbol_sectors.get(symbol, "DEFAULT")
            self.active_sectors.add(sector) # Just tracking

        return False
        
    def calculate_trade_parameters_strict(self, direction: str, current_price: float, atr_value: float) -> dict:
        """
        Calculates trade parameters according to strict Prompt 4 rules.
        """
        # The 1% Rule
        risk_pct = 0.01 
        account_balance = self.account_size
        capital_at_risk = account_balance * risk_pct
        
        stop_dist = atr_value * 2.0
        
        if direction == "LONG":
            stop_loss_price = current_price - stop_dist
            take_profit_price = current_price + (atr_value * 4.0) # 1:2 Risk/Reward
        else: # SHORT
            stop_loss_price = current_price + stop_dist
            take_profit_price = current_price - (atr_value * 4.0) # 1:2 Risk/Reward
            
        dist_pct = stop_dist / current_price
        
        if dist_pct > 0:
            position_size_usd = capital_at_risk / dist_pct
        else:
            position_size_usd = 0
            
        position_size = position_size_usd / current_price

        return {
            "position_size": position_size,
            "position_size_usd": position_size_usd,
            "stop_loss_price": stop_loss_price,
            "stop_loss": stop_loss_price, # Alias
            "take_profit_price": take_profit_price,
            "take_profit": take_profit_price # Alias
        }

    def calculate_trade_parameters(self, symbol: str, direction: str, entry_price: float, is_a_plus_setup: bool = False, atr_override: float = None) -> dict:
        """
        Dynamically calculates position size using Fractional Dynamic Risk (Anti-Martingale)
        and Volatility-Adjusted Stops (ATR).
        Exponential Growth: Asymmetric R:R, Regime-Dependent Aggression, Risk-Free Pyramiding.
        """
        atr = self.get_atr_proxy(entry_price)
        
        # Chandelier Exit multiplier (usually 2.5x ATR)
        stop_dist = atr * 2.5 
        
        if direction == "LONG":
            stop_loss = entry_price - stop_dist
            take_profit = entry_price + (stop_dist * 3.0) # 1:3 R:R minimum
            break_even_target = entry_price + stop_dist # 1:1 target for auto-breakeven and initial scale out
        else: # SHORT
            stop_loss = entry_price + stop_dist
            take_profit = entry_price - (stop_dist * 3.0) # 1:3 R:R minimum
            break_even_target = entry_price - stop_dist
            
        # Fractional Dynamic Risk (Anti-Martingale)
        equity_modulator = self.get_equity_curve_multiplier()
        base_risk = self.max_risk_per_trade
        if symbol in self.risk_overrides and "maxRisk" in self.risk_overrides[symbol]:
            base_risk = self.risk_overrides[symbol]["maxRisk"]
            
        risk_pct = base_risk * equity_modulator

        # Update Snowball math: Base risk is scaled according to realized profit
        # If account has grown, we slowly reinvest profits using a squashed multiplier 
        # so we don't risk entire profits quickly.
        profit = self.account_size - self.initial_account_size
        if profit > 0:
            # Reinvest 20% of net profit curve into sizing dynamically
            capital_at_risk = (self.initial_account_size * risk_pct) + ((profit * 0.20) * risk_pct)
        else:
            capital_at_risk = self.account_size * risk_pct
        
        # Position sizing formula: Capital at Risk / Distance to Stop
        dist_pct = stop_dist / entry_price
        
        # If the required stop-loss is too wide (e.g. > 15% distance), deny the trade
        if dist_pct > 0.15:
            return None
            
        total_position_size_usd = capital_at_risk / dist_pct if dist_pct > 0 else 0
        
        # Enter 100% of intended size
        initial_position_size_usd = total_position_size_usd
        
        return {
            "position_size_usd": round(initial_position_size_usd, 2),
            "total_intended_size_usd": round(total_position_size_usd, 2),
            "stop_loss": round(stop_loss, 4),
            "take_profit": round(take_profit, 4),
            "break_even_target": round(break_even_target, 4),
            "atr_proxy": round(atr, 4),
            "risk_amount": capital_at_risk
        }
        
    def remove_all_trades_for_symbol(self, symbol: str, exit_price: float = None):
        tids_to_remove = [tid for tid, trade in self.active_trades.items() if trade.get("symbol") == symbol]
        for tid in tids_to_remove:
            self.remove_trade(tid, exit_price)

    def remove_trade(self, trade_id: str, exit_price: float = None):
        if trade_id in self.active_trades:
            trade = self.active_trades[trade_id]
            if exit_price is not None and trade.get("entry_price"):
                entry = trade.get("entry_price", 0)
                direction = trade.get("direction", "LONG")
                profit = (exit_price - entry) if direction == "LONG" else (entry - exit_price)
                
                # We need nominal profit USD
                position_size_usd = trade.get("position_size_usd", 0)
                risk_amount = trade.get("risk_amount", 0)
                symbol = trade.get("symbol")
                
                # Approximate USD profit based on price delta percentage
                pct_return = profit / entry if entry > 0 else 0
                profit_usd = position_size_usd * pct_return
                
                self.log_trade_outcome(profit_usd, symbol, risk_amount)

            symbol = trade.get("symbol")
            if symbol:
                sector = self.symbol_sectors.get(symbol, "DEFAULT")
                if sector in self.active_sectors:
                    self.active_sectors.remove(sector)
            del self.active_trades[trade_id]

    def add_trade(self, trade_id: str, symbol: str, direction: str, entry_price: float, stop_loss: float, take_profit: float, break_even_target: float, risk_amount: float = 0, position_size_usd: float = 0):
        sector = self.symbol_sectors.get(symbol, "DEFAULT")
        self.active_sectors.add(sector)
        self.active_trades[trade_id] = {
            "symbol": symbol,
            "direction": direction,
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "break_even_target": break_even_target,
            "risk_amount": risk_amount,
            "position_size_usd": position_size_usd,
            "start_time": time.time(),
            "candle_count": 0
        }
        
    def manage_active_trade(self, trade_id: str, current_price: float) -> dict:
        """
        Implements dynamic trailing stops, auto-breakeven, and Stagnant Trade Purging.
        """
        if trade_id not in self.active_trades:
            return None
            
        trade = self.active_trades[trade_id]
        trade["candle_count"] += 0.01 # Approximate (assuming this matches update frequency)
        
        # Time-Based Drawdowns (Cut the Dead Wood)
        # If trade is stagnant (sideways) for > 100 iterations (nominal "candles")
        # without major move, close it.
        if trade["candle_count"] > 100 and not trade.get("scaled_out", False):
            return {"event": "STAGNANT_EXIT", "reason": "Time-based decay reached"}

        direction = trade["direction"]
        entry = trade["entry_price"]
        sl = trade["stop_loss"]
        tp = trade.get("take_profit", entry * 2) # fallback
        target_1to1 = trade["break_even_target"]
        
        action = None
        atr = self.get_atr_proxy(current_price)
        
        if direction == "LONG":
            if current_price <= trade["stop_loss"]:
                return {"event": "TRAIL_STOP_EXIT", "reason": "Price crossed trailing stop limit"}
            elif current_price >= tp:
                return {"event": "TAKE_PROFIT_EXIT", "reason": "1:3 Take Profit target reached"}
            
            elif current_price >= target_1to1 and sl < entry:
                # Move stop to breakeven
                trade["stop_loss"] = entry
                # Mark as scaled out (The "Runner")
                if not trade.get("scaled_out", False):
                    trade["scaled_out"] = True
                    action = {"event": "SCALE_OUT", "new_sl": entry, "scale_pct": 0.50} # Sell 50% at 1:1
                else:
                    action = {"event": "MOVE_TO_BREAKEVEN", "new_sl": entry}
            
            # Let it run to 1:3 target without Chandelier trailing
                
        elif direction == "SHORT":
            if current_price >= trade["stop_loss"]:
                return {"event": "TRAIL_STOP_EXIT", "reason": "Price crossed trailing stop limit"}
            elif current_price <= tp:
                return {"event": "TAKE_PROFIT_EXIT", "reason": "1:3 Take Profit target reached"}
                
            elif current_price <= target_1to1 and sl > entry:
                # Move stop to breakeven
                trade["stop_loss"] = entry
                if not trade.get("scaled_out", False):
                    trade["scaled_out"] = True
                    action = {"event": "SCALE_OUT", "new_sl": entry, "scale_pct": 0.50}
                else:
                    action = {"event": "MOVE_TO_BREAKEVEN", "new_sl": entry}

            # Let it run to 1:3 target without Chandelier trailing
                
        return action

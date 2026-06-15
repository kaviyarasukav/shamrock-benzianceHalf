import asyncio
import time
import logging

class AutopilotManager:
    def __init__(self, symbol_states_ref, symbol_states_lock_ref, publish_callback):
        self.symbol_states = symbol_states_ref
        self.symbol_states_lock = symbol_states_lock_ref
        self.publish_callback = publish_callback
        self.is_running = False
        
    def start(self):
        self.is_running = True
        
    def stop(self):
        self.is_running = False
        
    async def run_loop(self):
        while True:
            if not self.is_running:
                await asyncio.sleep(2)
                continue
                
            try:
                symbols_to_evaluate = []
                async with self.symbol_states_lock:
                    for s_name, s_state in self.symbol_states.items():
                        # We need Confluence engine and Macro context
                        symbols_to_evaluate.append((s_name, s_state))
                        
                for s_name, s_state in symbols_to_evaluate:
                    await self._evaluate_asset(s_name, s_state)
                    
            except Exception as e:
                logging.error(f"Autopilot evaluation error: {e}")
                
            await asyncio.sleep(5) # Evaluate every 5 seconds
            
    async def _evaluate_asset(self, symbol, state):
        c_engine = state.get("confluence_engine")
        if not c_engine:
            return
            
        m_state = c_engine.market_state
        
        risk_manager = getattr(self, 'risk_manager', None)
        if risk_manager:
            for tid, tdata in risk_manager.active_trades.items():
                if tdata.get("symbol") == symbol:
                    # Already have an active trade for this symbol
                    return
        
        # 1. Fetch MTF score & Macro regime
        macro_regime = m_state.get("macro_regime", "CHOP")
        mtf_bias = c_engine.get_mtf_bias() # LONG, SHORT, NEUTRAL
        current_price = m_state.get("current_price", 0)
        
        # 2. Check SMC state
        smc_score = 0
        order_blocks = m_state.get("order_blocks", {"bullish": [], "bearish": []})
        proximity_threshold = 0.005 # 0.5%
        
        if current_price > 0:
            for ob in order_blocks.get("bullish", []):
                if len(ob) >= 2:
                    ob_bottom, ob_top = ob[0], ob[1]
                    if ob_bottom <= current_price <= ob_top or abs(current_price - ((ob_bottom + ob_top) / 2.0)) / current_price <= proximity_threshold:
                        smc_score += 15
                        break
            
            for ob in order_blocks.get("bearish", []):
                if len(ob) >= 2:
                    ob_bottom, ob_top = ob[0], ob[1]
                    if ob_bottom <= current_price <= ob_top or abs(current_price - ((ob_bottom + ob_top) / 2.0)) / current_price <= proximity_threshold:
                        smc_score -= 15
                        break
        
        fvgs = m_state.get("fvgs", {})
        if isinstance(fvgs, dict):
            for tf, fvg_list in fvgs.items():
                if isinstance(fvg_list, list) and fvg_list:
                    # Generic indication of imbalance presence
                    smc_score += 5 if mtf_bias == "LONG" else -5
                    break

        # 3. Check Order Flow (Spoofing, Icebergs)
        of_score = 0
        if m_state.get("active_spoofs"):
            of_score += 5 # Volatility present
        if m_state.get("active_icebergs"):
            of_score += 10
            
        # Check CVD Divergence
        cvd_div = c_engine.get_delta_divergence() if c_engine and hasattr(c_engine, 'get_delta_divergence') else "NONE"
        if cvd_div == "BULLISH_DIVERGENCE":
            of_score += 15
        elif cvd_div == "BEARISH_DIVERGENCE":
            of_score -= 15

        # 4. Momentum / Volume Climax Check
        if c_engine.is_volume_climax():
            # If climax happens, it could be exhaustion
            of_score -= 20 * (1 if mtf_bias == "LONG" else -1)
            
        # 5. Build Conviction Score
        # Start at 50 to represent absolute neutrality
        conviction = 50 
        
        if mtf_bias == "LONG":
            conviction += 25
        elif mtf_bias == "SHORT":
            conviction -= 25
            
        if macro_regime == "RISK_ON":
            conviction += 10
        elif macro_regime in ["RISK_OFF", "SHOCK"]:
            conviction -= 15

        conviction += smc_score
        # Order flow directly adds/subtracts to conviction
        conviction += of_score
        
        # Playbook Combos Check (High-Win-Rate)
        success_combo, payload_combo = False, None
        if hasattr(c_engine, 'evaluate_playbook_combos'):
            success_combo, payload_combo = c_engine.evaluate_playbook_combos()
            
        if success_combo and payload_combo:
            combo_dir = payload_combo.get("direction", "NONE")
            if combo_dir == "LONG":
                conviction += 30
            elif combo_dir == "SHORT":
                conviction -= 30

        # Advanced Technical Overbought/Oversold checks from 1m TF
        ind_1m = m_state.get("latest_indicators", {}).get("1m", {})
        if "rsi1" in ind_1m and isinstance(ind_1m["rsi1"], list) and len(ind_1m["rsi1"]) > 0:
            rsi_val = ind_1m["rsi1"][-1]
            if rsi_val is not None:
                if rsi_val > 75:
                    conviction -= 15 # Overbought
                elif rsi_val < 25:
                    conviction += 15 # Oversold
                    
        # Z-Score Reversion
        z_score = m_state.get("last_z_score", 0.0)
        hurst = m_state.get("macro_metrics", {}).get("hurst", 0.5)
        if hurst < 0.5: # Choppy, mean reverting
            if z_score <= -2.5:
                conviction += 20
            elif z_score >= 2.5:
                conviction -= 20
        else: # Trending
            if z_score <= -1.5 and mtf_bias == "LONG": # Buy dip
                conviction += 15
            elif z_score >= 1.5 and mtf_bias == "SHORT": # Sell rip
                conviction -= 15

        # Option Gamma Hedging Weighting
        options_bias_score = 0
        if "active_gamma_exposure" in m_state:
            gamma_alert = m_state["active_gamma_exposure"]
            if time.time() - gamma_alert.get("timestamp", 0) < 300: # 5 min exposure window
                hedge_action = gamma_alert.get("hedge_action", "")
                if hedge_action == "BUY_UNDERLYING":
                    options_bias_score = 30 # Massive upward hedging pressure
                elif hedge_action == "SELL_UNDERLYING":
                    options_bias_score = -30 # Massive downward hedging pressure
        
        conviction += options_bias_score

        # Cap 0-100
        conviction = max(0, min(100, conviction))
        
        # Emit scan thought
        reason_str = ""
        if success_combo and payload_combo:
            reason_str = f" 🔥 Playbook Match: {payload_combo.get('metadata', {}).get('playbook_combo', '')}!"
            
        scan_msg = f"Analyzing {symbol}... Conviction: {conviction}/100. MTF: {mtf_bias}. Macro: {macro_regime}. Z-Score: {round(z_score, 2)}.{reason_str}"
        
        self.publish_callback({
            "type": "AUTOPILOT_LOG",
            "symbol": symbol,
            "message": scan_msg
        })
        
        # 6. Generate Trade Signal if threshold met
        target_threshold = getattr(self, 'conviction_threshold', 75)
        
        if conviction >= target_threshold: # A+ Long
            self._trigger_signal(symbol, "LONG", conviction, m_state.get("current_price", 0))
        elif conviction <= (100 - target_threshold): # A+ Short
            self._trigger_signal(symbol, "SHORT", 100 - conviction, m_state.get("current_price", 0))
            
    def _trigger_signal(self, symbol, direction, conviction, price):
        order_type = getattr(self, 'order_type', 'MARKET')
        risk_manager = getattr(self, 'risk_manager', None)
        
        pos_size = 0
        sl = 0
        tp = 0
        
        if risk_manager:
            if risk_manager.is_vetoed(symbol):
                self.publish_callback({
                    "type": "AUTOPILOT_LOG",
                    "symbol": symbol,
                    "message": f"ABORT {direction} signal on {symbol}: VETOED by Risk Manager (Heat/Drawdown limits)."
                })
                return
                
            if direction == "LONG":
                trade_params = risk_manager.calculate_trade_parameters(symbol, "LONG", price, is_a_plus_setup=True)
            else:
                trade_params = risk_manager.calculate_trade_parameters(symbol, "SHORT", price, is_a_plus_setup=True)
                
            if not trade_params:
                # Stop loss too wide or risk exceeded, abort!
                self.publish_callback({
                    "type": "AUTOPILOT_LOG",
                    "symbol": symbol,
                    "message": f"ABORT {direction} signal on {symbol}: Risk parameters too wide."
                })
                return
            
            pos_size = trade_params["position_size_usd"]
            sl = trade_params["stop_loss"]
            tp = trade_params["take_profit"]

        self.publish_callback({
            "type": "STRATEGY_SIGNAL",
            "symbol": symbol,
            "direction": direction,
            "confidence": conviction,
            "signal_type": "AUTOPILOT_A_PLUS",
            "timeframe": "MULTIPLE",
            "price": price,
            "order_type": order_type,
            "position_size_usd": pos_size,
            "take_profit": tp,
            "stop_loss": sl,
            "metadata": {
                "reason": f"Autopilot {direction} conviction threshold breached.",
                "execution_mode": order_type,
                "position_size_usd": pos_size,
                "take_profit": tp,
                "stop_loss": sl
            }
        })

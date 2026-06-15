import sys
import json
import time
import asyncio
import numpy as np
import logging
import websockets
import zmq
import zmq.asyncio
import argparse
import ccxt.async_support as ccxt
import pandas as pd


from buffers import OrderBookRingBuffer, TickDataRingBuffer, DOMTracker, SpoofingFilter, SmartTape, IcebergDetector, OptionsAnalyzer, CandleAggregator
from indicators import IndicatorEngine
from strategy_confluence import ConfluenceEngine, MicroGearsEngine, MacroTrendEngine
from macro_worker import MacroRegimeAnalyzer
from smc_engine import SMCEngine
from hedging_engine import DeltaNeutralHedgingEngine
from arbitrage_engine import ArbitrageEngine
from grid_engine import GridEngine
from autopilot_manager import AutopilotManager

from risk_manager import RiskManager

global_risk_manager = RiskManager()
hedging_engine = DeltaNeutralHedgingEngine()
arbitrage_engine = ArbitrageEngine()
grid_engine = GridEngine()

# Multi-Asset State
symbol_states = {} # symbol -> {ob_buffer, tick_buffer, dom_tracker, spoof_filter, smart_tape, confluence_engine, candle_aggregator, indicator_engine, last_indicators}
symbol_states_lock = asyncio.Lock()
symbol_queues = {}
symbol_workers = {}
options_analyzer = OptionsAnalyzer()
global_strategy_mode = "HYBRID"

outbound_queue = asyncio.Queue()

def send_msg(payload):
    def _do_send():
        outbound_queue.put_nowait(json.dumps(payload))
        
    try:
        loop = asyncio.get_running_loop()
        outbound_queue.put_nowait(json.dumps(payload))
    except RuntimeError:
        if main_loop and main_loop.is_running():
            main_loop.call_soon_threadsafe(_do_send)

autopilot_manager = AutopilotManager(symbol_states, symbol_states_lock, send_msg)


async def get_or_create_symbol_worker(symbol):
    await get_or_create_symbol_state(symbol)
    if symbol not in symbol_workers:
        symbol_queues[symbol] = asyncio.Queue()
        symbol_workers[symbol] = asyncio.create_task(symbol_worker_loop(symbol, symbol_queues[symbol]))

async def symbol_worker_loop(symbol, queue):
    while True:
        try:
            line_str = await queue.get()
            await process_symbol_msg(symbol, line_str)
        except Exception as e:
            logging.error(f"Worker error for {symbol}: {e}")
            import traceback
            traceback.print_exc()

async def get_or_create_symbol_state(symbol):
    async with symbol_states_lock:
        if symbol not in symbol_states:
            engine = ConfluenceEngine(risk_manager=global_risk_manager)
            shadow_engine = ConfluenceEngine(risk_manager=global_risk_manager) 
            # Give the shadow engine a slightly different configuration so it creates a different PnL profile
            # For demonstration, we'll force it into SCALPER mode which trades more frequently.
            shadow_engine.strategy_mode = "SCALPER"
            shadow_engine.is_shadow = True  # We'll attach this property dynamically
            
            micro_engine = MicroGearsEngine(risk_manager=global_risk_manager)
            macro_engine = MacroTrendEngine(risk_manager=global_risk_manager)
            
            engine.strategy_mode = global_strategy_mode
            micro_engine.strategy_mode = global_strategy_mode
            macro_engine.strategy_mode = global_strategy_mode
            
            engine.publish_callback = send_msg
            shadow_engine.publish_callback = send_msg
            micro_engine.publish_callback = send_msg
            macro_engine.publish_callback = send_msg
            
            symbol_states[symbol] = {
                "ob_buffer": OrderBookRingBuffer(max_size=1000),
                "tick_buffer": TickDataRingBuffer(capacity=10000),
                "dom_tracker": DOMTracker(depth_levels=20),
                "spoof_filter": SpoofingFilter(proximity_threshold=0.005),
                "smart_tape": SmartTape(),
                "iceberg_detector": IcebergDetector(),
                "candle_aggregator": CandleAggregator(timeframes=["1m", "5m", "15m", "1h"], max_history=2000),
                "indicator_engine": IndicatorEngine(),
                "market_data": pd.DataFrame(columns=["Timestamp", "Open", "High", "Low", "Close", "Volume"]),
                "last_indicators": {tf: None for tf in ["1m", "5m", "15m", "1h"]},
                "smc_engines": {tf: SMCEngine() for tf in ["1m", "5m", "15m", "1h"]},
                "confluence_engine": engine,
                "shadow_confluence_engine": shadow_engine,
                "micro_engine": micro_engine,
                "macro_engine": macro_engine
            }
        return symbol_states[symbol]

main_loop = None

def on_macro_update(payload):
    # This is called from a thread in MacroRegimeAnalyzer
    send_msg(payload)
    # We use a helper to update engines asynchronously
    if main_loop and main_loop.is_running():
        asyncio.run_coroutine_threadsafe(update_engines_macro(payload), main_loop)

async def update_engines_macro(payload):
    async with symbol_states_lock:
        for s_name, s_state in list(symbol_states.items()):
            for engine_name in ["confluence_engine", "shadow_confluence_engine", "macro_engine"]:
                if hasattr(s_state[engine_name], 'update_macro_regime'):
                    s_state[engine_name].update_macro_regime(
                        payload.get("state", "CHOP"),
                        payload.get("killswitch_active", False),
                        payload.get("metrics", {})
                    )


exchange = None

async def init_exchange(api_key, secret_key, use_testnet=False, market_type='spot'):
    global exchange
    if exchange:
        await exchange.close()
    try:
        exchange = ccxt.binance({
            'apiKey': api_key,
            'secret': secret_key,
            'enableRateLimit': True,
            'options': {
                'defaultType': market_type
            }
        })
        if use_testnet:
            exchange.set_sandbox_mode(True)
        await exchange.load_markets()
        send_msg({"status": "INFO", "message": f"CCXT Exchange Initialized and Markets Loaded. Testnet: {use_testnet}"})
    except Exception as e:
        send_msg({"status": "ERROR", "message": f"Failed to init CCXT: {e}"})

async def execute_direct_order(signal):
    global exchange
    if not exchange:
        return
    try:
        # Check if the real payload is nested inside 'data'
        data_payload = signal.get('data', signal)
        
        # direct low latency execution
        symbol = signal.get('symbol', data_payload.get('ticker')) # expected e.g. BTC/USDT
        action = data_payload.get('direction', data_payload.get('action'))
        if not action:
            return
            
        side = 'buy' if action in ['BUY', 'LONG', 'CLOSE_SHORT'] else 'sell'
        order_type = data_payload.get('order_type', 'limit_chase').lower()
        price = data_payload.get('price', data_payload.get('trigger_price'))
        
        qty = data_payload.get('metadata', {}).get('requested_quantity') or data_payload.get('quantity')
        
        # Determine quantity for SCALE_OUT/SCALE_IN
        metadata = data_payload.get('metadata', {})
        if not qty and metadata.get('action') in ['SCALE_OUT', 'SCALE_IN']:
            orig_usd = metadata.get('original_position_size_usd', 0)
            if not orig_usd:
                orig_usd = metadata.get('position_size_usd', 0)
            scale_pct = metadata.get('scale_pct', 1.0)
            if orig_usd and price and price > 0:
                qty = (orig_usd * scale_pct) / price

        if not qty:
            position_size_usd = data_payload.get('position_size_usd') or metadata.get('position_size_usd')
            if position_size_usd and price and price > 0:
                qty = position_size_usd / price
            else:
                qty = data_payload.get('weight', 0.001)

        from decimal import Decimal, ROUND_DOWN
        try:
            if exchange.markets and symbol in exchange.markets:
                # Use strict string formulation before casting to CCXT standard numeric format
                # CCXT internal functions handle string safely to prevent IEEE float conversion mangling 
                precision_qty_str = exchange.amount_to_precision(symbol, float(qty))
                qty = float(Decimal(precision_qty_str))
                if price:
                    precision_price_str = exchange.price_to_precision(symbol, float(price))
                    price = float(Decimal(precision_price_str))
        except Exception:
            pass
            
        params = {}
        take_profit = data_payload.get('metadata', {}).get('takeProfit', data_payload.get('take_profit'))
        stop_loss = data_payload.get('metadata', {}).get('stopLoss', data_payload.get('stop_loss'))
        iceberg_qty = data_payload.get('metadata', {}).get('icebergQty')
        tif = data_payload.get('timeInForce') or data_payload.get('metadata', {}).get('timeInForce')

        if take_profit:
            params['takeProfitPrice'] = float(take_profit)
        if stop_loss:
            params['stopLossPrice'] = float(stop_loss)
        if iceberg_qty:
            params['icebergQty'] = float(iceberg_qty)
        if tif:
            params['timeInForce'] = tif
                
        send_msg({"status": "INFO", "message": f"Direct Python Execution: {side} {qty} {symbol} {order_type}"})
        
        if order_type == 'limit':
            order = await exchange.create_limit_order(symbol, side, qty, price, params)
            send_msg({"type": "HFT_ORDER_PLACED", "data": order})
        elif order_type == 'limit_chase':
            # Asynchronous Limit Chase Algorithm
            asyncio.create_task(limit_chase_worker(exchange, symbol, side, qty, price, params))
        elif order_type == 'twap':
            # TWAP Execution Algorithm
            duration_mins = signal.get('metadata', {}).get('duration_mins', 60)
            asyncio.create_task(twap_worker(exchange, symbol, side, qty, price, duration_mins))
        else:
            order = await exchange.create_market_order(symbol, side, qty, params)
            send_msg({"type": "HFT_ORDER_PLACED", "data": order})
    except Exception as e:
        send_msg({"type": "HFT_ORDER_ERROR", "error": str(e)})

async def twap_worker(exch, symbol, side, total_qty, starting_price, duration_mins):
    try:
        intervals = 10  # Slice order into 10 parts
        slice_qty = total_qty / intervals
        wait_time_sec = (duration_mins * 60) / intervals
        
        try:
            if exch.markets and symbol in exch.markets:
                slice_qty = float(exch.amount_to_precision(symbol, slice_qty))
        except Exception:
            pass
            
        send_msg({"status": "INFO", "message": f"Starting TWAP for {symbol}: {intervals} slices of {slice_qty} every {wait_time_sec}s"})
        
        for i in range(intervals):
            # Fetch latest price
            ticker = await exch.fetch_ticker(symbol)
            current_price = ticker['last']
            
            # Place slice order using limit chase logic
            send_msg({"status": "INFO", "message": f"TWAP Slice {i+1}/{intervals}: starting limit chase."})
            asyncio.create_task(limit_chase_worker(exch, symbol, side, slice_qty, current_price))
            
            if i < intervals - 1:
                await asyncio.sleep(wait_time_sec)
                
        send_msg({"status": "INFO", "message": f"TWAP Completed for {symbol}."})
    except Exception as e:
        send_msg({"type": "HFT_ORDER_ERROR", "error": f"TWAP error: {str(e)}"})

async def limit_chase_worker(exch, symbol, side, qty, initial_price, params=None):
    if params is None:
        params = {}
    try:
        current_price = initial_price
        max_retries = 10
        tick_size = 0.5 
        
        try:
            if exch.markets and symbol in exch.markets:
                market = exch.market(symbol)
                precision = market.get('precision', {}).get('price')
                if precision:
                    tick_size = float(precision)
        except Exception:
            pass
            
        for i in range(max_retries):
            try:
                if exch.markets and symbol in exch.markets:
                    current_price = float(exch.price_to_precision(symbol, current_price))
            except Exception:
                pass
            # 1. Place the Limit Order
            order = await exch.create_limit_order(symbol, side, qty, current_price, params)
            order_id = order['id']
            send_msg({"status": "INFO", "message": f"Chase Loop {i+1}: Placed {side} Limit at {current_price}"})
            
            # 2. Wait exactly 3 seconds
            await asyncio.sleep(3)
            
            # 3. Check Order Status
            fetched_order = await exch.fetch_order(order_id, symbol)
            if fetched_order['status'] in ['closed', 'canceled']:
                send_msg({"type": "HFT_ORDER_PLACED", "data": fetched_order})
                send_msg({"status": "INFO", "message": f"Chase Loop: Order filled or canceled externally. Done."})
                return
            
            # 4. If open, cancel and replace 1 tick higher/lower
            await exch.cancel_order(order_id, symbol)
            send_msg({"status": "WARN", "message": f"Chase Loop {i+1}: Order {order_id} unfilled. Canceling and replacing."})
            
            if side == 'buy':
                current_price += tick_size
            else:
                current_price -= tick_size
                
        send_msg({"status": "ERROR", "message": f"Limit Chase failed after {max_retries} retries for {symbol}."})
    except Exception as e:
        send_msg({"type": "HFT_ORDER_ERROR", "error": f"Limit chase error: {str(e)}"})

async def process_msg(line):
    global global_strategy_mode
    try:
        msg = json.loads(line)
        if not isinstance(msg, dict):
            return
        msg_type = msg.get("type")
        msg_data = msg.get("data", {})
        
        if msg_type == "EXCHANGE_KEYS":
            asyncio.create_task(init_exchange(msg_data.get("apiKey"), msg_data.get("secretKey"), msg_data.get("useTestnet", False)))
        elif msg_type == "OPTIONS_SNAPSHOT":
            if 'global_macro_analyzer' in globals() and global_macro_analyzer:
                global_macro_analyzer.update_options_from_stream(msg_data)
        elif msg_type == "AUTOPILOT_START":
            conviction_threshold = msg_data.get("convictionThreshold", 75)
            order_type = msg_data.get("orderType", "MARKET")
            max_risk = msg_data.get("maxRisk", 2.0)
            max_heat = msg_data.get("maxHeat", 20.0)
            
            # Apply risk rules
            if 'global_risk_manager' in globals() and global_risk_manager:
                # convert percentage (1.0 -> 0.01)
                global_risk_manager.max_risk_per_trade = float(max_risk) / 100.0
                global_risk_manager.max_portfolio_heat = float(max_heat) / 100.0
                
            autopilot_manager.conviction_threshold = conviction_threshold
            autopilot_manager.order_type = order_type
            autopilot_manager.risk_manager = global_risk_manager
            autopilot_manager.start()
                
            global_strategy_mode = "AUTOPILOT"
            async with symbol_states_lock:
                for s_name, s_state in symbol_states.items():
                    for engine_name in ["confluence_engine", "micro_engine", "macro_engine"]:
                        if engine_name in s_state and hasattr(s_state[engine_name], 'strategy_mode'):
                            s_state[engine_name].strategy_mode = "AUTOPILOT"
                        if engine_name in s_state and hasattr(s_state[engine_name], 'conviction_threshold'):
                            s_state[engine_name].conviction_threshold = conviction_threshold
        elif msg_type == "AUTOPILOT_STOP":
            autopilot_manager.stop()
            global_strategy_mode = "HYBRID"
            async with symbol_states_lock:
                for s_name, s_state in symbol_states.items():
                    for engine_name in ["confluence_engine", "micro_engine", "macro_engine"]:
                        if engine_name in s_state and hasattr(s_state[engine_name], 'strategy_mode'):
                            s_state[engine_name].strategy_mode = "HYBRID"
        elif msg_type == "RISK_OVERRIDES_UPDATE":
            if 'global_risk_manager' in globals() and global_risk_manager:
                parsed_overrides = {}
                for sym, vals in msg_data.items():
                    parsed_overrides[sym] = {
                        "maxHeat": float(vals.get("maxHeat", 5.0)) / 100.0,
                        "maxRisk": float(vals.get("maxRisk", 1.0)) / 100.0
                    }
                global_risk_manager.risk_overrides = parsed_overrides
        elif msg_type == "DROP_SYMBOL":
            symbol = msg_data.get("symbol")
            if symbol:
                async with symbol_states_lock:
                    if symbol in symbol_states:
                        del symbol_states[symbol]
                    if symbol in symbol_workers:
                        symbol_workers[symbol].cancel()
                        del symbol_workers[symbol]
                    if symbol in symbol_queues:
                        del symbol_queues[symbol]
        elif msg_type in ["CONFIG_UPDATE", "BALANCE_UPDATE", "EXECUTION_RULES_UPDATE"]:
            if msg_type == "CONFIG_UPDATE":
                global_strategy_mode = msg_data.get("mode", "HYBRID")
            for sym, q in symbol_queues.items():
                q.put_nowait(line)
        else:
            symbol = None
            if msg_type == "OPTIONS_FLOW":
                symbol = msg_data.get("symbol")
                un_sym = msg_data.get("underlying_symbol") or f"{symbol}USDT"
                if symbol:
                    await get_or_create_symbol_worker(symbol)
                    symbol_queues[symbol].put_nowait(line)
                if un_sym and un_sym != symbol:
                    await get_or_create_symbol_worker(un_sym)
                    symbol_queues[un_sym].put_nowait(line)
            else:
                if msg_type in ["DEPTH", "TRADE", "SEED_KLINES", "POSITION_STATE", "SIGNAL_REJECTED"]:
                    symbol = msg_data.get("symbol")
                if symbol:
                    await get_or_create_symbol_worker(symbol)
                    symbol_queues[symbol].put_nowait(line)
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        send_msg({"type": "ERROR", "message": f"Route Error: {str(e)}", "traceback": tb})

async def process_symbol_msg(s_main, line):
    global global_strategy_mode
    try:
        msg = json.loads(line)
        if not isinstance(msg, dict):
            return
        msg_type = msg.get("type")
        msg_data = msg.get("data", {})
        
        if isinstance(msg_data, list):
            msg_data = {"list_data": msg_data}
        elif not isinstance(msg_data, dict):
            msg_data = {}
        
        if msg_type == "EXCHANGE_KEYS":
            pass # Handled in router

        elif msg_type == "CONFIG_UPDATE":
            mode = msg_data.get("mode", "HYBRID")
            indicators_config = msg_data.get("indicators", [])
            global_strategy_mode = mode
            async with symbol_states_lock:
                for s_name, s_state in symbol_states.items():
                    for engine_name in ["confluence_engine", "micro_engine", "macro_engine"]:
                        if engine_name in s_state and hasattr(s_state[engine_name], 'strategy_mode'):
                            s_state[engine_name].strategy_mode = mode
                            
                    if indicators_config and "indicator_engine" in s_state:
                         s_state["indicator_engine"].update_config(indicators_config)
                         
                         # Trigger an immediate recalculation to update UI instantly without waiting for next candle close
                         for tf in s_state["candle_aggregator"].timeframes:
                             candles = s_state["candle_aggregator"].get_candles(tf)
                             if len(candles) > 0:
                                 new_indicators = s_state["indicator_engine"].calculate(candles)
                                 if new_indicators:
                                     s_state["last_indicators"][tf] = new_indicators
                                     send_msg({
                                         "type": "INDICATORS_UPDATE",
                                         "symbol": s_name,
                                         "tf": tf,
                                         "indicators": new_indicators,
                                         "ts": candles[-1].get("ts", 0)
                                     })

        elif msg_type == "EXECUTION_RULES_UPDATE":
            rules = msg_data.get("list_data", []) if isinstance(msg_data, dict) and "list_data" in msg_data else (msg_data if isinstance(msg_data, list) else [])
            async with symbol_states_lock:
                if s_main in symbol_states:
                    s_name = s_main
                    s_state = symbol_states[s_main]
                    for engine_name in ["confluence_engine", "shadow_confluence_engine"]:
                        if hasattr(s_state[engine_name], 'update_execution_rules'):
                            s_state[engine_name].update_execution_rules(rules)
                            
        elif msg_type == "DEPTH":
            data = msg_data
            symbol = data.get("symbol")
            if not symbol: return
            
            state = await get_or_create_symbol_state(symbol)
            
            # Update DOM and Buffers
            state["dom_tracker"].update(data)
            state["ob_buffer"].append(data)
            
            # Update Engines with price
            mid_price = data.get("mid_price", 0)
            state["last_price"] = mid_price
            state["confluence_engine"].update_state(symbol, price=mid_price)
            state["shadow_confluence_engine"].update_state(symbol, price=mid_price)
            state["micro_engine"].update_state(symbol, price=mid_price)
            state["macro_engine"].update_state(symbol, price=mid_price)
            
            # Calculate Analytics
            weighted_obi = state["dom_tracker"].get_weighted_imbalance(decay=0.15)
            walls = state["dom_tracker"].detect_walls(multiplier=8.0)
            spoof_events = state["spoof_filter"].analyze(walls, mid_price)
            
            state["confluence_engine"].update_state(symbol, spoof_events=spoof_events, book_imbalance=weighted_obi)
            state["shadow_confluence_engine"].update_state(symbol, spoof_events=spoof_events, book_imbalance=weighted_obi)
            state["micro_engine"].update_state(symbol, spoof_events=spoof_events, book_imbalance=weighted_obi)
            state["macro_engine"].update_state(symbol, spoof_events=spoof_events, book_imbalance=weighted_obi)
            
            send_msg({
                "type": "ANALYTICS_UPDATE",
                "symbol": symbol,
                "timestamp": data.get("timestamp", int(time.time() * 1000)),
                "weighted_obi": weighted_obi,
                "walls": walls,
                "spoof_events": spoof_events
            })

        elif msg_type == "SEED_KLINES":
            symbol = msg_data.get("symbol")
            tf = msg_data.get("tf")
            klines = msg_data.get("klines", [])
            if not symbol or not tf or not klines: return
            
            state = await get_or_create_symbol_state(symbol)
            # klines are assumed to be a list of {"o", "h", "l", "c", "v", "ts"}
            state["candle_aggregator"].seed_candles(tf, klines)
            
            # Immediately calculate indicators if enough candles are seeded
            candles = state["candle_aggregator"].get_candles(tf)
            new_indicators = state["indicator_engine"].calculate(candles)
            if new_indicators:
                state["last_indicators"][tf] = new_indicators
                
                # IMPORTANT: Feed immediate MTF state to engine so Auto Execution works right away
                state["confluence_engine"].update_state(symbol, candle=candles[-1], indicators=new_indicators, tf=tf, market_data=state.get("market_data"))
                state["shadow_confluence_engine"].update_state(symbol, candle=candles[-1], indicators=new_indicators, tf=tf, market_data=state.get("market_data"))
                state["micro_engine"].update_state(symbol, candle=candles[-1], indicators=new_indicators, tf=tf, market_data=state.get("market_data"))
                state["macro_engine"].update_state(symbol, candle=candles[-1], indicators=new_indicators, tf=tf, market_data=state.get("market_data"))
                
                send_msg({
                    "type": "INDICATORS_UPDATE",
                    "symbol": symbol,
                    "tf": tf,
                    "indicators": new_indicators,
                    "ts": klines[-1].get("ts", 0)
                })

        elif msg_type == "TRADE":
            data = msg_data
            symbol = data.get("symbol")
            trade = data.get("data", {})
            if not symbol or not trade: return
            
            state = await get_or_create_symbol_state(symbol)
            
            try:
                if isinstance(trade, list):
                    trade = trade[-1] if len(trade) > 0 else {}
                if not isinstance(trade, dict):
                    return
                price = float(trade.get("p", 0))
                qty = float(trade.get("q", 0))
            except (TypeError, ValueError, AttributeError):
                return
            
            ts = int(trade.get("T", 0))
            is_buyer_maker = bool(trade.get("m", False))
            side = "SELL" if is_buyer_maker else "BUY"
            
            # 1. Process Smart Tape
            aggregated = state["smart_tape"].process_trade(price, qty, ts, is_buyer_maker)
            
            # 2. Multi-Timeframe Candle Aggregation
            closed_candles = state["candle_aggregator"].process_tick(price, qty, ts)
            
            # SMC check mitigation on every tick
            for tf, s_engine in state["smc_engines"].items():
                if s_engine.check_mitigation(price, ts):
                    state["confluence_engine"].update_state(symbol, order_blocks=s_engine.order_blocks, fvgs=s_engine.fvgs if hasattr(s_engine, 'fvgs') else [], tf=tf)
                    state["shadow_confluence_engine"].update_state(symbol, order_blocks=s_engine.order_blocks, fvgs=s_engine.fvgs if hasattr(s_engine, 'fvgs') else [], tf=tf)
                    state["micro_engine"].update_state(symbol, order_blocks=s_engine.order_blocks, fvgs=s_engine.fvgs if hasattr(s_engine, 'fvgs') else [], tf=tf)
                    state["macro_engine"].update_state(symbol, order_blocks=s_engine.order_blocks, fvgs=s_engine.fvgs if hasattr(s_engine, 'fvgs') else [], tf=tf)
                    send_msg({
                        "type": "SMC_UPDATE",
                        "symbol": symbol,
                        "tf": tf,
                        "order_blocks": s_engine.order_blocks,
                        "fvgs": s_engine.fvgs if hasattr(s_engine, 'fvgs') else []
                    })

            for candle in closed_candles:
                tf = candle["tf"]
                
                # Append to Pandas DataFrame (strict 500 rows for live stream)
                if tf == "1m":
                     new_row = pd.DataFrame([{
                         "Timestamp": candle["ts"],
                         "Open": float(candle["o"]),
                         "High": float(candle["h"]),
                         "Low": float(candle["l"]),
                         "Close": float(candle["c"]),
                         "Volume": float(candle["v"])
                     }])
                     if state["market_data"].empty:
                         state["market_data"] = new_row
                     else:
                         state["market_data"] = pd.concat([state["market_data"], new_row], ignore_index=True)
                     if len(state["market_data"]) > 500:
                         state["market_data"] = state["market_data"].iloc[-500:].reset_index(drop=True)
                
                # Calculate indicators for this timeframe
                candles = state["candle_aggregator"].get_candles(tf)
                new_indicators = state["indicator_engine"].calculate(candles)
                
                # Update SMC Engine on candle close
                if state["smc_engines"][tf].update(candles, price):
                    state["confluence_engine"].update_state(symbol, order_blocks=state["smc_engines"][tf].order_blocks, fvgs=state["smc_engines"][tf].fvgs if hasattr(state["smc_engines"][tf], 'fvgs') else [], tf=tf)
                    state["shadow_confluence_engine"].update_state(symbol, order_blocks=state["smc_engines"][tf].order_blocks, fvgs=state["smc_engines"][tf].fvgs if hasattr(state["smc_engines"][tf], 'fvgs') else [], tf=tf)
                    state["micro_engine"].update_state(symbol, order_blocks=state["smc_engines"][tf].order_blocks, fvgs=state["smc_engines"][tf].fvgs if hasattr(state["smc_engines"][tf], 'fvgs') else [], tf=tf)
                    state["macro_engine"].update_state(symbol, order_blocks=state["smc_engines"][tf].order_blocks, fvgs=state["smc_engines"][tf].fvgs if hasattr(state["smc_engines"][tf], 'fvgs') else [], tf=tf)
                    send_msg({
                        "type": "SMC_UPDATE",
                        "symbol": symbol,
                        "tf": tf,
                        "order_blocks": state["smc_engines"][tf].order_blocks,
                        "fvgs": state["smc_engines"][tf].fvgs if hasattr(state["smc_engines"][tf], 'fvgs') else []
                    })
                
                
                if new_indicators:
                    prev_indicators = state["last_indicators"][tf]
                    signals = state["indicator_engine"].generate_signals(new_indicators, prev_indicators)
                    
                    # Add indicator data to the candle payload
                    candle["indicators"] = new_indicators
                    
                    for sig in signals:
                        sig_payload = {
                            "type": "STRATEGY_SIGNAL",
                            "symbol": symbol,
                            "direction": sig["direction"],
                            "confidence": 75, # Auto generated basic confidence
                            "signal_type": sig["type"],
                            "timeframe": tf,
                            "price": price,
                            "metadata": {
                                "reason": sig["reason"],
                                "value": sig["value"],
                                "rsi": new_indicators.get("rsi1"),
                                "supertrend": new_indicators.get("supertrend1")
                            }
                        }
                        send_msg(sig_payload)
                
                    state["last_indicators"][tf] = new_indicators
                    
                    send_msg({
                        "type": "INDICATORS_UPDATE",
                        "symbol": symbol,
                        "tf": tf,
                        "indicators": new_indicators,
                        "ts": candle["ts"]
                    })
                
                send_msg({
                    "type": "CANDLE_CLOSED",
                    "symbol": symbol,
                    "data": candle
                })
                # Feed closed candle into confluence engine for MTF analysis
                state["confluence_engine"].update_state(symbol, candle=candle, indicators=new_indicators, tf=tf, market_data=state.get("market_data"))
                state["shadow_confluence_engine"].update_state(symbol, candle=candle, indicators=new_indicators, tf=tf, market_data=state.get("market_data"))
                state["micro_engine"].update_state(symbol, candle=candle, indicators=new_indicators, tf=tf, market_data=state.get("market_data"))
                state["macro_engine"].update_state(symbol, candle=candle, indicators=new_indicators, tf=tf, market_data=state.get("market_data"))

            # 3. Update Tick Buffer
            state["tick_buffer"].append(price, qty, ts, is_buyer_maker)

            # 4. Detect Icebergs
            iceberg = state["iceberg_detector"].analyze(price, qty, side, state["dom_tracker"], ts)
            if iceberg:
                iceberg["symbol"] = symbol
                state["confluence_engine"].update_state(symbol, iceberg=iceberg)
                state["shadow_confluence_engine"].update_state(symbol, iceberg=iceberg)
                state["micro_engine"].update_state(symbol, iceberg=iceberg)
                state["macro_engine"].update_state(symbol, iceberg=iceberg)
                send_msg(iceberg)
            
            # 5. Analysis on finalized aggregated trade
            if aggregated:
                z_score = state["tick_buffer"].get_z_score(aggregated['qty'])
                market_profile = state["tick_buffer"].get_market_profile()
                
                state["confluence_engine"].update_state(symbol, cvd=aggregated['cvd'], volume=aggregated['qty'], z_score=z_score, price=price, market_profile=market_profile)
                state["shadow_confluence_engine"].update_state(symbol, cvd=aggregated['cvd'], volume=aggregated['qty'], z_score=z_score, price=price, market_profile=market_profile)
                state["micro_engine"].update_state(symbol, cvd=aggregated['cvd'], volume=aggregated['qty'], z_score=z_score, price=price, market_profile=market_profile)
                state["macro_engine"].update_state(symbol, price=price)
                
                send_msg({
                    "type": "TRADE_ANALYTICS",
                    "symbol": symbol,
                    "vwap": state["tick_buffer"].get_vwap(),
                    "cvd": aggregated['cvd'],
                    "aggregated_trade": aggregated,
                    "z_score": z_score,
                    "timestamp": ts
                })

        elif msg_type == "POSITION_STATE":
            symbol = msg_data.get("symbol")
            if symbol and symbol == s_main:
                state = await get_or_create_symbol_state(symbol)
                if msg_data.get("isShadow"):
                    state["shadow_confluence_engine"].sync_position(msg_data.get("active", False), msg_data.get("direction", "NONE"))
                else:
                    state["confluence_engine"].sync_position(msg_data.get("active", False), msg_data.get("direction", "NONE"))

        elif msg_type == "BALANCE_UPDATE":
            usdt_bal = msg_data.get("USDT", {})
            free = float(usdt_bal.get("free", 0.0))
            locked = float(usdt_bal.get("locked", 0.0))
            total = free + locked
            if total > 0:
                if s_main in symbol_states:
                    state = symbol_states[s_main]
                    state["confluence_engine"].update_balance(total)
                    state["shadow_confluence_engine"].update_balance(total)

        elif msg_type == "OPTIONS_FLOW":
            data = msg_data
            symbol = data.get("symbol")
            underlying_price = 0.0
            if s_main in symbol_states:
                underlying_price = symbol_states[s_main]["tick_buffer"].get_last_price()
            
            try:
                alerts = options_analyzer.analyze(data, underlying_price)
                for alert in alerts:
                    send_msg(alert)
                    
                    # 5. Gamma Exposure (GEX) & Options Sweeps -> Direct Hedging
                    if alert.get("type") == "OPTIONS_SWEEP_DETECTED" and alert.get("usd_value", 0) > 10_000_000:
                        if alert.get("option_type") == "PUT":
                            # Emergency short to protect inventory
                            hedge_sig = hedging_engine.emergency_delta_hedge(s_main, "SELL", alert.get("usd_value") * 0.05, underlying_price)
                            if hedge_sig:
                                send_msg(hedge_sig)

                    async with symbol_states_lock:
                        if s_main in symbol_states:
                            symbol_states[s_main]["confluence_engine"].update_state(s_main, sweep=alert)
                            symbol_states[s_main]["shadow_confluence_engine"].update_state(s_main, sweep=alert)
                            symbol_states[s_main]["micro_engine"].update_state(s_main, sweep=alert)
                            symbol_states[s_main]["macro_engine"].update_state(s_main, sweep=alert)
            except Exception as ae:
                send_msg({"type": "ERROR", "message": f"Options analysis error for {symbol}: {str(ae)}"})

        elif msg_type == "OPTIONS_SNAPSHOT":
            pass # Handled in router
        
        elif msg_type == "SIGNAL_REJECTED":
            symbol = msg_data.get("symbol")
            if symbol and symbol == s_main:
                async with symbol_states_lock:
                    if symbol in symbol_states:
                        s_state = symbol_states[symbol]
                        for engine_key in ["confluence_engine", "micro_engine", "macro_engine"]:
                            if hasattr(s_state[engine_key], 'handle_signal_rejection'):
                                s_state[engine_key].handle_signal_rejection()

        # Worker cycle run (isolated)
        async with symbol_states_lock:
            if s_main in symbol_states:
                s_state = symbol_states[s_main]
                
                # Check what type global_strategy_mode is (string or list)
                modes = global_strategy_mode if isinstance(global_strategy_mode, list) else [global_strategy_mode]
                
                if "HYBRID" in modes or "AUTOPILOT" in modes:
                    s_state["confluence_engine"].run_cycle()
                else:
                    if any(m in modes for m in ["MICRO", "MICRO_GEARS", "SCALP"]):
                        s_state["micro_engine"].run_cycle()
                    if any(m in modes for m in ["MACRO", "MACRO_TREND", "SWING"]):
                        s_state["macro_engine"].run_cycle()

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        send_msg({"type": "ERROR", "message": f"Worker Error for {s_main}: {str(e)}", "traceback": tb})

async def playback_data_feeder():
    import csv
    import os
    
    playback_file = "quant_engine/sample_tardis_trades.csv"
    if not os.path.exists(playback_file):
        dummy_trades = [
            {"timestamp": 1713000000000000, "side": "buy", "price": "65000.0", "amount": "0.1"},
            {"timestamp": 1713000001000000, "side": "buy", "price": "65010.0", "amount": "2.5"},
            {"timestamp": 1713000002000000, "side": "buy", "price": "65050.0", "amount": "5.0"},
            {"timestamp": 1713000003000000, "side": "sell", "price": "65040.0", "amount": "0.1"},
            {"timestamp": 1713000100000000, "side": "buy", "price": "66000.0", "amount": "1.0"},
        ]
        with open(playback_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "side", "price", "amount"])
            writer.writeheader()
            writer.writerows(dummy_trades)

    while True:
        try:
            modes = global_strategy_mode if isinstance(global_strategy_mode, list) else [global_strategy_mode]
            if "PLAYBACK" in modes:
                with open(playback_file, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        modes = global_strategy_mode if isinstance(global_strategy_mode, list) else [global_strategy_mode]
                        if "PLAYBACK" not in modes:
                            break
                        
                        ts = int(row.get('timestamp', 0)) // 1000 
                        price = float(row.get('price', 0))
                        qty = float(row.get('amount', 0))
                        side = row.get('side', '').upper()
                        
                        target_symbol = "BTC/USDT"
                        trade_data = {
                            "symbol": target_symbol,
                            "data": {
                                "p": str(price),
                                "q": str(qty),
                                "T": ts,
                                "m": (side == "SELL")
                            }
                        }
                        await process_msg(json.dumps({"type": "TRADE", "data": trade_data}))
                        await asyncio.sleep(0.5) 
            await asyncio.sleep(2)
        except Exception as e:
            logging.error(f"Playback feeder error: {e}")
            await asyncio.sleep(2)

async def alpha_worker():
    global global_strategy_mode
    while True:
        try:
            # Check what type global_strategy_mode is (string or list)
            modes = global_strategy_mode if isinstance(global_strategy_mode, list) else [global_strategy_mode]

            # If ARBITRAGE and GRID are not in modes, we can skip and sleep
            run_arb = any(m in modes for m in ["ARBITRAGE", "ARB", "HYBRID"])
            run_grid = any(m in modes for m in ["GRID", "HYBRID"])

            if not run_arb and not run_grid:
                await asyncio.sleep(10)
                continue

            symbols_to_check = []
            async with symbol_states_lock:
                # Snapshot the state
                for sym, state in symbol_states.items():
                    symbols_to_check.append((sym, state.get("last_price", 0)))
            
            for symbol, price in symbols_to_check:
                if price <= 0:
                    continue
                    
                if run_arb:
                    # 1. Delta-Neutral Hedging Check
                    hedge_sig = await hedging_engine.generate_hedge_signal(symbol, 1000, price)
                    if hedge_sig:
                        send_msg(hedge_sig)

                    # 2. Spot-Futures Arbitrage
                    sf_arb = await arbitrage_engine.check_spot_futures_arbitrage(symbol)
                    if sf_arb:
                        send_msg(sf_arb)

                    # 3. Triangular Arbitrage (using BTC and USDT as base/quote and ETH as inter for arbitrary pairs)
                    if symbol == "BTC/USDT":
                        tri_arb = await arbitrage_engine.check_triangular_arbitrage(base="BTC", quote="USDT", intermediate="ETH")
                        if tri_arb:
                            send_msg(tri_arb)

                if run_grid:
                    # 4. Grid Trading Generation
                    if symbol not in grid_engine.active_grids:
                        # Real Grid Execution
                        grid_signals = grid_engine.init_grid(symbol, price, grid_levels=5, step_pct=0.005, capital=1000)
                        for grid_signal in grid_signals:
                            send_msg(grid_signal)
            
            await asyncio.sleep(30) # Run every 30s
        except Exception as e:
            logging.error(f"Alpha worker error: {e}")
            await asyncio.sleep(10)

async def binance_ws_loop():
    uri = "wss://stream.binance.com:9443/stream"
    subscribed_symbols = set()
    ws_conn = None
    
    async def subscribe_new():
        nonlocal ws_conn
        while True:
            current_symbols = set(symbol_states.keys())
            new_symbols = current_symbols - subscribed_symbols
            if new_symbols and ws_conn:
                streams = [f"{s.replace('/', '').lower()}@depth20@100ms" for s in new_symbols] + [f"{s.replace('/', '').lower()}@aggTrade" for s in new_symbols]
                subscribe_msg = {
                    "method": "SUBSCRIBE",
                    "params": streams,
                    "id": 1
                }
                try:
                    await ws_conn.send(json.dumps(subscribe_msg))
                    subscribed_symbols.update(new_symbols)
                except Exception:
                    pass
            await asyncio.sleep(2)

    asyncio.create_task(subscribe_new())
    
    while True:
        try:
            async with websockets.connect(uri) as ws:
                ws_conn = ws
                subscribed_symbols.clear() # Request resubscription
                while True:
                    msg = await ws.recv()
                    raw = json.loads(msg)
                    if 'stream' in raw and 'data' in raw:
                        stream_name = raw['stream']
                        data = raw['data']
                        symbols_list = list(symbol_states.keys())
                        
                        target_symbol = None
                        for s in symbols_list:
                            if stream_name.startswith(s.replace('/', '').lower() + '@'):
                                target_symbol = s
                                break
                        
                        if not target_symbol:
                            continue
                            
                        modes = global_strategy_mode if isinstance(global_strategy_mode, list) else [global_strategy_mode]
                        if "PLAYBACK" in modes:
                            continue

                        if '@depth' in stream_name:
                            depth_data = {
                                "symbol": target_symbol,
                                "bids": [[float(p), float(q)] for p, q in data.get('bids', [])],
                                "asks": [[float(p), float(q)] for p, q in data.get('asks', [])],
                                "timestamp": data.get('E', int(time.time() * 1000))
                            }
                            if depth_data["bids"] and depth_data["asks"]:
                                depth_data["mid_price"] = (depth_data["bids"][0][0] + depth_data["asks"][0][0]) / 2
                            await process_msg(json.dumps({"type": "DEPTH", "data": depth_data}))
                        elif '@aggTrade' in stream_name:
                            trade_data = {
                                "symbol": target_symbol,
                                "data": {
                                    "p": data.get('p', '0'),
                                    "q": data.get('q', '0'),
                                    "T": data.get('T', 0),
                                    "m": data.get('m', False)
                                }
                            }
                            await process_msg(json.dumps({"type": "TRADE", "data": trade_data}))
        except Exception as e:
            ws_conn = None
            await asyncio.sleep(5)

async def ipc_handler(sub_socket, push_socket):
    async def ipc_writer():
        while True:
            try:
                msg = await outbound_queue.get()
                await push_socket.send_string(msg)
            except Exception as e:
                logging.error(f"IPC Writer error: {e}")
                await asyncio.sleep(1)

    asyncio.create_task(ipc_writer())
    send_msg({"status": "READY", "message": "Async Python Quant Engine Initialized"})

    while True:
        try:
            line_str = await sub_socket.recv_string()
            asyncio.create_task(process_msg(line_str))
        except Exception as e:
            logging.error(f"IPC Handler error: {e}")
            break

async def risk_reporting_worker():
    while True:
        try:
            total_risk = 0
            for tid, tdata in global_risk_manager.active_trades.items():
                total_risk += tdata.get("risk_amount", 0)
                
            heat = total_risk / global_risk_manager.account_size if global_risk_manager.account_size > 0 else 0
            drawdown = (global_risk_manager.daily_high_water_mark - global_risk_manager.account_size) / global_risk_manager.daily_high_water_mark if global_risk_manager.daily_high_water_mark > 0 else 0
            
            send_msg({
                "type": "RISK_STATE_UPDATE",
                "data": {
                    "account_size": global_risk_manager.account_size,
                    "drawdown_pct": round(drawdown * 100, 4),
                    "portfolio_heat_pct": round(heat * 100, 2),
                    "active_sectors": list(global_risk_manager.active_sectors),
                    "killswitch_active": global_risk_manager.killswitch_active,
                    "max_daily_drawdown": global_risk_manager.max_daily_drawdown,
                    "max_portfolio_heat": global_risk_manager.max_portfolio_heat,
                    "open_trades_count": len(global_risk_manager.active_trades)
                }
            })
        except Exception as e:
            pass
        await asyncio.sleep(5)

async def main():
    global main_loop
    global global_macro_analyzer
    main_loop = asyncio.get_running_loop()
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--pub-port', type=int, required=True)
    parser.add_argument('--pull-port', type=int, required=True)
    args = parser.parse_args()
    
    global_macro_analyzer = MacroRegimeAnalyzer(on_macro_update)
    global_macro_analyzer.start()
    
    asyncio.create_task(alpha_worker())
    asyncio.create_task(playback_data_feeder())
    asyncio.create_task(autopilot_manager.run_loop())
    asyncio.create_task(risk_reporting_worker())
    
    asyncio.create_task(binance_ws_loop())
    
    ctx = zmq.asyncio.Context()
    sub_socket = ctx.socket(zmq.SUB)
    sub_socket.connect(f"tcp://127.0.0.1:{args.pub_port}")
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")

    push_socket = ctx.socket(zmq.PUSH)
    push_socket.connect(f"tcp://127.0.0.1:{args.pull_port}")

    await ipc_handler(sub_socket, push_socket)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Graceful shutdown received (Ctrl+C). Cleaning up resources...")
        if exchange:
            # Emulate the client.close_connection() required by the Prompt by closing CCXT
            try:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(exchange.close())
            except Exception:
                pass
        print("Bot shutdown complete.")

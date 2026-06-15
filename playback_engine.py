import csv
import json
import os
import sys
import argparse
from strategy_confluence import MicroGearsEngine, ConfluenceEngine
from buffers import TickDataRingBuffer, DOMTracker

# ==============================================================================
# TARDIS.DEV HISTORICAL PLAYBACK ENGINE
# ==============================================================================
# This simulator allows you to benchmark your MicroGears and Confluence engines
# against historical, institutional-grade tick data.
#
# INSTRUCTIONS:
# 1. Download sample CSVs from Tardis.dev (Trades & Derivative Tick).
# 2. Run this script pointing to the trades file:
#    python3 playback_engine.py --trades /path/to/tardis_trades.csv
#
# IMPORTANT: This runs locally without Binance API limits or WebSocket latency.
# It simulates entries, Stop Losses, and Take Profits.
# ==============================================================================

class TardisSimulator:
    def __init__(self, trades_file, depth_file):
        self.trades_file = trades_file
        self.depth_file = depth_file
        
        self.tick_buffer = TickDataRingBuffer(capacity=1000)
        self.dom_tracker = DOMTracker(depth_levels=20)
        self.micro_engine = MicroGearsEngine()
        self.confluence_engine = ConfluenceEngine()
        
        # PnL Tracker
        self.position = None
        self.entry_price = 0.0
        self.pnl = 0.0
        self.total_trades = 0
        self.wins = 0

    def load_and_run(self):
        print(f"Starting Tardis Playback Simulation...")
        print(f"Trades File: {self.trades_file}")
        print(f"Depth File: {self.depth_file}")

        # In a real environment, you'd merge the CSV streams chronologically.
        # For simplicity in this backtester, we will process trades to mock execution 
        # but realistically, Tardis sets contain millisecond timestamps for both.
        
        if not os.path.exists(self.trades_file):
            print(f"Error: Trade file {self.trades_file} not found. (Provide Tardis or similar CSV)")
            return

        with open(self.trades_file, 'r') as f:
            reader = csv.DictReader(f)
            # Expected tardis cols: timestamp, side, price, amount
            
            for row in reader:
                try:
                    ts = int(row.get('timestamp', 0)) // 1000 # Convert to ms if it's microsec
                    price = float(row.get('price', 0))
                    qty = float(row.get('amount', 0))
                    side = row.get('side', '').upper()
                    
                    if not side or qty == 0: continue

                    # 1. Feed the buffer
                    self.tick_buffer.append(price, qty, ts)
                    
                    # 2. Feed the Micro Strategy
                    # To accurately trigger, we need aggregated trades.
                    z_score = self.tick_buffer.get_z_score(qty)
                    
                    # Instead of running the full complex pipeline, we just update the MicroEngine
                    # with the latest parameters
                    self.micro_engine.update_state("BTCUSDT", price=price, z_score=z_score)
                    signal = self.micro_engine.run_cycle()

                    if signal:
                        self._process_signal(signal, price)
                        
                    # Stop loss/Take profit tracking
                    self._check_position(price)

                except Exception as e:
                    pass

        print("\n=== Backtest Complete ===")
        print(f"Total Trades: {self.total_trades}")
        print(f"Win Rate: {(self.wins/self.total_trades * 100):.2f}%" if self.total_trades > 0 else "0.0%")
        print(f"Net PnL: {self.pnl:.2f} points")

    def _process_signal(self, signal, current_price):
        action = signal.get("action")
        if action in ["BUY", "SELL"] and not self.position:
            self.position = "LONG" if action == "BUY" else "SHORT"
            self.entry_price = current_price
            self.total_trades += 1
            print(f"ENTRY {self.position} @ {current_price}")

    def _check_position(self, current_price):
        if not self.position: return
        
        # Simple 0.5% TP and 0.25% SL simulator
        if self.position == "LONG":
            if current_price >= self.entry_price * 1.005:
                self._exit("WIN", current_price)
            elif current_price <= self.entry_price * 0.9975:
                self._exit("LOSS", current_price)
        elif self.position == "SHORT":
            if current_price <= self.entry_price * 0.995:
                self._exit("WIN", current_price)
            elif current_price >= self.entry_price * 1.0025:
                self._exit("LOSS", current_price)

    def _exit(self, result, exit_price):
        gross_pnl = (exit_price - self.entry_price) if self.position == "LONG" else (self.entry_price - exit_price)
        self.pnl += gross_pnl
        if result == "WIN": self.wins += 1
        
        print(f"EXIT {self.position} @ {exit_price} | PnL: {gross_pnl:.2f}")
        self.position = None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--trades", required=True, help="Path to Tardis trades CSV")
    parser.add_argument("--depth", required=False, help="Path to Tardis derivative book derivative CSV")
    args = parser.parse_args()
    
    sim = TardisSimulator(args.trades, args.depth)
    sim.load_and_run()

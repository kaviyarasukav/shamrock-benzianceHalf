import ccxt.async_support as ccxt
import time
import json
import logging

class DeltaNeutralHedgingEngine:
    """
    Phase 5.2: Delta-Neutral Hedging
    Safe, server-based hedging to collect funding rates without directional risk.
    """
    def __init__(self, exchange_spot_id='binance', exchange_futures_id='binanceusdm'):
        self.spot_exchange = getattr(ccxt, exchange_spot_id)({'enableRateLimit': True})
        self.futures_exchange = getattr(ccxt, exchange_futures_id)({'enableRateLimit': True})
        self.min_apr_threshold = 0.10 # 10% annualized threshold to enter
        self.positions = {}
        
    async def analyze_funding_rates(self, symbol="BTC/USDT"):
        """
        Analyzes the funding rate of a perpetual contract.
        In CCXT, the fetch_funding_rate will yield the current funding rate.
        """
        try:
            # If symbol comes in as BTCUSDT, add the slash for ccxt
            formatted_symbol = symbol
            if "/" not in formatted_symbol and formatted_symbol.endswith("USDT"):
                formatted_symbol = formatted_symbol[:-4] + "/USDT"
                
            funding_info = await self.futures_exchange.fetch_funding_rate(formatted_symbol.replace('USDT', 'USDT:USDT'))
            funding_rate = funding_info.get('fundingRate', 0)
            
            # Approximate annualized yield assuming 8h funding
            annualized_yield = funding_rate * 3 * 365 
            
            return {
                "symbol": symbol,
                "funding_rate": funding_rate,
                "annualized_yield": annualized_yield,
                "action": "ENTER_HEDGE" if annualized_yield >= self.min_apr_threshold else "WAIT"
            }
        except ccxt.BadSymbol:
            return None
        except Exception as e:
            if "does not have market symbol" in str(e):
                return None
            logging.error(f"Error fetching funding rate for {symbol}: {e}")
            return None

    def calculate_hedge_size(self, capital, current_price):
        """
        Calculates the amount of asset to buy on Spot and short on Futures
        capital is divided by 2 (half to spot, half to futures margin).
        """
        spot_capital = capital / 2
        asset_qty = spot_capital / current_price
        return asset_qty

    def emergency_delta_hedge(self, symbol, action, qty_usd, current_price):
        """
        Phase 5: Emergency Delta Hedge against massive Options Sweeps.
        Aggressively shorts Spot or Futures to protect long inventory.
        """
        if current_price <= 0:
            return None
            
        qty = qty_usd / current_price
        return {
            "type": "HEDGE_ACTION",
            "symbol": symbol,
            "action": "EXECUTE",
            "target_qty": qty,
            "estimated_hedge": f"Gamma protection: {qty_usd}",
            "legs": [
                {"market": "FUTURES", "side": action, "qty": qty}
            ],
            "metadata": {
                "reason": f"Options Sweep Emergency Delta Hedge: {qty_usd} USD"
            }
        }

    async def generate_hedge_signal(self, symbol, capital, current_price):
        analysis = await self.analyze_funding_rates(symbol)
        if not analysis:
            return None
        
        if analysis['action'] == 'ENTER_HEDGE' and symbol not in self.positions:
            qty = self.calculate_hedge_size(capital, current_price)
            self.positions[symbol] = {
                "status": "HEDGED",
                "qty": qty,
                "entry_price": current_price
            }
            # Emit signals to Node.js broker to execute
            return {
                "type": "HEDGE_ACTION",
                "symbol": symbol,
                "action": "ENTER",
                "target_qty": qty,
                "expected_yield": analysis['annualized_yield'],
                "legs": [
                    {"market": "SPOT", "side": "BUY", "qty": qty},
                    {"market": "FUTURES", "side": "SELL", "qty": qty}
                ]
            }
            
        elif analysis['action'] == 'WAIT' and symbol in self.positions:
            # Check if we should exit (if yield goes negative)
            if analysis['annualized_yield'] < 0:
                qty = self.positions[symbol]["qty"]
                del self.positions[symbol]
                return {
                    "type": "HEDGE_ACTION",
                    "symbol": symbol,
                    "action": "EXIT",
                    "legs": [
                        {"market": "SPOT", "side": "SELL", "qty": qty},
                        {"market": "FUTURES", "side": "BUY", "qty": qty, "reduce_only": True}
                    ]
                }
        return None

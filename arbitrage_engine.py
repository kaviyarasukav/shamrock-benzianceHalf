import ccxt.async_support as ccxt
import time
import logging

class ArbitrageEngine:
    """
    Phase 5.1: Arbitrage Engine
    Identifies arbitrage opportunities like Spot-Futures basis, Triangular Arbitrage, Cross-exchange Arbitrage.
    """
    def __init__(self, primary_exchange='binance', secondary_exchange='kraken'):
        self.ex_primary = getattr(ccxt, primary_exchange)({'enableRateLimit': True})
        self.ex_secondary = getattr(ccxt, secondary_exchange)({'enableRateLimit': True})
        self.ex_futures = getattr(ccxt, f"{primary_exchange}usdm")({'enableRateLimit': True}) if hasattr(ccxt, f"{primary_exchange}usdm") else None

    async def check_spot_futures_arbitrage(self, symbol="BTC/USDT"):
        """
        Spot-Futures arbitrage (Cash and Carry).
        Checks if Futures price is significantly higher than Spot price.
        """
        try:
            if not self.ex_futures:
                return None
            
            # Formatter for CCXT 
            formatted_symbol = symbol
            if "/" not in formatted_symbol and formatted_symbol.endswith("USDT"):
                formatted_symbol = formatted_symbol[:-4] + "/USDT"
            
            futures_symbol = formatted_symbol.replace('USDT', 'USDT:USDT')
            
            # Since fetch_ticker might need formatted_symbol too:
            spot_ticker = await self.ex_primary.fetch_ticker(formatted_symbol)
            futures_ticker = await self.ex_futures.fetch_ticker(futures_symbol)
            
            spot_price = spot_ticker['last']
            futures_price = futures_ticker['last']
            
            spread_pct = (futures_price - spot_price) / spot_price
            
            threshold = 0.005 # 0.5%
            
            if spread_pct > threshold:
                return {
                    "type": "ARB_ACTION",
                    "subtype": "SPOT_FUTURES",
                    "symbol": symbol,
                    "spread_pct": spread_pct,
                    "spot_price": spot_price,
                    "futures_price": futures_price,
                    "action": "EXECUTE",
                    "legs": [
                        {"market": "SPOT", "side": "BUY"},
                        {"market": "FUTURES", "side": "SELL"}
                    ]
                }
        except ccxt.BadSymbol:
            return None
        except Exception as e:
            if "does not have market symbol" in str(e):
                return None
            logging.error(f"Spot-Futures Arb Error: {e}")
        return None

    async def check_cross_exchange_arbitrage(self, symbol="BTC/USDT"):
        """
        Cross-exchange Arbitrage (e.g. Binance vs Kraken).
        """
        try:
            formatted_symbol = symbol
            if "/" not in formatted_symbol and formatted_symbol.endswith("USDT"):
                formatted_symbol = formatted_symbol[:-4] + "/USDT"
                
            ticker_a = await self.ex_primary.fetch_ticker(formatted_symbol)
            ticker_b = await self.ex_secondary.fetch_ticker(formatted_symbol)
            
            price_a = ticker_a['last']
            price_b = ticker_b['last']
            
            spread_pct = abs(price_a - price_b) / min(price_a, price_b)
            threshold = 0.005 # 0.5% threshold to account for transfer fees
            
            if spread_pct > threshold:
                buy_exchange = 'PRIMARY' if price_a < price_b else 'SECONDARY'
                sell_exchange = 'SECONDARY' if price_a < price_b else 'PRIMARY'
                
                return {
                    "type": "ARB_ACTION",
                    "subtype": "CROSS_EXCHANGE",
                    "symbol": symbol,
                    "spread_pct": spread_pct,
                    "buy_on": buy_exchange,
                    "sell_on": sell_exchange,
                    "action": "ALERT_ONLY"
                }
        except Exception as e:
            logging.error(f"Cross-Exchange Arb Error: {e}")
        return None

    async def check_triangular_arbitrage(self, base="BTC", quote="USDT", intermediate="ETH"):
        """Triangular arbitrage on a single exchange."""
        # e.g., USDT -> BTC -> ETH -> USDT
        pair1 = f"{base}/{quote}"
        pair2 = f"{intermediate}/{base}"
        pair3 = f"{intermediate}/{quote}"
        
        try:
            ticker1 = await self.ex_primary.fetch_ticker(pair1)
            ticker2 = await self.ex_primary.fetch_ticker(pair2)
            ticker3 = await self.ex_primary.fetch_ticker(pair3)
            
            p1 = ticker1['ask']
            p2 = ticker2['ask']
            p3 = ticker3['bid']
            
            # Start with 1 Quote (USDT)
            step1 = 1 / p1 # BTC
            step2 = step1 / p2 # ETH
            step3 = step2 * p3 # USDT
            
            profit_pct = (step3 - 1)
            if profit_pct > 0.002: # 0.2% fee threshold
                return {
                    "type": "ARB_ACTION",
                    "subtype": "TRIANGULAR",
                    "path": f"{quote}->{base}->{intermediate}->{quote}",
                    "profit_pct": profit_pct,
                    "action": "EXECUTE",
                    "legs": [
                        {"symbol": f"{base}{quote}", "side": "BUY", "market": "SPOT", "type": "MARKET"},
                        {"symbol": f"{intermediate}{base}", "side": "BUY", "market": "SPOT", "type": "MARKET"},
                        {"symbol": f"{intermediate}{quote}", "side": "SELL", "market": "SPOT", "type": "MARKET"}
                    ]
                }
        except Exception as e:
            logging.error(f"Triangular Arb Error: {e}")
        return None

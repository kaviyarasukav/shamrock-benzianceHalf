import logging

class GridEngine:
    """
    Phase 5.3: Grid-based Buy/Sell Algorithms.
    Generates passive grid limits to profit from ranging sideways markets.
    """
    def __init__(self):
        self.active_grids = {} # symbol -> grid configuration

    def init_grid(self, symbol, current_price, grid_levels=5, step_pct=0.01, capital=1000):
        """
        Calculates ladder entry and exit setups given a capital allocation.
        """
        buy_orders = []
        sell_orders = []
        
        capital_per_level = capital / (grid_levels * 2)

        for i in range(1, grid_levels + 1):
            buy_price = current_price * (1 - (step_pct * i))
            sell_price = current_price * (1 + (step_pct * i))

            buy_orders.append({
                "market": "SPOT",
                "side": "BUY",
                "price": buy_price,
                "qty_usd": capital_per_level
            })
            sell_orders.append({
                "market": "SPOT",
                "side": "SELL",
                "price": sell_price,
                "qty_usd": capital_per_level
            })

        self.active_grids[symbol] = {
            "center": current_price,
            "buys": buy_orders,
            "sells": sell_orders
        }

        # Return signals to order_router (which will break them into laddered limit entries)
        return [
            {
                "type": "STRATEGY_SIGNAL",
                "symbol": symbol,
                "direction": "LONG", # just to trigger BUYs
                "strategy_id": "GRID_BOT",
                "order_type": "LIMIT",
                "price": current_price,
                "weight": 0.5, # percentage of portfolio
                "metadata": {
                     "ladder_steps": grid_levels,
                     "ladder_step_pct": step_pct,
                     "market": "SPOT"
                }
            },
            {
                "type": "STRATEGY_SIGNAL",
                "symbol": symbol,
                "direction": "SHORT", # to trigger SELLs
                "strategy_id": "GRID_BOT",
                "order_type": "LIMIT",
                "price": current_price,
                "weight": 0.5, # percentage of portfolio
                "metadata": {
                     "ladder_steps": grid_levels,
                     "ladder_step_pct": step_pct,
                     "market": "SPOT"
                }
            }
        ]

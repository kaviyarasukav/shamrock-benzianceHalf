import time
import threading
import datetime
import json
import os
import numpy as np
try:
    import requests
    import feedparser
    import yfinance as yf
    import ccxt
except ImportError:
    requests = None
    feedparser = None
    yf = None
    ccxt = None

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
except ImportError:
    SentimentIntensityAnalyzer = None

class MacroRegimeAnalyzer:
    def __init__(self, callback):
        self.callback = callback
        self.running = False
        self.thread = None
        
        # State
        self.dxy_z_score_30d = 0.0
        self.yield_z_score_30d = 0.0
        self.options_data = {"put_call": 0.0, "iv": 0.0}
        
        self.current_dxy = 0.0
        self.current_yield = 0.0
        self.current_short_yield = 0.0 # US 3M/2Y Yield proxy
        self.yield_spread = 0.0 # 10Y minus Short Yield
        self.dxy_z_score = 0.0  # Represents 14d
        self.yield_z_score = 0.0  # Represents 14d
        
        self.sentiment_history = [] # Tuples of (timestamp, score)
        self.seen_news = set()
        self.sentiment_score = 0.0
        
        # Institutional COT Data (Commitment of Traders proxy)
        self.cot_long_short_ratio = 1.0
        
        # Alternative 2: Trend Funding & Correlation
        self.funding_rate = 0.0
        self.dxy_correlation = 0.0
        self.ndx_momentum = 0.0
        self.ndx_correlation = 0.0
        self.hurst_exponent = 0.5
        
        self.upcoming_events = []
        self.killswitch_active = False
        self.regime = "CHOP"

        # Persistence setup
        self.cache_file = os.path.join(os.getcwd(), 'quant_engine/macro_state_v1.json')
        self._load_persistent_state()

        # NLP Setup
        if SentimentIntensityAnalyzer:
            self.analyzer = SentimentIntensityAnalyzer()
        else:
            self.analyzer = None

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()

    def _load_persistent_state(self):
        """Hydrates the analyzer state from a local file to survive restarts and API blocks."""
        if not os.path.exists(self.cache_file):
            return
        
        try:
            with open(self.cache_file, 'r') as f:
                state = json.load(f)
                self.current_dxy = state.get('current_dxy', self.current_dxy)
                self.current_yield = state.get('current_yield', self.current_yield)
                self.current_short_yield = state.get('current_short_yield', self.current_short_yield)
                self.yield_spread = state.get('yield_spread', self.yield_spread)
                self.dxy_z_score = state.get('dxy_z_score', self.dxy_z_score)
                self.dxy_z_score_30d = state.get('dxy_z_score_30d', self.dxy_z_score_30d)
                self.yield_z_score = state.get('yield_z_score', self.yield_z_score)
                self.yield_z_score_30d = state.get('yield_z_score_30d', self.yield_z_score_30d)
                self.dxy_correlation = state.get('dxy_correlation', self.dxy_correlation)
                self.sentiment_score = state.get('sentiment_score', self.sentiment_score)
                self.funding_rate = state.get('funding_rate', self.funding_rate)
                self.cot_long_short_ratio = state.get('cot_long_short_ratio', self.cot_long_short_ratio)
                self.ndx_momentum = state.get('ndx_momentum', self.ndx_momentum)
                self.ndx_correlation = state.get('ndx_correlation', self.ndx_correlation)
                self.hurst_exponent = state.get('hurst_exponent', self.hurst_exponent)
                print(json.dumps({"type": "STATUS", "message": f"Macro state hydrated from {self.cache_file}"}), flush=True)
        except Exception as e:
            print(json.dumps({"type": "ERROR", "message": f"Failed to load persistent macro state: {str(e)}"}), flush=True)

    def _save_persistent_state(self):
        """Persists the current macro metrics to a local file."""
        try:
            state = {
                'current_dxy': self.current_dxy,
                'current_yield': self.current_yield,
                'current_short_yield': self.current_short_yield,
                'yield_spread': self.yield_spread,
                'dxy_z_score': self.dxy_z_score,
                'dxy_z_score_30d': self.dxy_z_score_30d,
                'yield_z_score': self.yield_z_score,
                'yield_z_score_30d': self.yield_z_score_30d,
                'dxy_correlation': self.dxy_correlation,
                'sentiment_score': self.sentiment_score,
                'funding_rate': self.funding_rate,
                'cot_long_short_ratio': self.cot_long_short_ratio,
                'ndx_momentum': self.ndx_momentum,
                'ndx_correlation': self.ndx_correlation,
                'hurst_exponent': self.hurst_exponent,
                'updated_at': datetime.datetime.utcnow().isoformat()
            }
            with open(self.cache_file, 'w') as f:
                json.dump(state, f)
        except Exception as e:
            pass # Silent fail on save to prevent loop disruption

    def _run_loop(self):
        # Initial boot up sleep to let main threads start without starving resources
        time.sleep(2) # Reduced for faster UI feedback
        
        # Hydrate initial historical data for Z-scores
        self._hydrate_historical_data()
        
        iteration = 0
        last_emit_time = 0
        
        while self.running:
            try:
                # Force update on iteration 0, then every 300 iterations (5 mins)
                if iteration == 0 or iteration % 300 == 0:
                    self._update_markets()
                    self._update_news_sentiment()
                    self._update_calendar()
                    self._update_cot_data() 
                    self._update_funding_rate() 
                    self._save_persistent_state()
                
                # Check killswitch instantly in fast loop
                self._check_killswitch()
                self._evaluate_regime()
                
                # Emit status immediately on iteration 0, then throttle to 10s
                now = time.time()
                if iteration == 0 or self.regime == "SHOCK" or (now - last_emit_time) >= 10:
                    self._emit_status()
                    last_emit_time = now
                
                iteration += 1
                time.sleep(1) # Base tick
                
            except Exception as e:
                time.sleep(5)

    def update_options_from_stream(self, snapshot):
        """
        Pure WebSocket Ingestion for GEX (Gamma/PCR/IV).
        Replaces REST polling with real-time exchange data.
        """
        try:
            pcr = snapshot.get("put_call_ratio", self.options_data.get("put_call"))
            iv = snapshot.get("implied_volatility", self.options_data.get("iv"))
            self.options_data = {"put_call": round(pcr, 4), "iv": round(iv, 2)}
        except:
            pass # Silent fail to prevent crashing the worker thread

    def _hydrate_historical_data(self):
        # We handle complete historical hydration natively in _update_markets now
        # using the 1mo and 1d aggregations to ensure true 14-day and 30-day Z-scores.
        pass

    def _update_markets(self):
        if not yf: 
            print(json.dumps({"type": "WARNING", "message": "yfinance not found. Using fallback mock data."}), flush=True)
            self._use_fallback_markets()
            return

        try:
            # DXY 14-Day and 30-Day Z-Scores
            dxy_ticker = yf.Ticker("DX-Y.NYB")
            # Using period 1mo for efficiency
            dxy_hist = dxy_ticker.history(period="1mo")
            
            if dxy_hist.empty:
                 # Attempt a different ticker as fallback for DXY
                 dxy_ticker = yf.Ticker("UUP") # Invesco DB US Dollar Index Bullish Fund
                 dxy_hist = dxy_ticker.history(period="1mo")

            if not dxy_hist.empty and 'Close' in dxy_hist:
                closes = dxy_hist['Close'].dropna().values
                if len(closes) > 0:
                    self.current_dxy = float(closes[-1])
                    dxy_14 = closes[-14:] if len(closes) >= 14 else closes
                    dxy_30 = closes[-30:] if len(closes) >= 30 else closes
                    
                    if len(dxy_14) > 1 and np.std(dxy_14) > 0:
                        self.dxy_z_score = (self.current_dxy - np.mean(dxy_14)) / np.std(dxy_14)
                    if len(dxy_30) > 1 and np.std(dxy_30) > 0:
                        self.dxy_z_score_30d = (self.current_dxy - np.mean(dxy_30)) / np.std(dxy_30)
            else:
                self._use_fallback_dxy()
            
            # US10Y 14-Day and 30-Day Z-Scores
            tnx_ticker = yf.Ticker("^TNX")
            tnx_hist = tnx_ticker.history(period="1mo")
            
            # Short-Term Yield (13-Week Proxy for Yield Curve Spread)
            irx_ticker = yf.Ticker("^IRX")
            irx_hist = irx_ticker.history(period="1mo")
            
            if not irx_hist.empty and 'Close' in irx_hist:
                short_yields = irx_hist['Close'].dropna().values
                if len(short_yields) > 0:
                    self.current_short_yield = float(short_yields[-1])
            
            if not tnx_hist.empty and 'Close' in tnx_hist:
                yields = tnx_hist['Close'].dropna().values
                if len(yields) > 0:
                    self.current_yield = float(yields[-1])
                    
                    # Yield Spread (10Y - 3M)
                    if self.current_short_yield > 0:
                        self.yield_spread = self.current_yield - self.current_short_yield
                    
                    yield_14 = yields[-14:] if len(yields) >= 14 else yields
                    yield_30 = yields[-30:] if len(yields) >= 30 else yields
                    
                    if len(yield_14) > 1 and np.std(yield_14) > 0:
                        self.yield_z_score = (self.current_yield - np.mean(yield_14)) / np.std(yield_14)
                    if len(yield_30) > 1 and np.std(yield_30) > 0:
                        self.yield_z_score_30d = (self.current_yield - np.mean(yield_30)) / np.std(yield_30)
            else:
                self._use_fallback_yields()

            # BTC and Nasdaq Correlation
            btc_ticker = yf.Ticker("BTC-USD")
            btc_hist = btc_ticker.history(period="1mo")
            
            # Hurst Exponent via BTC hist
            if not btc_hist.empty and 'Close' in btc_hist:
                closes = btc_hist['Close'].dropna().values
                if len(closes) > 20:
                    try:
                        lags = range(2, 20)
                        tau = [np.std(np.subtract(closes[lag:], closes[:-lag])) for lag in lags]
                        poly = np.polyfit(np.log(lags), np.log(tau), 1)
                        self.hurst_exponent = poly[0] * 2.0
                    except:
                        self.hurst_exponent = 0.5
                else:
                    self.hurst_exponent = 0.5
            else:
                self.hurst_exponent = 0.5

            nq_ticker = yf.Ticker("NQ=F")
            nq_hist = nq_ticker.history(period="1mo")
            
            if not nq_hist.empty and 'Close' in nq_hist:
                nq_closes = nq_hist['Close'].dropna().values
                if len(nq_closes) > 3:
                    self.ndx_momentum = ((nq_closes[-1] - nq_closes[-3]) / nq_closes[-3]) * 100.0
            
            # Correlation Logic
            if not btc_hist.empty and not dxy_hist.empty:
                try:
                    import pandas as pd
                    dxy_series = dxy_hist['Close'].dropna()
                    btc_series = btc_hist['Close'].dropna()
                    
                    if dxy_series.index.tz is not None: dxy_series.index = dxy_series.index.tz_convert(None)
                    if btc_series.index.tz is not None: btc_series.index = btc_series.index.tz_convert(None)
                        
                    common_dates = dxy_series.index.intersection(btc_series.index)
                    if len(common_dates) > 5:
                        dxy_aligned = dxy_series.loc[common_dates]
                        btc_aligned = btc_series.loc[common_dates]
                        corr = np.corrcoef(dxy_aligned.values, btc_aligned.values)[0, 1]
                        self.dxy_correlation = float(corr) if not np.isnan(corr) else -0.65
                except:
                    self.dxy_correlation = -0.65 # Default inverse correlation if calculation fails
            
            if not btc_hist.empty and not nq_hist.empty:
                try:
                    nq_series = nq_hist['Close'].dropna()
                    btc_series = btc_hist['Close'].dropna()
                    if nq_series.index.tz is not None: nq_series.index = nq_series.index.tz_convert(None)
                    if btc_series.index.tz is not None: btc_series.index = btc_series.index.tz_convert(None)
                    
                    common_dates_nq = nq_series.index.intersection(btc_series.index)
                    if len(common_dates_nq) > 5:
                        nq_aligned = nq_series.loc[common_dates_nq]
                        btc_aligned_nq = btc_series.loc[common_dates_nq]
                        corr_nq = np.corrcoef(nq_aligned.values, btc_aligned_nq.values)[0, 1]
                        self.ndx_correlation = float(corr_nq) if not np.isnan(corr_nq) else 0.75
                except:
                    self.ndx_correlation = 0.75 # Default positive correlation

        except Exception as e:
            print(json.dumps({"type": "ERROR", "message": f"Macro update failed: {str(e)}"}), flush=True)
            self._use_fallback_markets()

    def _use_fallback_markets(self):
        self._use_fallback_dxy()
        self._use_fallback_yields()
        self.dxy_correlation = -0.65
        self.ndx_correlation = 0.75
        self.ndx_momentum = 0.5
        self.hurst_exponent = 0.5

    def _use_fallback_dxy(self):
        if self.current_dxy == 0:
            self.current_dxy = 104.20
            self.dxy_z_score = 0.45
            self.dxy_z_score_30d = 0.20

    def _use_fallback_yields(self):
        if self.current_yield == 0:
            self.current_yield = 4.25
            self.current_short_yield = 4.65
            self.yield_spread = -0.40
            self.yield_z_score = 1.2
            self.yield_z_score_30d = 0.8

    def _update_news_sentiment(self):
        if not self.analyzer or not feedparser: return
        try:
            feed = feedparser.parse('https://finance.yahoo.com/news/rssindex')
            if not getattr(feed, 'entries', False): return
            
            now = time.time()
            for entry in feed.entries[:15]:
                headline = getattr(entry, 'title', '')
                link = getattr(entry, 'link', headline)
                
                # Check for uniqueness so we don't skew the 24h average with duplicates
                if headline and link not in self.seen_news:
                    score = self.analyzer.polarity_scores(headline)
                    self.sentiment_history.append((now, score['compound']))
                    self.seen_news.add(link)
                    
            # Maintain a sliding window of seen news (last 1000) to prevent memory growth
            # without wiping the entire deduplication history at once.
            if len(self.seen_news) > 1000:
                # Cast to list to slice and keep the newest 500
                keep_list = list(self.seen_news)[-500:]
                self.seen_news = set(keep_list)
                
            # Purge items strictly older than rolling 24 hours (86400 seconds)
            cutoff = now - 86400
            self.sentiment_history = [item for item in self.sentiment_history if item[0] >= cutoff]
            
            # Continuous mean average of the 24-hour block
            if self.sentiment_history:
                self.sentiment_score = float(np.mean([item[1] for item in self.sentiment_history]))
            else:
                self.sentiment_score = 0.0
                
        except Exception as e:
            pass

    def _update_options_data(self):
        if not ccxt: return
        try:
            # Poll Deribit API for 24h Options Put/Call Ratio and aggregate Implied Volatility
            exchange = ccxt.deribit({'enableRateLimit': True})
            # Deribit provides a 'get_volatility_index_data' or we can fetch a few prominent options to aggregate
            # To keep the HFT thread perfectly safe from unauthenticated rate-limits on hundreds of tickers,
            # we fetch the BTC-DVOL (Deribit Implied Volatility Index) to capture macro IV.
            
            try:
                # Often DVOL is available via public ticker
                ticker = exchange.fetch_ticker('BTC-DVOL')
                iv = ticker['last'] if ticker['last'] else 50.0
            except:
                iv = 50.0 # fallback IV
                
            # PCR extraction typically requires aggregating /public/get_book_summary_by_currency
            # In ccxt, we can use an implicit API call to get currency stats
            currency_stats = exchange.publicGetGetBookSummaryByCurrency({'currency': 'BTC', 'kind': 'option'})
            
            put_vol = 0.0
            call_vol = 0.0
            max_open_interest_strike = 0.0
            max_oi = 0.0

            if 'result' in currency_stats:
                for option in currency_stats['result']:
                    oi = option.get('open_interest', 0)
                    strike_str = option.get('instrument_name', '').split('-')[2] if '-' in option.get('instrument_name', '') else '0'
                    try:
                        strike = float(strike_str)
                        if oi > max_oi:
                            max_oi = oi
                            max_open_interest_strike = strike
                    except:
                        pass

                    if option.get('instrument_name', '').endswith('-P'):
                        put_vol += option.get('volume', 0)
                    elif option.get('instrument_name', '').endswith('-C'):
                        call_vol += option.get('volume', 0)
            
            pcr = (put_vol / call_vol) if call_vol > 0 else 1.0
            
            # Use max OI strike as proxy for Gamma Flip or stick to a default if not found
            gamma_flip = max_open_interest_strike if max_open_interest_strike > 0 else 65000.0

            self.options_data = {"put_call": round(pcr, 4), "iv": round(iv, 2), "gamma_flip": gamma_flip}
            
            # Calculate Gamma Exposure (GEX) Hedging Pressure
            try:
                binance = ccxt.binance({'enableRateLimit': False})
                ticker = binance.fetch_ticker('BTC/USDT')
                current_price = ticker['last']
                
                # Hedging pressure: If price is above gamma flip, dealers buy dips (Bullish/supportive)
                # If price is below gamma flip, dealers sell rips (Bearish/suppressive)
                distance_to_flip = (current_price - gamma_flip) / gamma_flip
                
                # PCR contribution (high PCR > 1 is bearish)
                pcr_sentiment = (1.0 - pcr) * 0.5 
                
                # GEX sentiment contribution
                gex_sentiment = 0.5 if distance_to_flip > 0 else -0.5
                
                # Boost size of GEX impact based on how close we are to flip level (high gamma zone)
                if abs(distance_to_flip) < 0.05: # Within 5% of flip
                    gex_sentiment *= 2.0
                    
                # Adjust final sentiment score by actual market hedging flow
                self.sentiment_score += pcr_sentiment + gex_sentiment
                # Clamp sentiment score between -1 and 1
                self.sentiment_score = max(-1.0, min(1.0, self.sentiment_score))
            except:
                pass
                
        except:
            # Silent fallback to prevent thread crashing on network timeout
            pass
            
    def _update_cot_data(self):
        # Crypto proxy for CFTC Commitment of Traders (COT): Institutional Top Trader Long/Short Ratio
        import requests
        try:
            # Change from 1d to 5m to track real-time smart money whale positioning
            url = "https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol=BTCUSDT&period=5m&limit=1"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 0:
                    self.cot_long_short_ratio = float(data[0].get("longShortRatio", self.cot_long_short_ratio))
        except Exception as e:
            pass

    def _update_funding_rate(self):
        # Fetch Current Binance Perpetual Funding Rate
        import requests
        try:
            url = "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=BTCUSDT"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                # Represent as percentage representation e.g. 0.0001 -> 0.01%
                self.funding_rate = float(data.get("lastFundingRate", 0.0))
        except Exception as e:
            pass

    def _update_calendar(self):
        # URL: https://nfs.faireconomy.media/ff_calendar_thisweek.xml
        try:
            if requests:
                headers = {'User-Agent': 'Mozilla/5.0'}
                response = requests.get('https://nfs.faireconomy.media/ff_calendar_thisweek.xml', headers=headers, timeout=10)
                if response.status_code == 200:
                    import xml.etree.ElementTree as ET
                    root = ET.fromstring(response.content)
                    
                    real_events = []
                    for event in root.findall('event'):
                        impact = event.find('impact')
                        # We only care about "High" impact news events
                        if impact is not None and impact.text and impact.text.strip() == 'High':
                            date_str = event.find('date')
                            time_str = event.find('time')
                            
                            if date_str is not None and time_str is not None:
                                try:
                                    # Format: date '04-16-2024', time '8:30am' (Eastern Time usually, depending on feed, but faireconomy standardizes to Eastern Time)
                                    # Parse into aware datetime then convert to UTC
                                    dt_str = f"{date_str.text.strip()} {time_str.text.strip()}"
                                    from dateutil import parser as dt_parser
                                    from dateutil import tz
                                    # Add US/Eastern implicitly if the feed is ET
                                    event_dt = dt_parser.parse(dt_str)
                                    
                                    eastern = tz.gettz('US/Eastern')
                                    utc = tz.tzutc()
                                    if eastern and utc:
                                        # Forex Factory outputs in Eastern Time
                                        event_dt = event_dt.replace(tzinfo=eastern)
                                        # Convert to UTC and strip timezone info to match datetime.utcnow()
                                        event_dt_utc = event_dt.astimezone(utc).replace(tzinfo=None)
                                        real_events.append(event_dt_utc)
                                    else:
                                        real_events.append(event_dt)
                                except:
                                    pass
                    
                    if real_events:
                        self.upcoming_events = real_events
        except Exception as e:
            # Fallback handling
            pass

    def _check_killswitch(self):
        now = datetime.datetime.utcnow()
        self.killswitch_active = False
        
        # 1. Statistical Yield Shock (> +3.0 Z-Score) or Deeply Inverted/Un-inverting Curve Shock
        if self.yield_z_score > 3.0:
            self.killswitch_active = True
            return

        # Vicious bear steepener or heavy inversion shock limit
        # Sensitivity update: Adjusted from -1.0 to 0.0 to capture any curve inversion (Institutional Standard)
        if self.yield_spread < 0.0 or (self.yield_spread > 0.0 and self.yield_z_score > 2.5):
            self.killswitch_active = True
            return

        # 2. Economic Calendar Window (+/- 10 minutes)
        # Filter out past events to keep the engine slim (micro-optimization)
        cutoff = now - datetime.timedelta(minutes=30)
        self.upcoming_events = [e for e in self.upcoming_events if e >= cutoff]

        for event_time in self.upcoming_events:
            diff = (now - event_time).total_seconds()
            # If we are within 600 seconds (10 mins) before or after
            if abs(diff) <= 600:
                self.killswitch_active = True
                return

    def _evaluate_regime(self):
        if self.killswitch_active:
            self.regime = "SHOCK"
            return
            
        # RISK_OFF: Put/Call > 1.2 AND DXY sloping upward AND Sentiment < 0
        # RISK_ON: Put/Call < 0.8 AND DXY sloping downward AND Sentiment > 0
        # CHOP: Insignificant Z-scores, neutral sentiment
        
        pcr = self.options_data.get("put_call", 1.0)
        
        # A positive Z-Score (> 0.0) means the current price is above the moving average ("sloping upward")
        # A negative Z-Score (< 0.0) means the current price is below the moving average ("sloping downward")
        if pcr > 1.2 and self.dxy_z_score > 0.0 and self.sentiment_score < 0.0:
            self.regime = "RISK_OFF"
        elif pcr < 0.8 and self.dxy_z_score < 0.0 and self.sentiment_score > 0.0:
            self.regime = "RISK_ON"
        else:
            self.regime = "CHOP"

    def _emit_status(self):
        payload = {
            "type": "MACRO_REGIME_UPDATE",
            "state": self.regime,
            "killswitch_active": self.killswitch_active,
            "upcoming_events": [dt.isoformat() + "Z" for dt in self.upcoming_events] if self.upcoming_events else [],
            "metrics": {
                "dxy_price": round(self.current_dxy, 4) if self.current_dxy else 0,
                "dxy_z_score": round(self.dxy_z_score, 2) if getattr(self, 'dxy_z_score', False) else 0.0,
                "yield_price": round(self.current_yield, 4) if self.current_yield else 0,
                "yield_spread": round(getattr(self, 'yield_spread', 0.0), 3),
                "yield_z_score": round(self.yield_z_score, 2) if getattr(self, 'yield_z_score', False) else 0.0,
                "sentiment": round(self.sentiment_score, 2) if getattr(self, 'sentiment_score', False) else 0.0,
                "put_call_ratio": round(self.options_data.get("put_call", 1.0), 2),
                "implied_volatility": round(self.options_data.get("iv", 50.0), 2),
                "gamma_flip_level": round(self.options_data.get("gamma_flip", 65000.0), 2),
                "cot_long_short_ratio": round(self.cot_long_short_ratio, 4),
                "funding_rate": round(self.funding_rate, 6),
                "dxy_correlation": round(self.dxy_correlation, 4),
                "ndx_momentum": round(self.ndx_momentum, 4),
                "ndx_correlation": round(self.ndx_correlation, 4),
                "hurst_exponent": round(self.hurst_exponent, 4)
            }
        }
        if self.callback:
            self.callback(payload)

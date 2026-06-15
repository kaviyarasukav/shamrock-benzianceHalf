// TypeScript Interfaces representing the JSON Schemas
// This ensures strict typing across the Node.js execution engine.

export type OrderType = 'MARKET' | 'LIMIT' | 'LIMIT_MAKER' | 'STOP_LOSS' | 'STOP_LOSS_LIMIT' | 'TAKE_PROFIT' | 'TAKE_PROFIT_LIMIT' | 'OCO' | 'LIMIT_CHASE' | 'TWAP' | 'CANCEL';
export type TimeInForce = 'GTC' | 'IOC' | 'FOK' | 'GTX';

export interface StrategySignal {
  signal_id: string;
  timestamp: number;
  strategy_id: string;
  symbol: string;
  action: 'BUY' | 'SELL' | 'CLOSE_LONG' | 'CLOSE_SHORT' | 'HALT' | 'RESUME' | 'HALT_ALL';
  order_type: OrderType;
  price?: number | string;
  stopPrice?: number | string;
  stopLimitPrice?: number | string;
  timeInForce?: TimeInForce;
  icebergQty?: number | string;
  weight: number; // 0-100
  stop_loss?: number | string;
  take_profit?: number | string;
  position_size_usd?: number | string;
  metadata?: Record<string, any>;
  isShadow?: boolean;
}

export interface ExecutionOrder {
  internal_order_id: string;
  clientOrderId?: string;
  timestamp: number;
  symbol: string;
  side: 'BUY' | 'SELL';
  type: OrderType | 'CANCEL_REPLACE';
  quantity: number | string;
  price?: number | string;
  stopPrice?: number | string;
  stopLimitPrice?: number | string;
  timeInForce?: TimeInForce;
  icebergQty?: number | string;
  reduce_only?: boolean;
  market?: 'SPOT' | 'FUTURES' | 'OPTIONS';
  metadata?: Record<string, any>; // Used to pass action tracking
  algo?: 'TWAP' | 'VWAP' | 'PEG_BBO';
  totalQuantity?: number | string;
  durationMs?: number;
  sliceCount?: number;
  maxRetries?: number;
  urgency?: 'LOW' | 'MEDIUM' | 'HIGH';
  isShadow?: boolean;
  cancelReplaceClientOrderId?: string;
}

export interface PositionState {
  symbol: string;
  active: boolean;
  direction: "LONG" | "SHORT" | "NONE";
  isShadow?: boolean;
}

export interface MarketDataDepth {
  symbol: string;
  timestamp: number;
  bids: { p: number; q: number }[];
  asks: { p: number; q: number }[];
  mid_price: number;
  bid_total_volume: number;
  ask_total_volume: number;
  imbalance: number; // (bid_vol - ask_vol) / (bid_vol + ask_vol)
}

export interface MarketDataTrade {
  symbol: string;
  data: {
    p: string; // price
    q: string; // quantity
    T: number; // timestamp
    m: boolean; // is buyer maker
  };
}

export interface SystemHeartbeat {
  timestamp: number;
  status: 'OK' | 'ERROR' | 'HALTED';
  message?: string;
}

export interface VolumeSpike {
  symbol: string;
  timestamp: number;
  price: number;
  quantity: number;
  side: 'BUY' | 'SELL';
  z_score: number;
  is_unusual: boolean;
}

export interface CumulativeVolumeDelta {
  symbol: string;
  timestamp: number;
  cvd: number; // Cumulative Delta
  session_high: number;
  session_low: number;
  last_delta: number; // Delta of the last trade
}

export interface LargeOrder {
  symbol: string;
  timestamp: number;
  price: number;
  quantity: number;
  side: 'BUY' | 'SELL';
  usd_value: number;
}

export interface LiquidityShift {
  symbol: string;
  timestamp: number;
  side: 'BID' | 'ASK';
  type: 'ADDED' | 'REMOVED';
  price: number;
  quantity: number;
  usd_value: number;
}

export interface OptionsFlow {
  symbol: string;
  timestamp: number;
  side: 'BUY' | 'SELL';
  type: 'CALL' | 'PUT';
  strike: number;
  expiry: string;
  price: number;
  quantity: number;
  usd_value: number;
  is_block_trade: boolean;
}

export interface IcebergDetected {
  symbol: string;
  timestamp: number;
  price: number;
  total_traded: number;
  displayed_qty: number;
  side: 'HIDDEN_BUYER' | 'HIDDEN_SELLER';
}

export interface SpoofingDetected {
  symbol: string;
  timestamp: number;
  price: number;
  side: 'BID' | 'ASK';
  action: string;
  severity: 'LOW' | 'MEDIUM' | 'HIGH';
}

export interface OptionsSweepDetected {
  symbol: string;
  option_type: string;
  strike: number;
  expiry: string;
  usd_value: number;
  message: string;
}

export interface GammaExposureAlert {
  symbol: string;
  option_type: string;
  moneyness: string;
  estimated_hedge: string;
  usd_value: number;
  message: string;
}

export interface ConfluenceSignal {
  symbol: string;
  timestamp: string;
  strategy_id?: string;
  ticker: string;
  direction: 'LONG' | 'SHORT';
  trigger_price: number;
  leverage_multiplier?: number;
  conditions_met: {
    cvd_slope?: number;
    z_score?: number;
    iceberg_distance?: number;
    call_sweep_value?: number;
    [key: string]: any;
  };
}

import type { IndicatorConfigBase, SMCConfig, RSIConfig } from './IndicatorConfig';
export type { IndicatorConfigBase, SMCConfig, RSIConfig };

export interface OrderBlock {
  id: string;
  type: 'OB';
  symbol: string;
  tf: string;
  direction: 'BULLISH' | 'BEARISH';
  start_time: number;
  end_time: number | null;
  top_price: number;
  bottom_price: number;
  status: 'active' | 'mitigated';
}


export interface MacroRegimeUpdate {
  symbol?: string; // Optional, usually global
  timestamp: string | number;
  state: string;
  killswitch_active: boolean;
  upcoming_events?: string[];
  metrics: {
    dxy_price: number;
    dxy_z_score: number;
    yield_price: number;
    yield_spread: number;
    yield_z_score: number;
    sentiment: number;
    put_call_ratio: number;
    implied_volatility: number;
    cot_long_short_ratio: number;
    funding_rate?: number;
    dxy_correlation?: number;
  };
}

// Map topics to their strict payload types
export interface BrokerMessageMap {
  MARKET_DATA_DEPTH: MarketDataDepth;
  MARKET_DATA_TRADE: MarketDataTrade;
  VOLUME_SPIKE: VolumeSpike;
  CUMULATIVE_VOLUME_DELTA: CumulativeVolumeDelta;
  LARGE_ORDER_DETECTED: LargeOrder;
  LIQUIDITY_SHIFT: LiquidityShift;
  ICEBERG_DETECTED: IcebergDetected;
  SPOOFING_DETECTED: SpoofingDetected;
  OPTIONS_SWEEP_DETECTED: OptionsSweepDetected;
  GAMMA_EXPOSURE_ALERT: GammaExposureAlert;
  POSITION_STATE: PositionState;
  USER_ORDER_UPDATE: any; // Can be typed strictly later based on Binance executionReport
  USER_BALANCE_UPDATE: Record<string, { free: string; locked: string }>;
  SYSTEM_HEARTBEAT: SystemHeartbeat;
  STRATEGY_SIGNAL: StrategySignal;
  CONFLUENCE_SIGNAL: ConfluenceSignal;
  EXECUTE_ORDER: ExecutionOrder;
  MARKET_DATA_REQUEST: { symbol: string };
  OPTIONS_FLOW: OptionsFlow;
  OPTIONS_SNAPSHOT: {
    symbol: string;
    timestamp: number;
    put_call_ratio: number;
    implied_volatility: number;
  };
  MACRO_REGIME_UPDATE: MacroRegimeUpdate;
  UPDATE_RISK: { symbol: string; action: string; new_sl?: number | string };
  RISK_STATE_UPDATE: any;
  ACTIVE_SMC_CACHE: Record<string, {
    order_blocks: OrderBlock[];
    fvgs?: any[];
  }>;
  SMC_UPDATE: {
    symbol: string;
    tf: string;
    order_blocks: OrderBlock[];
    fvgs?: any[];
  };
  CONFIG_UPDATE: {
    indicators: IndicatorConfigBase[];
  };
  INDICATORS_UPDATE: {
    symbol: string;
    tf: string;
    ts?: number;
    indicators: {
      rsi: number;
      supertrend: number;
      supertrend_direction: number;
      price: number;
    };
  };
  CANDLE_CLOSED: {
    symbol: string;
    data: {
      tf: string;
      o: number;
      h: number;
      l: number;
      c: number;
      v: number;
      ts: number;
      indicators?: {
        rsi: number;
        supertrend: number;
        supertrend_direction: number;
        price: number;
      };
    };
  };
  ALPHA_SIGNAL: any;
  EXECUTION_REPORT: any;
  EXECUTION_ERROR: any;
  SYSTEM_INFO_MESSAGE: any;
}

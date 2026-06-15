import Joi from 'joi';
import { StrategySignal, ExecutionOrder, ConfluenceSignal, IndicatorConfigBase, OrderBlock } from './types';

export const IndicatorConfigBaseSchema = Joi.object<IndicatorConfigBase>({
  id: Joi.string().required(),
  type: Joi.string().required(),
  enabled: Joi.boolean().required(),
}).unknown(true);

export const OrderBlockSchema = Joi.object<OrderBlock>({
  id: Joi.string().required(),
  type: Joi.string().valid('OB').required(),
  symbol: Joi.string().required(),
  tf: Joi.string().required(),
  direction: Joi.string().valid('BULLISH', 'BEARISH').required(),
  start_time: Joi.number().required(),
  end_time: Joi.number().allow(null).required(),
  top_price: Joi.number().required(),
  bottom_price: Joi.number().required(),
  status: Joi.string().valid('active', 'mitigated').required()
});

/**
 * Shared validation schema for DOM Signals (Confluence Signals).
 * Strictly enforces the structure of signals coming from the Python Quant Engine.
 */
export const DOMSignalSchema = Joi.object<ConfluenceSignal>({
  symbol: Joi.string().required(),
  timestamp: Joi.string().isoDate().required(),
  strategy_id: Joi.string().optional(),
  ticker: Joi.string().required(),
  direction: Joi.string().valid('LONG', 'SHORT', 'CLOSE_LONG', 'CLOSE_SHORT', 'EXIT').required(),
  trigger_price: Joi.number().positive().required(),
  conditions_met: Joi.object({
    cvd_slope: Joi.number().optional(),
    z_score: Joi.number().optional(),
    iceberg_distance: Joi.number().optional(),
    call_sweep_value: Joi.number().optional(),
  }).unknown(true).required()
});

/**
 * Schema for Strategy Signals generated internally or received from strategies.
 */
export const StrategySignalSchema = Joi.object<StrategySignal>({
  signal_id: Joi.string().required(),
  timestamp: Joi.number().required(),
  strategy_id: Joi.string().required(),
  symbol: Joi.string().required(),
  action: Joi.string().valid('BUY', 'SELL', 'CLOSE_LONG', 'CLOSE_SHORT', 'HALT').required(),
  order_type: Joi.string().valid('MARKET', 'LIMIT', 'LIMIT_MAKER', 'STOP_LOSS', 'STOP_LOSS_LIMIT', 'TAKE_PROFIT', 'TAKE_PROFIT_LIMIT', 'OCO').required(),
  price: Joi.number().positive().optional(),
  stopPrice: Joi.number().positive().optional(),
  stopLimitPrice: Joi.number().positive().optional(),
  timeInForce: Joi.string().valid('GTC', 'IOC', 'FOK', 'GTX').optional(),
  icebergQty: Joi.number().positive().optional(),
  weight: Joi.number().min(0).max(100).required(),
  metadata: Joi.object().optional(),
  isShadow: Joi.boolean().optional()
});

/**
 * Schema for Execution Orders sent to the exchange.
 */
export const ExecutionOrderSchema = Joi.object<ExecutionOrder>({
  internal_order_id: Joi.string().required(),
  clientOrderId: Joi.string().optional(),
  timestamp: Joi.number().required(),
  symbol: Joi.string().required(),
  side: Joi.string().valid('BUY', 'SELL').required(),
  type: Joi.string().valid('MARKET', 'LIMIT', 'LIMIT_MAKER', 'STOP_LOSS', 'STOP_LOSS_LIMIT', 'TAKE_PROFIT', 'TAKE_PROFIT_LIMIT', 'OCO', 'CANCEL').required(),
  quantity: Joi.alternatives().try(Joi.number().positive(), Joi.string()).required(),
  price: Joi.alternatives().try(Joi.number().positive(), Joi.string()).optional(),
  stopPrice: Joi.alternatives().try(Joi.number().positive(), Joi.string()).optional(),
  stopLimitPrice: Joi.alternatives().try(Joi.number().positive(), Joi.string()).optional(),
  timeInForce: Joi.string().valid('GTC', 'IOC', 'FOK', 'GTX').optional(),
  icebergQty: Joi.alternatives().try(Joi.number().positive(), Joi.string()).optional(),
  reduce_only: Joi.boolean().optional(),
  market: Joi.string().valid('SPOT', 'FUTURES', 'OPTIONS').optional(),
  metadata: Joi.object().optional(),
  algo: Joi.string().valid('TWAP', 'VWAP').optional(),
  totalQuantity: Joi.alternatives().try(Joi.number(), Joi.string()).optional(),
  durationMs: Joi.number().optional(),
  sliceCount: Joi.number().optional(),
  urgency: Joi.string().valid('LOW', 'MEDIUM', 'HIGH').optional(),
  isShadow: Joi.boolean().optional()
});

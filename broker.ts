import { EventEmitter } from 'events';
import { BrokerMessageMap } from './shared-contracts/types';

// Define the strict interface for our Message Broker
// This allows us to swap EventEmitter with Redis or ZeroMQ in the future
export interface IMessageBroker {
  publish<K extends keyof BrokerMessageMap>(topic: K, message: BrokerMessageMap[K]): void;
  subscribe<K extends keyof BrokerMessageMap>(topic: K, handler: (message: BrokerMessageMap[K]) => void): void;
  unsubscribe<K extends keyof BrokerMessageMap>(topic: K, handler: (message: BrokerMessageMap[K]) => void): void;
}

// In-Memory implementation for Phase 0.1
class EventEmitterBroker implements IMessageBroker {
  private emitter = new EventEmitter();

  constructor() {
    // Increase max listeners to prevent memory leak warnings if many strategies subscribe
    this.emitter.setMaxListeners(100);
  }

  publish<K extends keyof BrokerMessageMap>(topic: K, message: BrokerMessageMap[K]): void {
    // Execute asynchronously to prevent blocking the WebSocket thread
    // This fulfills the "asynchronous architecture" requirement
    setImmediate(() => {
      try {
        this.emitter.emit(topic, message);
      } catch (error) {
        console.error(`[Broker] Critical error emitting topic ${topic}:`, error);
      }
    });
  }

  subscribe<K extends keyof BrokerMessageMap>(topic: K, handler: (message: BrokerMessageMap[K]) => void): void {
    this.emitter.on(topic, handler);
  }

  unsubscribe<K extends keyof BrokerMessageMap>(topic: K, handler: (message: BrokerMessageMap[K]) => void): void {
    this.emitter.off(topic, handler);
  }
}

// Export a singleton instance of the broker
export const broker = new EventEmitterBroker();

// Define standard topics (Interface Contracts)
export const TOPICS: { [K in keyof BrokerMessageMap]: K } = {
  MARKET_DATA_DEPTH: 'MARKET_DATA_DEPTH',
  MARKET_DATA_TRADE: 'MARKET_DATA_TRADE',
  VOLUME_SPIKE: 'VOLUME_SPIKE',
  CUMULATIVE_VOLUME_DELTA: 'CUMULATIVE_VOLUME_DELTA',
  LARGE_ORDER_DETECTED: 'LARGE_ORDER_DETECTED',
  LIQUIDITY_SHIFT: 'LIQUIDITY_SHIFT',
  ICEBERG_DETECTED: 'ICEBERG_DETECTED',
  SPOOFING_DETECTED: 'SPOOFING_DETECTED',
  OPTIONS_SWEEP_DETECTED: 'OPTIONS_SWEEP_DETECTED',
  GAMMA_EXPOSURE_ALERT: 'GAMMA_EXPOSURE_ALERT',
  POSITION_STATE: 'POSITION_STATE',
  USER_ORDER_UPDATE: 'USER_ORDER_UPDATE',
  USER_BALANCE_UPDATE: 'USER_BALANCE_UPDATE',
  SYSTEM_HEARTBEAT: 'SYSTEM_HEARTBEAT',
  STRATEGY_SIGNAL: 'STRATEGY_SIGNAL',
  CONFLUENCE_SIGNAL: 'CONFLUENCE_SIGNAL',
  EXECUTE_ORDER: 'EXECUTE_ORDER',
  MARKET_DATA_REQUEST: 'MARKET_DATA_REQUEST',
  OPTIONS_FLOW: 'OPTIONS_FLOW',
  OPTIONS_SNAPSHOT: 'OPTIONS_SNAPSHOT',
  MACRO_REGIME_UPDATE: 'MACRO_REGIME_UPDATE',
  CANDLE_CLOSED: 'CANDLE_CLOSED',
  INDICATORS_UPDATE: 'INDICATORS_UPDATE',
  UPDATE_RISK: 'UPDATE_RISK',
  RISK_STATE_UPDATE: 'RISK_STATE_UPDATE',
  ACTIVE_SMC_CACHE: 'ACTIVE_SMC_CACHE',
  SMC_UPDATE: 'SMC_UPDATE',
  CONFIG_UPDATE: 'CONFIG_UPDATE',
  ALPHA_SIGNAL: 'ALPHA_SIGNAL',
  EXECUTION_REPORT: 'EXECUTION_REPORT',
  EXECUTION_ERROR: 'EXECUTION_ERROR',
  SYSTEM_INFO_MESSAGE: 'SYSTEM_INFO_MESSAGE'
};

import { broker, TOPICS } from './broker';
import { DOMSignalSchema } from './shared-contracts/schemas';
import { ConfluenceSignal } from './shared-contracts/types';

/**
 * SignalSubscriber Service
 * Listens for strategy signals from the broker, validates them,
 * and routes them to the Execution Engine and UI.
 */
class SignalSubscriber {
  constructor() {
    this.init();
  }

  private init() {
    console.log('[SignalSubscriber] Initializing persistent listener on TOPICS.CONFLUENCE_SIGNAL');
    
    broker.subscribe(TOPICS.CONFLUENCE_SIGNAL, (payload: any) => {
      this.handleSignal(payload);
    });
  }

  private async handleSignal(payload: any) {
    // 1. Validate the payload against the shared contract
    const { error, value } = DOMSignalSchema.validate(payload, { allowUnknown: true });

    if (error) {
      console.error(`[SignalSubscriber] Rejected malformed signal: ${error.message}`, payload);
      // Feedback loop: Notify Python that the signal was rejected due to schema validation failure
      try {
        const { sendToQuantEngine } = await import('./server');
        const symbol = payload?.ticker || payload?.symbol || 'UNKNOWN';
        sendToQuantEngine('SIGNAL_REJECTED', { symbol, reason: `SCHEMA_VALIDATION_ERROR: ${error.message}` });
      } catch (e) {
        console.error('[SignalSubscriber] Failed to send rejection feedback to Quant Engine:', e);
      }
      return;
    }

    const signal = value as ConfluenceSignal;
    console.log(`[SignalSubscriber] Validated signal received for ${signal.ticker}: ${signal.direction}`);

    // 2. Standardize and Broadcast
    // We publish to STRATEGY_SIGNAL. The OrderRouter picks this up to calculate 
    // risk-adjusted size and apply exchange-specific precision before executing.
      const actionMap: Record<string, "BUY" | "SELL" | "CLOSE_LONG" | "CLOSE_SHORT"> = {
        'LONG': 'BUY',
        'SHORT': 'SELL',
        'CLOSE_LONG': 'CLOSE_LONG',
        'CLOSE_SHORT': 'CLOSE_SHORT',
        'EXIT': 'SELL', // Default fallback for exits
      };
      
      const mappedAction: "BUY" | "SELL" | "CLOSE_LONG" | "CLOSE_SHORT" = actionMap[signal.direction] || 'SELL';

      broker.publish(TOPICS.STRATEGY_SIGNAL, {
        ...signal,
        signal_id: `sig_${Date.now()}_${Math.floor(Math.random() * 1000000)}`,
        timestamp: Date.now(),
        strategy_id: signal.strategy_id || 'HYBRID',
        symbol: signal.ticker,
        action: mappedAction,
        order_type: (signal as any).order_type || 'LIMIT',
      price: signal.trigger_price,
      weight: (signal as any).weight !== undefined ? (signal as any).weight : 100,
      metadata: {
        ...signal.conditions_met,
        position_size_usd: (signal as any).position_size_usd,
        requested_quantity: (signal as any).position_size_usd ? ((signal as any).position_size_usd / signal.trigger_price).toString() : undefined,
        takeProfit: (signal as any).take_profit,
        stopLoss: (signal as any).stop_loss
      }
    });
  }
}

// Export a singleton instance
export const signalSubscriber = new SignalSubscriber();

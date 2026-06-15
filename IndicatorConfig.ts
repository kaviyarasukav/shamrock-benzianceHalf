export interface IndicatorConfigBase {
  id: string;
  type: string;
  enabled: boolean;
  [key: string]: any;
}

export interface SMCConfig extends IndicatorConfigBase {
  type: 'SMC';
  show_ob: boolean;
}

export interface RSIConfig extends IndicatorConfigBase {
  type: 'RSI';
  length: number;
  source: 'close' | 'open' | 'high' | 'low';
}

export interface EMAConfig extends IndicatorConfigBase {
  type: 'EMA';
  length: number;
}

export interface MACDConfig extends IndicatorConfigBase {
  type: 'MACD';
  fast_length: number;
  slow_length: number;
  signal_length: number;
  source: 'close' | 'open' | 'high' | 'low';
}

export interface BollingerBandsConfig extends IndicatorConfigBase {
  type: 'BB';
  length: number;
  stdDev: number;
  source: 'close' | 'open' | 'high' | 'low';
}

export interface VWAPConfig extends IndicatorConfigBase {
  type: 'VWAP';
}

export interface VolumeProfileConfig extends IndicatorConfigBase {
  type: 'VolumeProfile';
  bins: number;
}

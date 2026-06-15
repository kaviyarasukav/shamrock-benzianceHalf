import "dotenv/config";
import express from "express";

process.on("uncaughtException", (err) => {
  console.error("UNCAUGHT EXCEPTION:", err);
});
process.on("unhandledRejection", (reason, promise) => {
  console.error("UNHANDLED REJECTION:", reason);
});

import { createServer as createViteServer } from "vite";
import axios from "axios";
import path from "path";
import { fileURLToPath } from "url";
import { WebSocket, WebSocketServer } from "ws";
import crypto from "node:crypto";
import { spawn, execSync, exec, ChildProcess } from "child_process";
import * as zmq from "zeromq";
import { getCachedData, setCachedData, clearCache } from "./server/cache";

import { broker, TOPICS } from "./broker";
import { signalSubscriber } from "./signal_subscriber";
import { initializeDbLogger } from "./src/db/db_logger";
import { db, getSetting, setSetting } from "./src/db/sqlite_journal";
import { initializeOrderRouter } from "./src/execution/order_router";
import {
  withCircuitBreaker,
  globalCircuitBreaker,
} from "./src/circuit_breaker";
import { algoExecutionManager } from "./src/execution_algorithms";
import { formatQuantity } from "./src/lib/precision";

const safeDirname = process.cwd();

const CACHE_TTL = {
  ticker: 10000, // 10 seconds
  exchangeInfo: 3600000, // 1 hour
  klines: 10000, // 10 seconds
  depth: 5000, // 5 seconds
  trades: 5000, // 5 seconds
};

const USE_TESTNET_ENV = process.env.BINANCE_USE_TESTNET !== "false"; // Default to testnet for safety

// Dynamic Keys fetching
let activeApiKey: string | null = null;
let activeSecretKey: string | null = null;
let useTestnet: boolean = USE_TESTNET_ENV;
let binanceBaseUrl: string = useTestnet
  ? "https://testnet.binance.vision"
  : "https://api.binance.com";

export function loadKeysFromDB() {
  const dbApiKey = getSetting("BINANCE_API_KEY", true);
  const dbSecretKey = getSetting("BINANCE_SECRET_KEY", true);
  const dbTestnet = getSetting("BINANCE_USE_TESTNET", false);

  // Use DB key if it's a non-empty string, otherwise fallback to ENV then null
  activeApiKey =
    dbApiKey && dbApiKey.length > 0
      ? dbApiKey
      : process.env.BINANCE_API_KEY || null;
  activeSecretKey =
    dbSecretKey && dbSecretKey.length > 0
      ? dbSecretKey
      : process.env.BINANCE_SECRET_KEY || null;

  if (dbTestnet !== null) {
    useTestnet = dbTestnet === "true";
  } else {
    useTestnet = USE_TESTNET_ENV;
  }

  binanceBaseUrl = useTestnet
    ? "https://testnet.binance.vision"
    : "https://api.binance.com";

  console.log(
    `[Credentials] Keys loaded. Mode: ${useTestnet ? "TESTNET" : "PRODUCTION"}`,
  );
}

// Always use production WS for market data (it's public, read-only, and more reliable than testnet WS)
const BINANCE_WS_URL = "wss://stream.binance.com:9443/ws";

// Circuit Breaker & Weight Tracking
let currentWeight = 0;
const WEIGHT_LIMIT = 6000; // Binance standard limit per minute
const CIRCUIT_BREAKER_THRESHOLD = 4800; // 80% of limit
let circuitBreakerTripped = false;

// Create a dedicated axios instance for Binance with timeout
const binanceAxios = axios.create({
  timeout: 30000, // 30 seconds
});

// Reset weight every minute
setInterval(() => {
  currentWeight = 0;
  if (circuitBreakerTripped) {
    console.log("🟢 Circuit Breaker reset. Resuming API calls.");
    circuitBreakerTripped = false;
  }
}, 60000);

// Add request interceptor to proactively block calls when tripped
binanceAxios.interceptors.request.use(
  (config) => {
    if (circuitBreakerTripped) {
      const error = new Error(
        "🔴 API request blocked by local Circuit Breaker (Rate Limit safety).",
      );
      return Promise.reject(error);
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  },
);

// Axios interceptor to track weight
binanceAxios.interceptors.response.use(
  (response) => {
    const weightUsed =
      response.headers["x-mbx-used-weight"] ||
      response.headers["x-mbx-used-weight-1m"];
    if (weightUsed) {
      currentWeight = parseInt(weightUsed, 10);
      if (
        currentWeight >= CIRCUIT_BREAKER_THRESHOLD &&
        !circuitBreakerTripped
      ) {
        console.warn(
          `🔴 CIRCUIT BREAKER TRIPPED! Weight usage at ${currentWeight}/${WEIGHT_LIMIT}. Pausing REST API calls.`,
        );
        circuitBreakerTripped = true;
      }
    }
    return response;
  },
  (error) => {
    if (error.response && error.response.headers) {
      const weightUsed =
        error.response.headers["x-mbx-used-weight"] ||
        error.response.headers["x-mbx-used-weight-1m"];
      if (weightUsed) currentWeight = parseInt(weightUsed, 10);
    }
    return Promise.reject(error);
  },
);

// Validate environment variables using the new dynamic keys helper
if (!activeApiKey || !activeSecretKey) {
  console.warn(
    "⚠️ WARNING: BINANCE_API_KEY or BINANCE_SECRET_KEY is missing from DB and ENV.",
  );
  console.warn(
    "⚠️ The trading bot will wait for user to configure them via the UI.",
  );
}

const activeDepthWs = new Map<
  string,
  { ws: WebSocket; lastAccessed: number }
>();
const activeTradeWs = new Map<
  string,
  { ws: WebSocket; lastAccessed: number }
>();
let optionsWs: WebSocket | null = null;
let optionsAttempt = 0;
let isAutopilotOn = false;
let autopilotDiscoveryInterval: NodeJS.Timeout | null = null;
let autopilotConvictionThreshold = 85;

// --- CACHES & STATE ---
const activeSMCCache: Record<string, { order_blocks: any[]; fvgs?: any[] }> =
  {};

// --- PYTHON IPC TCP SERVER ---
const pubSocket = new zmq.Publisher();
const pullSocket = new zmq.Pull();
let isZmqBound = false;
let ipcPort = 0; // Not used but kept to avoid ts errors below

async function startIPCServer(): Promise<void> {
  if (isZmqBound) return;
  await pubSocket.bind("tcp://127.0.0.1:5555");
  await pullSocket.bind("tcp://127.0.0.1:5556");
  isZmqBound = true;
  console.log("[IPC] ZeroMQ Publisher bound to port 5555");
  console.log("[IPC] ZeroMQ Pull bound to port 5556");

  // Start listening without blocking
  listenForPythonMessages();
}

async function listenForPythonMessages() {
  for await (const [msg] of pullSocket) {
    try {
      const parsed = JSON.parse(msg.toString());
      handleQuantEngineMsg(parsed);
    } catch (err) {
      console.error(
        "[Quant Engine IPC Format Error]",
        err,
        "Msg:",
        msg.toString().substring(0, 200),
      );
    }
  }
}

// --- PYTHON QUANT ENGINE BRIDGE ---
let quantEngine: ChildProcess | null = null;
let isQuantEngineIntentionalClose = false;

async function spawnEngine() {
  if (!ipcPort) await startIPCServer();
  const enginePath = path.join(
    safeDirname,
    "quant_engine/main.py",
  );

  if (quantEngine) {
    quantEngine.removeAllListeners("close");
    try {
      quantEngine.kill("SIGKILL");
    } catch (e) {}
  }

  quantEngine = spawn(
    "python3",
    [enginePath, "--pub-port", "5555", "--pull-port", "5556"],
    { stdio: ["ignore", "pipe", "pipe"] },
  );

  quantEngine.stderr?.on("data", (d: any) =>
    console.log(`[Quant ERROR]: ${d.toString()}`),
  );
  let stdoutBuffer = "";
  quantEngine.stdout?.on("data", (data: any) => {
    stdoutBuffer += data.toString();
    if (stdoutBuffer.includes("\n")) {
      const lines = stdoutBuffer.split("\n");
      // The last element is either empty string (if string ended with \n)
      // or an incomplete line. Keep it in buffer.
      stdoutBuffer = lines.pop() || "";

      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const parsed = JSON.parse(line);
          if (parsed.type) {
            handleQuantEngineMsg(parsed);
          } else {
            console.log(`[Quant out (JSON)]:`, parsed);
          }
        } catch (e) {
          console.log(`[Quant stdout]: ${line}`);
        }
      }
    }
  });

  quantEngine.on("close", (code: number | null) => {
    console.log(`[Quant Engine] Exited with code ${code}`);
    if (!isQuantEngineIntentionalClose) {
      setTimeout(spawnEngine, 2000);
    }
  });
}

function startQuantEngine() {
  console.log("[Quant Engine] Checking Python environment in background...");
  // Check for critical dependencies
  exec(
    "python3 -c \"import numpy; import pandas; import requests; import yfinance; import zmq; print('READY')\"",
    (err) => {
      if (err) {
        console.log(
          "[Quant Engine] Dependencies missing. Attempting background installation...",
        );

        // Try multiple ways to install: pip3, pip, and with/without --break-system-packages
        const commands = [
          "curl -sS https://bootstrap.pypa.io/get-pip.py -o get-pip.py && python3 get-pip.py --break-system-packages && python3 -m pip install numpy pandas requests yfinance vaderSentiment feedparser python-dateutil ccxt websockets pyzmq --break-system-packages --prefer-binary",
          "python3 -m pip install numpy pandas requests yfinance vaderSentiment feedparser python-dateutil ccxt websockets pyzmq --break-system-packages --prefer-binary",
          "pip3 install numpy pandas requests yfinance vaderSentiment feedparser python-dateutil ccxt websockets pyzmq --break-system-packages --prefer-binary",
          "python3 -m pip install numpy pandas requests yfinance vaderSentiment feedparser python-dateutil ccxt websockets pyzmq --user --prefer-binary",
          "pip3 install numpy pandas requests yfinance vaderSentiment feedparser python-dateutil ccxt websockets pyzmq --user --prefer-binary",
        ];

        const tryInstall = (index: number) => {
          if (index >= commands.length) {
            console.error(
              "[Quant Engine] All dependency installation attempts failed. The engine might fail to start.",
            );
            spawnEngine();
            return;
          }

          console.log(
            `[Quant Engine] Trying installation command: ${commands[index]}`,
          );
          exec(commands[index], (pipErr, stdout, stderr) => {
            if (pipErr) {
              console.warn(
                `[Quant Engine] Installation attempt ${index + 1} failed:`,
                pipErr.message,
              );
              if (stderr) console.warn("[Quant Engine] Stderr:", stderr);
              tryInstall(index + 1);
            } else {
              console.log(
                "[Quant Engine] Dependencies installed successfully on attempt " +
                  (index + 1),
              );
              spawnEngine();
            }
          });
        };

        tryInstall(0);
      } else {
        console.log("[Quant Engine] Python environment verified.");
        spawnEngine();
      }
    },
  );
}

function handleQuantEngineMsg(msg: any) {
  if (msg.status === "READY") {
    console.log(`[Quant Engine] ${msg.message}`);

    // Send initial configuration config (Step 1)
    const modeStr = getSetting("STRATEGY_MODE") || '["SCALP"]';
    let strategyMode;
    try {
      strategyMode = JSON.parse(modeStr);
    } catch (e) {
      strategyMode = modeStr;
    }

    let indicators = [];
    try {
      const configStr = getSetting("INDICATOR_CONFIG");
      if (configStr) {
        indicators = JSON.parse(configStr);
      } else {
        indicators = [
          {
            id: "supertrend1",
            type: "SUPERTREND",
            length: 10,
            multiplier: 3.0,
            enabled: true,
          },
          {
            id: "rsi1",
            type: "RSI",
            length: 14,
            source: "close",
            enabled: true,
          },
          { id: "ema_7", type: "EMA", length: 7, enabled: true },
          { id: "ema_9", type: "EMA", length: 9, enabled: true },
          { id: "ema_21", type: "EMA", length: 21, enabled: true },
          { id: "ema_25", type: "EMA", length: 25, enabled: true },
          { id: "cci20", type: "CCI", length: 20, enabled: true },
          { id: "lsma25", type: "LSMA", length: 25, enabled: true },
          { id: "vwap1", type: "VWAP", enabled: true },
          { id: "smc1", type: "SMC", show_ob: true, enabled: true },
        ];
        setSetting("INDICATOR_CONFIG", JSON.stringify(indicators));
      }
    } catch (e) {
      console.error("[Quant Engine Setup] Error loading indicators:", e);
    }

    sendToQuantEngine("CONFIG_UPDATE", { mode: strategyMode, indicators });

    try {
      const overrides = JSON.parse(getSetting("RISK_OVERRIDES") || "{}");
      sendToQuantEngine("RISK_OVERRIDES_UPDATE", overrides);
    } catch (e) {}

    if (activeApiKey && activeSecretKey) {
      sendToQuantEngine("EXCHANGE_KEYS", {
        apiKey: activeApiKey,
        secretKey: activeSecretKey,
        useTestnet: useTestnet,
      });
    }
  } else if (msg.type === "ANALYTICS_UPDATE") {
    // Publish Python-calculated analytics to the broker
    const cachedDepth = getCachedData(`depth_${msg.symbol}`, 2000);
    if (cachedDepth && cachedDepth.bids && cachedDepth.asks) {
      broker.publish(TOPICS.MARKET_DATA_DEPTH, {
        symbol: msg.symbol,
        ...cachedDepth,
        weighted_obi: msg.weighted_obi,
        walls: msg.walls,
      } as any);
    }

    // Publish spoofing events
    if (msg.spoof_events && msg.spoof_events.length > 0) {
      msg.spoof_events.forEach((event: any) => {
        broker.publish(TOPICS.SPOOFING_DETECTED, {
          symbol: msg.symbol,
          timestamp: Date.now(),
          price: event.price,
          side: event.side,
          action: event.action,
          severity: event.severity || "MEDIUM",
        });
      });
    }
  } else if (msg.type === "HEDGE_ACTION" || msg.type === "ARBITRAGE_SIGNAL") {
    const prefix = msg.type === "HEDGE_ACTION" ? "HEDGE" : "ARB";
    console.log(
      `[Alpha] ${prefix} Action generated for ${msg.symbol || msg.path}: ${msg.action}`,
    );
    broker.publish("ALPHA_SIGNAL", msg);

    if (
      msg.action === "ENTER" ||
      msg.action === "EXIT" ||
      msg.action === "EXECUTE"
    ) {
      if (msg.legs && Array.isArray(msg.legs)) {
        getInternalExchangeInfo()
          .then((exchangeInfo) => {
            msg.legs.forEach((leg: any, index: number) => {
              let rawQty = leg.qty || 0.001; // Safely default if missing
              const cleanSymbol = leg.symbol
                ? leg.symbol.replace("/", "")
                : msg.symbol
                  ? msg.symbol.replace("/", "")
                  : "BTCUSDT";
              const symbolInfo = exchangeInfo?.symbols?.find(
                (s: any) => s.symbol === cleanSymbol,
              );
              const lotSizeFilter = symbolInfo?.filters?.find(
                (f: any) => f.filterType === "LOT_SIZE",
              );
              const stepSize = lotSizeFilter?.stepSize || "0.00001";

              const finalQtyStr = formatQuantity(rawQty.toString(), stepSize);

              const order: any = {
                internal_order_id: `${prefix.toLowerCase()}_${Date.now()}_leg${index}`,
                timestamp: Date.now(),
                symbol: cleanSymbol,
                side: leg.side,
                type: "MARKET",
                quantity: finalQtyStr,
                market: leg.market || "SPOT",
                reduce_only: leg.reduce_only,
                metadata: {
                  action: `${prefix}_${msg.action}`,
                  strategy_id: prefix,
                },
              };
              console.log(
                `[Execution Engine] Auto-dispatching leg for ${prefix}:`,
                order,
              );
              broker.publish(TOPICS.EXECUTE_ORDER, order);
            });
          })
          .catch((err) => {
            console.error(
              `[Execution Engine] Failed to fetch exchange info for ${prefix} leg formatting`,
              err,
            );
          });
      }
    }
  } else if (msg.type === "ICEBERG_DETECTED") {
    broker.publish(TOPICS.ICEBERG_DETECTED, {
      symbol: msg.symbol || "UNKNOWN",
      timestamp: msg.timestamp,
      price: msg.price,
      total_traded: msg.total_traded,
      displayed_qty: msg.displayed_qty,
      side: msg.side,
    });
  } else if (msg.type === "OPTIONS_SWEEP_DETECTED") {
    broker.publish(TOPICS.OPTIONS_SWEEP_DETECTED, {
      symbol: msg.symbol,
      option_type: msg.option_type,
      strike: msg.strike,
      expiry: msg.expiry,
      usd_value: msg.usd_value,
      message: msg.message,
    });
  } else if (msg.type === "GAMMA_EXPOSURE_ALERT") {
    broker.publish(TOPICS.GAMMA_EXPOSURE_ALERT, {
      symbol: msg.symbol,
      option_type: msg.option_type,
      moneyness: msg.moneyness,
      estimated_hedge: msg.estimated_hedge,
      usd_value: msg.usd_value,
      message: msg.message,
    });
  } else if (msg.type === "TRADE_ANALYTICS") {
    // Publish aggregated trade and CVD
    broker.publish(TOPICS.CUMULATIVE_VOLUME_DELTA, {
      symbol: msg.symbol,
      timestamp: msg.timestamp,
      cvd: msg.cvd,
      session_high: msg.aggregated_trade.cvd_high,
      session_low: msg.aggregated_trade.cvd_low,
      last_delta:
        msg.aggregated_trade.qty *
        (msg.aggregated_trade.side === "BUY" ? 1 : -1),
    });

    // If Z-Score is high, publish a volume spike
    if (msg.z_score > 3.5) {
      broker.publish(TOPICS.VOLUME_SPIKE, {
        symbol: msg.symbol,
        timestamp: msg.timestamp,
        price: msg.aggregated_trade.price,
        quantity: msg.aggregated_trade.qty,
        side: msg.aggregated_trade.side,
        z_score: msg.z_score,
        is_unusual: true,
      });
    }
  } else if (msg.type === "AUTOPILOT_LOG") {
    broker.publish(TOPICS.SYSTEM_INFO_MESSAGE, {
      type: "AUTOPILOT_LOG",
      symbol: msg.symbol,
      message: msg.message,
    });
  } else if (msg.type === "MACRO_REGIME_UPDATE") {
    // Immediately update the high-speed shared memory cache (The Bridge)
    setCachedData("global_macro_regime", msg.state);
    setCachedData("global_macro_metrics", msg.metrics);
    setCachedData("global_killswitch", msg.killswitch_active);

    broker.publish(TOPICS.MACRO_REGIME_UPDATE, {
      symbol: "GLOBAL",
      timestamp: Date.now(),
      state: msg.state,
      killswitch_active: msg.killswitch_active,
      upcoming_events: msg.upcoming_events || [],
      metrics: msg.metrics,
    });
  } else if (msg.type === "STRATEGY_SIGNAL") {
    const payload = msg.data ? msg.data : msg;
    const rawSymbol = payload.symbol || msg.symbol || "UNKNOWN";
    const cleanSymbol = rawSymbol.replace("/", "");

    const isShadowTrade =
      payload.isShadow || msg.isShadow || (!activeApiKey && !activeSecretKey);

    broker.publish(TOPICS.CONFLUENCE_SIGNAL, {
      ...payload,
      symbol: cleanSymbol,
      ticker: cleanSymbol,
      timestamp: new Date().toISOString(),
      strategy_id: payload.strategy_id || payload.signal_type || "HYBRID",
      direction: payload.direction,
      trigger_price: payload.price || 0,
      conditions_met: payload.metadata || {},
      isShadow: isShadowTrade,
    });

    const strategyAction = payload.action ||
        (payload.direction === "LONG"
          ? "BUY"
          : payload.direction === "SHORT"
            ? "SELL"
            : payload.direction);

    const signalData = {
      ...payload,
      symbol: cleanSymbol,
      action: strategyAction,
      isShadow: isShadowTrade,
    };

    // Alpha Leak Fix: This publish natively routes the signal into src/execution/order_router.ts
    broker.publish(TOPICS.STRATEGY_SIGNAL, signalData);
  } else if (msg.type === "UPDATE_RISK") {
    const payload = msg.data ? msg.data : msg;
    broker.publish("UPDATE_RISK", {
      symbol: msg.symbol,
      ...payload,
    });
  } else if (msg.type === "CANDLE_CLOSED") {
    broker.publish(TOPICS.CANDLE_CLOSED, {
      symbol: msg.symbol,
      data: msg.data,
    });
  } else if (msg.type === "RISK_STATE_UPDATE") {
    broker.publish(TOPICS.RISK_STATE_UPDATE, msg.data);
  } else if (msg.type === "INDICATORS_UPDATE") {
    broker.publish(TOPICS.INDICATORS_UPDATE, {
      symbol: msg.symbol,
      tf: msg.tf,
      ts: msg.ts,
      indicators: msg.indicators,
    });
  } else if (msg.type === "SMC_UPDATE") {
    const key = `${msg.symbol}_${msg.tf}`;
    const current = activeSMCCache[key] || { order_blocks: [], fvgs: [] };

    let hasChanges = false;

    if (current.order_blocks.length !== msg.order_blocks.length) {
      hasChanges = true;
    } else {
      for (let i = 0; i < msg.order_blocks.length; i++) {
        let existing = current.order_blocks.find(
          (o: any) => o.id === msg.order_blocks[i].id,
        );
        if (!existing || existing.status !== msg.order_blocks[i].status) {
          hasChanges = true;
          break;
        }
      }
    }

    if (!hasChanges && msg.fvgs) {
      const currentFvgs = current.fvgs || [];
      if (currentFvgs.length !== msg.fvgs.length) {
        hasChanges = true;
      } else {
        for (let i = 0; i < msg.fvgs.length; i++) {
          let existing = currentFvgs.find((o: any) => o.id === msg.fvgs[i].id);
          if (!existing || existing.status !== msg.fvgs[i].status) {
            hasChanges = true;
            break;
          }
        }
      }
    }

    if (hasChanges) {
      activeSMCCache[key] = {
        order_blocks: msg.order_blocks,
        fvgs: msg.fvgs || [],
      };

      broker.publish(TOPICS.SMC_UPDATE, {
        symbol: msg.symbol,
        tf: msg.tf,
        order_blocks: msg.order_blocks,
        fvgs: msg.fvgs || [],
      });
    }
  } else if (msg.type === "HFT_ORDER_PLACED") {
    console.log(`[Quant Engine] HFT Order Placed:`, msg.data);
    broker.publish(TOPICS.EXECUTION_REPORT, msg.data);
  } else if (msg.type === "HFT_ORDER_ERROR") {
    console.error(`[Quant Engine] HFT Order Error:`, msg.error);
    broker.publish(TOPICS.EXECUTION_ERROR, { error: msg.error });
  } else if (msg.type === "SUBSCRIBE_SYMBOL") {
    console.log(`[Quant Engine] Autopilot requested subscription to ${msg.symbol}`);
    connectWebSocket(msg.symbol);
  } else if (msg.status === "INFO" || msg.status === "WARN") {
    console.log(`[Quant Engine ${msg.status}] ${msg.message}`);
    broker.publish(TOPICS.SYSTEM_INFO_MESSAGE, {
      status: msg.status,
      message: msg.message,
    });
  } else if (msg.status === "ERROR" || msg.type === "ERROR") {
    console.error(`[Quant Engine Error] ${msg.message}`);
    broker.publish(TOPICS.EXECUTION_ERROR, { error: msg.message });
    if (msg.traceback) console.error(msg.traceback);
    try {
      require("fs").appendFileSync(
        "quant_error.log",
        msg.message + "\n" + (msg.traceback || "") + "\n",
      );
    } catch (e) {}
  }
}

// 297: startQuantEngine(); is kept commented here as it is called within startServer() now.
// This prevents double initialization during server restarts or reloads.

export function restartQuantEngine() {
  console.log("[Quant Engine] Force restarting due to settings/keys change...");
  isQuantEngineIntentionalClose = true;
  if (quantEngine) {
    quantEngine.kill();
  }
  // Restart immediately after a brief delay for port/process cleanup
  setTimeout(() => {
    isQuantEngineIntentionalClose = false;
    spawnEngine();
  }, 1000);
}

let isSendingZmq = false;
const zmqQueue: string[] = [];

async function processZmqQueue() {
  if (isSendingZmq || zmqQueue.length === 0 || !isZmqBound) return;
  isSendingZmq = true;
  while (zmqQueue.length > 0) {
    const msg = zmqQueue.shift();
    if (!msg) continue;
    try {
      if (isZmqBound) {
        await pubSocket.send(msg);
      }
    } catch (err) {
      console.error("[IPC Error] Failed to send message via ZeroMQ:", err);
    }
  }
  isSendingZmq = false;
}

export function sendToQuantEngine(type: string, data: any) {
  try {
    if (isZmqBound) {
      zmqQueue.push(JSON.stringify({ type, data }));
      processZmqQueue();
    }
  } catch (err) {
    console.error("[IPC Error] Failed to queue message for ZeroMQ:", err);
  }
}
// ----------------------------------

// Liquidity Tracking State (Phase 2.1.5)
const previousDepthState = new Map<
  string,
  { bids: Map<number, number>; asks: Map<number, number> }
>();
const localOrderBooks = new Map<
  string,
  { bids: Map<number, number>; asks: Map<number, number>; lastUpdateId: number }
>();
const LIQUIDITY_SHIFT_THRESHOLD = 50000; // $50k USD shift
const autopilotTrackedAssets = new Set<string>();

// Cleanup idle WebSockets every minute
setInterval(() => {
  const now = Date.now();
  const IDLE_TIMEOUT = 60000; // 1 minute

  activeDepthWs.forEach((session, symbol) => {
    const isAutopilotTracked =
      typeof isAutopilotOn !== "undefined" &&
      isAutopilotOn &&
      autopilotTrackedAssets.has(symbol);
    if (!isAutopilotTracked && now - session.lastAccessed > IDLE_TIMEOUT) {
      console.log(`[WS] Closing idle Depth WS for ${symbol}`);
      session.ws.terminate(); // terminate is safer than close for stuck sockets
      activeDepthWs.delete(symbol);
      previousDepthState.delete(symbol);
      localOrderBooks.delete(symbol);
      sendToQuantEngine("DROP_SYMBOL", { symbol });
    } else {
      // Heartbeat: Send ping to keep connection alive
      if (session.ws.readyState === WebSocket.OPEN) {
        session.ws.ping();
      }
    }
  });

  activeTradeWs.forEach((session, symbol) => {
    const isAutopilotTracked =
      typeof isAutopilotOn !== "undefined" &&
      isAutopilotOn &&
      autopilotTrackedAssets.has(symbol);
    if (!isAutopilotTracked && now - session.lastAccessed > IDLE_TIMEOUT) {
      console.log(`[WS] Closing idle Trade WS for ${symbol}`);
      session.ws.terminate();
      activeTradeWs.delete(symbol);
    } else {
      if (session.ws.readyState === WebSocket.OPEN) {
        session.ws.ping();
      }
    }
  });
}, 60000);

// Notify Python Engine of Position State (Phase 3.4 Runtime State Sync)
setInterval(() => {
  const exchangeInfo = getCachedData("exchangeInfo", 86400000) as any;
  if (!exchangeInfo) return;

  activeDepthWs.forEach((session, upperSymbol) => {
    const symbolInfo = exchangeInfo.symbols.find(
      (s: any) => s.symbol === upperSymbol,
    );
    if (!symbolInfo) return;

    const baseAsset = symbolInfo.baseAsset;
    const baseBalance = parseFloat(accountBalances[baseAsset]?.free || "0");

    const lotSizeFilter = symbolInfo.filters.find(
      (f: any) => f.filterType === "LOT_SIZE",
    );
    const minQty = parseFloat(lotSizeFilter?.minQty || "0.00001");

    const isActive = baseBalance >= minQty;
    sendToQuantEngine("POSITION_STATE", {
      symbol: upperSymbol,
      active: isActive,
      direction: "LONG", // Assuming Spot Trading
    });
  });
}, 1000);

function connectWebSocket(symbol: string) {
  const upperSymbol = symbol.toUpperCase();
  const lowerSymbol = symbol.toLowerCase();

  // 1. Incremental Depth WebSocket (Pure LOB Streaming)
  if (!activeDepthWs.has(upperSymbol)) {
    const setupDepthWs = async () => {
      console.log(`[WS] Initializing Incremental LOB for ${upperSymbol}...`);

      // Phase 0: REST Snapshot Bootstrap for Absolute Baseline
      let lastUpdateId = 0;
      try {
        const snapshot = await binanceAxios.get(
          `${binanceBaseUrl}/api/v3/depth?symbol=${upperSymbol}&limit=1000`,
        );
        const bids = new Map<number, number>();
        const asks = new Map<number, number>();
        snapshot.data.bids.forEach((b: [string, string]) =>
          bids.set(parseFloat(b[0]), parseFloat(b[1])),
        );
        snapshot.data.asks.forEach((a: [string, string]) =>
          asks.set(parseFloat(a[0]), parseFloat(a[1])),
        );
        localOrderBooks.set(upperSymbol, {
          bids,
          asks,
          lastUpdateId: snapshot.data.lastUpdateId,
        });
        lastUpdateId = snapshot.data.lastUpdateId;
      } catch (err) {
        console.error(
          `[WS] Failed to fetch depth snapshot for ${upperSymbol}, starting from empty book.`,
        );
        localOrderBooks.set(upperSymbol, {
          bids: new Map(),
          asks: new Map(),
          lastUpdateId: 0,
        });
      }

      // Phase 1: Millisecond-level Diff Stream
      // Using @depth@100ms for pure incremental updates
      const depthWs = new WebSocket(
        `${BINANCE_WS_URL}/${lowerSymbol}@depth@100ms`,
      );

      const session = { ws: depthWs, lastAccessed: Date.now() };
      activeDepthWs.set(upperSymbol, session);

      depthWs.on("message", (data) => {
        try {
          const parsed = JSON.parse(data.toString());
          const book = localOrderBooks.get(upperSymbol);
          if (!book) return;

          // Mandatory Ignore: Drop updates that are already covered by the snapshot updateId
          // The first update to apply after a snapshot is where U <= lastUpdateId+1 AND u >= lastUpdateId+1
          if (parsed.u <= book.lastUpdateId) return;

          // Apply Incremental Diffs (LOB Delta)
          const processDiffs = (
            diffs: [string, string][],
            targetMap: Map<number, number>,
            side: "BID" | "ASK",
          ) => {
            diffs.forEach(([pStr, qStr]) => {
              const price = parseFloat(pStr);
              const qty = parseFloat(qStr);
              const prevQty = targetMap.get(price) || 0;
              const delta = qty - prevQty;
              const usdValue = delta * price;

              // Immediate Liquidity Shift Detection (The true "Liquidity Delta")
              if (Math.abs(usdValue) >= LIQUIDITY_SHIFT_THRESHOLD) {
                broker.publish(TOPICS.LIQUIDITY_SHIFT, {
                  symbol: upperSymbol,
                  timestamp: parsed.E,
                  side,
                  type: delta > 0 ? "ADDED" : "REMOVED",
                  price,
                  quantity: Math.abs(delta),
                  usd_value: Math.abs(usdValue),
                });
              }

              if (qty === 0) {
                targetMap.delete(price);
              } else {
                targetMap.set(price, qty);
              }
            });

            // Prevent Memory Leak: Trim book to 1000 levels
            if (targetMap.size > 2000) {
              const entries = Array.from(targetMap.entries());
              if (side === "BID") {
                entries.sort((a, b) => b[0] - a[0]); // Descending
              } else {
                entries.sort((a, b) => a[0] - b[0]); // Ascending
              }
              const toKeep = new Map(entries.slice(0, 1000));
              targetMap.clear();
              for (const [p, q] of toKeep) targetMap.set(p, q);
            }
          };

          processDiffs(parsed.b, book.bids, "BID");
          processDiffs(parsed.a, book.asks, "ASK");
          book.lastUpdateId = parsed.u;

          // Prepare Optimized Snapshot for Python Engine (Top 20 Levels)
          const sortedBids = Array.from(book.bids.entries())
            .sort((a, b) => b[0] - a[0])
            .slice(0, 20);
          const sortedAsks = Array.from(book.asks.entries())
            .sort((a, b) => a[0] - b[0])
            .slice(0, 20);

          if (sortedBids.length > 0 && sortedAsks.length > 0) {
            const normalizedBids = sortedBids.map(([p, q]) => ({ p, q }));
            const normalizedAsks = sortedAsks.map(([p, q]) => ({ p, q }));

            const bidVol = normalizedBids.reduce(
              (acc, curr) => acc + curr.q,
              0,
            );
            const askVol = normalizedAsks.reduce(
              (acc, curr) => acc + curr.q,
              0,
            );
            const mid = (normalizedBids[0].p + normalizedAsks[0].p) / 2;
            const imbalance =
              bidVol + askVol > 0 ? (bidVol - askVol) / (bidVol + askVol) : 0;

            const normalizedDepth = {
              symbol: upperSymbol,
              timestamp: parsed.E || Date.now(),
              bids: normalizedBids,
              asks: normalizedAsks,
              mid_price: mid,
              bid_total_volume: bidVol,
              ask_total_volume: askVol,
              imbalance: imbalance,
            };

            setCachedData(`depth_${upperSymbol}`, normalizedDepth);
            sendToQuantEngine("DEPTH", normalizedDepth);
          }
        } catch (e) {
          console.error(
            `[WS] Error processing depth event for ${upperSymbol}:`,
            e,
          );
        }
      });

      depthWs.on("error", (err) =>
        console.error(`[WS] Depth error for ${upperSymbol}:`, err),
      );

      depthWs.on("close", (code) => {
        console.log(`[WS] Depth closed for ${upperSymbol} (Code: ${code})`);
        if (activeDepthWs.get(upperSymbol)?.ws === depthWs) {
          activeDepthWs.delete(upperSymbol);
          // Auto-Restart unless it was intentionally cleaned up by idle timeout
          setTimeout(() => {
            console.log(`[WS] Auto-reconnecting Depth for ${upperSymbol}...`);
            connectWebSocket(upperSymbol);
          }, 2000);
        }
      });

      depthWs.on("pong", () => {
        // Pong received, Connection is alive.
        // We do not update lastAccessed here because lastAccessed reflects client usage.
      });
    };
    setupDepthWs();
  } else {
    activeDepthWs.get(upperSymbol)!.lastAccessed = Date.now();
  }

  // 2. Trade WebSocket
  if (!activeTradeWs.has(upperSymbol)) {
    const setupTradeWs = () => {
      console.log(`[WS] Connecting Trades for ${upperSymbol}...`);
      const tradeWs = new WebSocket(`${BINANCE_WS_URL}/${lowerSymbol}@trade`);

      const session = { ws: tradeWs, lastAccessed: Date.now() };
      activeTradeWs.set(upperSymbol, session);

      tradeWs.on("message", (data) => {
        try {
          const parsed = JSON.parse(data.toString());
          if (parsed.p && parsed.q) {
            const price = parseFloat(parsed.p);
            const qty = parseFloat(parsed.q);
            const symbol = upperSymbol;

            // Large Order Detection (Phase 2.1.4) - Keep in Node for immediate response
            const usdValue = price * qty;
            const LARGE_ORDER_THRESHOLD = 50000; // $50k USD
            if (usdValue >= LARGE_ORDER_THRESHOLD) {
              broker.publish(TOPICS.LARGE_ORDER_DETECTED, {
                symbol,
                timestamp: parsed.T,
                price,
                quantity: qty,
                side: parsed.m ? "SELL" : "BUY",
                usd_value: usdValue,
              });
            }

            const cached = getCachedData(
              `trades_${upperSymbol}`,
              CACHE_TTL.trades,
            );
            const trades = Array.isArray(cached) ? cached : [];
            const formatted = [
              {
                price: parsed.p,
                qty: parsed.q,
                time: parsed.T,
                isBuyerMaker: parsed.m,
              },
              ...trades,
            ].slice(0, 10);
            setCachedData(`trades_${upperSymbol}`, formatted);
            broker.publish(TOPICS.MARKET_DATA_TRADE, {
              symbol: upperSymbol,
              data: parsed,
            });
            sendToQuantEngine("TRADE", { symbol: upperSymbol, data: parsed });
          }
        } catch (e) {}
      });

      tradeWs.on("error", (err) =>
        console.error(`[WS] Trade error for ${upperSymbol}:`, err),
      );

      tradeWs.on("close", (code) => {
        console.log(`[WS] Trade closed for ${upperSymbol} (Code: ${code})`);
        if (activeTradeWs.get(upperSymbol)?.ws === tradeWs) {
          activeTradeWs.delete(upperSymbol);
          setTimeout(() => {
            console.log(`[WS] Auto-reconnecting Trade for ${upperSymbol}...`);
            connectWebSocket(upperSymbol);
          }, 2000);
        }
      });

      tradeWs.on("pong", () => {
        // Pong received, Connection is alive.
        // We do not update lastAccessed here because lastAccessed reflects client usage.
      });

      // Phase 4: Seed Historical Klines for Quant Engine
      const seedHistoricalKlines = async () => {
        try {
          const timeframes = ["1m", "5m", "15m", "1h"];
          for (const tf of timeframes) {
            const response = await binanceAxios.get(
              `${binanceBaseUrl}/api/v3/klines`,
              {
                params: { symbol: upperSymbol, interval: tf, limit: 100 },
              },
            );
            const data = response.data;
            if (Array.isArray(data)) {
              const formattedKlines = data.map((k: any) => ({
                ts: k[0],
                o: parseFloat(k[1]),
                h: parseFloat(k[2]),
                l: parseFloat(k[3]),
                c: parseFloat(k[4]),
                v: parseFloat(k[5]),
                tf: tf,
              }));
              sendToQuantEngine("SEED_KLINES", {
                symbol: upperSymbol,
                tf: tf,
                klines: formattedKlines,
              });
            }
          }
          console.log(`[WS] Historical Klines Seeded for ${upperSymbol}`);
        } catch (err) {
          console.error(
            `[WS] Failed to seed historical klines for ${upperSymbol}:`,
            err,
          );
        }
      };
      seedHistoricalKlines();
    };
    setupTradeWs();
  } else {
    activeTradeWs.get(upperSymbol)!.lastAccessed = Date.now();
  }
}

function startOptionsStream() {
  if (optionsWs) return;

  const endpoints = [
    "wss://nbstream.binance.com/eoptions/stream?streams=!trade@arr/!ticker@arr",
    "wss://vstream.binance.com/stream?streams=!trade@arr/!ticker@arr",
  ];

  const currentEndpoint = endpoints[optionsAttempt % endpoints.length];
  console.log(
    `[WS] Connecting to Combined Binance Options Stream (${currentEndpoint})...`,
  );
  optionsWs = new WebSocket(currentEndpoint);

  optionsWs.on("message", (data) => {
    try {
      const envelope = JSON.parse(data.toString());
      const stream = envelope.stream;
      const payload = envelope.data;

      if (stream && stream.includes("trade")) {
        // Handle Options Trades (Whales/Sweeps)
        if (Array.isArray(payload)) {
          payload.forEach((trade: any) => {
            const symbol = trade.s;
            const price = parseFloat(trade.p);
            const qty = parseFloat(trade.q);
            const usdValue = price * qty;

            if (usdValue >= 5000) {
              const parts = symbol.split("-");
              if (parts.length === 4) {
                const underlying = parts[0];
                const expiry = parts[1];
                const strike = parseFloat(parts[2]);
                const type = parts[3] === "C" ? "CALL" : "PUT";

                broker.publish(TOPICS.OPTIONS_FLOW, {
                  symbol: underlying,
                  timestamp: trade.T,
                  side: trade.m ? "SELL" : "BUY",
                  type,
                  strike,
                  expiry,
                  price,
                  quantity: qty,
                  usd_value: usdValue,
                  is_block_trade: usdValue >= 25000,
                });

                // Bridge to Python Quant Engine
                sendToQuantEngine("OPTIONS_FLOW", {
                  symbol: underlying,
                  timestamp: trade.T,
                  side: trade.m ? "SELL" : "BUY",
                  type,
                  strike,
                  expiry,
                  price,
                  quantity: qty,
                  usd_value: usdValue,
                  is_block_trade: usdValue >= 25000,
                });
              }
            }
          });
        }
      } else if (stream && stream.includes("ticker")) {
        // Handle Options Tickers (GEX / IV / PCR Aggregation)
        if (Array.isArray(payload)) {
          const stats: Record<
            string,
            { callVol: number; putVol: number; avgIV: number; count: number }
          > = {};

          payload.forEach((ticker: any) => {
            const parts = ticker.s.split("-");
            if (parts.length === 4) {
              const underlying = parts[0];
              const type = parts[3];
              const vol = parseFloat(ticker.V) || 0;
              const iv = parseFloat(ticker.vo) || 0;

              if (!stats[underlying])
                stats[underlying] = {
                  callVol: 0,
                  putVol: 0,
                  avgIV: 0,
                  count: 0,
                };

              if (type === "C") stats[underlying].callVol += vol;
              else stats[underlying].putVol += vol;

              if (iv > 0) {
                stats[underlying].avgIV += iv;
                stats[underlying].count++;
              }
            }
          });

          Object.keys(stats).forEach((symbol) => {
            const s = stats[symbol];
            const pcr = s.callVol > 0 ? s.putVol / s.callVol : 1;
            const iv = s.count > 0 ? s.avgIV / s.count : 0;

            const snapshot = {
              symbol,
              timestamp: Date.now(),
              put_call_ratio: pcr,
              implied_volatility: iv,
            };

            broker.publish(TOPICS.OPTIONS_SNAPSHOT, snapshot as any);
            // Bridge to Python for macro/gex context purely via WS
            sendToQuantEngine("OPTIONS_SNAPSHOT", snapshot);
          });
        }
      }
    } catch (e) {}
  });

  optionsWs.on("error", (err: any) => {
    const errMsg = err?.message || String(err);
    const isWafBlock =
      errMsg.includes("503") ||
      errMsg.includes("302") ||
      errMsg.includes("404") ||
      errMsg.includes("Unexpected server response");

    if (isWafBlock) {
      console.log(
        `[WS] Options endpoint ${currentEndpoint} blocked/invalid (${errMsg}).`,
      );
      optionsWs = null;

      if (optionsAttempt < endpoints.length - 1) {
        optionsAttempt++;
        console.log(`[WS] Retrying with secondary endpoint...`);
        setTimeout(startOptionsStream, 2000);
      } else {
        console.log(
          "[WS] All Binance Options endpoints blocked. Falling back to simulated flow.",
        );
        startSimulatedOptionsFlow();
      }
    } else {
      console.error("[WS] Options error:", errMsg);
    }
  });
  optionsWs.on("close", () => {
    if (optionsWs !== null) {
      console.log("[WS] Options stream closed. Reconnecting...");
      optionsWs = null;
      setTimeout(startOptionsStream, 5000);
    }
  });
}

let simulationInterval: NodeJS.Timeout | null = null;

function startSimulatedOptionsFlow() {
  if (simulationInterval) return;

  console.log("[WS] Started Simulated Options Flow");
  simulationInterval = setInterval(() => {
    const symbols = ["BTC", "ETH", "SOL"];
    const symbol = symbols[Math.floor(Math.random() * symbols.length)];
    const isCall = Math.random() > 0.5;
    const side = Math.random() > 0.5 ? "BUY" : "SELL";

    // Use real-time prices if available, otherwise fallback to defaults
    const ticker = getCachedData(`ticker_${symbol}USDT`, 60000) as any;
    const basePrice = ticker
      ? parseFloat(ticker.lastPrice)
      : symbol === "BTC"
        ? 65000
        : symbol === "ETH"
          ? 3500
          : 150;

    const strikeOffset = (Math.random() - 0.5) * 0.1; // +/- 5% (more realistic for block trades)
    const strike = Math.round((basePrice * (1 + strikeOffset)) / 100) * 100;

    // Increase quantity and price range to trigger "Massive Sweeps" (> $50k)
    const qty = Math.random() * 50 + 10;
    const price = basePrice * (Math.random() * 0.08 + 0.02); // Option price 2-10% of underlying
    const usdValue = qty * price;

    if (usdValue >= 5000) {
      const date = new Date();
      // 40% chance of short expiry (<= 7 days) to trigger sweep alerts
      const daysAhead =
        Math.random() < 0.4
          ? Math.floor(Math.random() * 7)
          : Math.floor(Math.random() * 30) + 1;
      date.setDate(date.getDate() + daysAhead);
      const expiry = `${date.getFullYear().toString().slice(2)}${(date.getMonth() + 1).toString().padStart(2, "0")}${date.getDate().toString().padStart(2, "0")}`;

      broker.publish(TOPICS.OPTIONS_FLOW, {
        symbol,
        timestamp: Date.now(),
        side,
        type: isCall ? "CALL" : "PUT",
        strike,
        expiry,
        price,
        quantity: qty,
        usd_value: usdValue,
        is_block_trade: usdValue >= 25000,
      });
    }
  }, 3000); // Simulate a trade every 3 seconds
}

// Utility to sign requests for private endpoints
function signRequest(params: Record<string, any>, secretKeyOverride?: string) {
  const secretToUse = secretKeyOverride || activeSecretKey;
  if (!secretToUse) {
    throw new Error("BINANCE_SECRET_KEY is missing from configuration");
  }

  const queryString = Object.keys(params)
    .filter((key) => params[key] !== undefined && params[key] !== null)
    .map((key) => {
      let value = params[key];
      // Convert numbers to fixed strings to avoid scientific notation (e.g., 1e-8)
      if (typeof value === "number") {
        // Use a safe fixed precision that covers all Binance assets (8 decimals is standard)
        // For prices, 8 is enough. For quantities, 8 is also standard for BTC pairings.
        value = value.toLocaleString("en-US", {
          useGrouping: false,
          minimumFractionDigits: 0,
          maximumFractionDigits: 10,
        });
      }
      return `${key}=${encodeURIComponent(value)}`;
    })
    .join("&");

  const signature = crypto
    .createHmac("sha256", secretToUse)
    .update(queryString)
    .digest("hex");

  return `${queryString}&signature=${signature}`;
}

// Fetch exchange info internally to keep cache warm
async function getInternalExchangeInfo() {
  const cacheKey = "exchangeInfo";
  let cached = getCachedData(cacheKey, CACHE_TTL.exchangeInfo);
  if (!cached) {
    try {
      const response = await binanceAxios.get(
        `${binanceBaseUrl}/api/v3/exchangeInfo`,
      );
      setCachedData(cacheKey, response.data);
      cached = response.data;
    } catch (error) {
      console.error("Failed to fetch internal exchange info:", error);
      throw error;
    }
  }
  return cached;
}

// User Data Stream & Account Balances
let accountBalances: Record<string, { free: string; locked: string }> = {};
let userDataListenKey: string | null = null;
let userDataWs: WebSocket | null = null;
let userDataKeepAliveInterval: NodeJS.Timeout | null = null;
let isUserDataIntentionalClose = false;

async function fetchInitialBalances(): Promise<boolean> {
  if (!activeApiKey || !activeSecretKey) return false;
  try {
    // Clear old balances before fetching new ones (critical for account switching)
    Object.keys(accountBalances).forEach((key) => delete accountBalances[key]);

    const signedQuery = signRequest({ timestamp: Date.now() });
    const response = await binanceAxios.get(
      `${binanceBaseUrl}/api/v3/account?${signedQuery}`,
      {
        headers: { "X-MBX-APIKEY": activeApiKey },
      },
    );
    if (response.data && response.data.balances) {
      response.data.balances.forEach((b: any) => {
        accountBalances[b.asset] = { free: b.free, locked: b.locked };
      });
      console.log("✅ Initial account balances fetched.");
      broker.publish(TOPICS.USER_BALANCE_UPDATE, accountBalances);
      sendToQuantEngine("BALANCE_UPDATE", accountBalances);
      return true;
    }
    return false;
  } catch (error: any) {
    if (
      error.response &&
      (error.response.status === 401 || error.response.status === 403 || error.response.status === 400)
    ) {
      console.warn(
        "⚠️ Cannot authenticate initially. Please provide correct Binance API keys via the UI.",
      );
    } else {
      let msg = error.response?.data?.msg || error.message;
      console.warn("Failed to fetch initial balances:", msg);
    }
    return false;
  }
}

let accountPollInterval: NodeJS.Timeout | null = null;

export function closeUserDataStream() {
  isUserDataIntentionalClose = true;
  if (userDataWs) {
    try {
      userDataWs.terminate();
    } catch (e) {}
    userDataWs = null;
  }
  if (userDataKeepAliveInterval) {
    clearInterval(userDataKeepAliveInterval);
    userDataKeepAliveInterval = null;
  }
  if (accountPollInterval) {
    clearInterval(accountPollInterval);
    accountPollInterval = null;
  }
}

async function startUserDataStream() {
  if (!activeApiKey || !activeSecretKey) return;

  // Binance deprecated the User Data Stream via REST /api/v3/userDataStream in 2026.
  // Instead of trying to connect to a 410 Gone endpoint, we fall back to polling.
  closeUserDataStream();
  isUserDataIntentionalClose = false;

  console.log(
    "ℹ️ User Data Stream (listenKey) deprecated. Initiating resilient high-frequency 1s fallback polling...",
  );

  let continuousErrorCount = 0;

  accountPollInterval = setInterval(async () => {
    if (isUserDataIntentionalClose || !activeApiKey) return;
    try {
      const signedQuery = signRequest({ timestamp: Date.now() });
      const response = await binanceAxios.get(
        `${binanceBaseUrl}/api/v3/account?${signedQuery}`,
        {
          headers: { "X-MBX-APIKEY": activeApiKey },
        },
      );
      continuousErrorCount = 0; // reset on success
      if (response.data && response.data.balances) {
        response.data.balances.forEach((b: any) => {
          accountBalances[b.asset] = { free: b.free, locked: b.locked };
        });

        // Broadcast the update so the UI and quant engine get it
        broker.publish(TOPICS.USER_BALANCE_UPDATE, accountBalances);
        sendToQuantEngine("BALANCE_UPDATE", accountBalances);
      }
    } catch (e: any) {
      continuousErrorCount++;
      // Stop polling completely if we get auth/permission errors (-2015, -2014) 
      // or if we fail continuously
      const isAuthError = e.response?.data?.code === -2015 || e.response?.data?.code === -2014;
      if (isAuthError || continuousErrorCount >= 10) {
        console.warn(`[User Data Stream] Stopping polling due to persistent errors or invalid keys. Code: ${e.response?.data?.code}`);
        closeUserDataStream();
      }
    }
  }, 2000);
}

// Initialize cache and user data on startup
// getInternalExchangeInfo().catch(console.error);
// startOptionsStream();
// if (BINANCE_API_KEY && BINANCE_SECRET_KEY) {
//   fetchInitialBalances().then(startUserDataStream).catch(console.error);
// }

// Validator function
async function validateOrderConstraints(
  symbol: string,
  price: number,
  quantity: number,
) {
  const exchangeInfo = await getInternalExchangeInfo();
  const symbolInfo = exchangeInfo.symbols.find((s: any) => s.symbol === symbol);

  if (!symbolInfo) {
    return {
      valid: false,
      reason: `Symbol ${symbol} not found in exchange info.`,
    };
  }

  const priceFilter = symbolInfo.filters.find(
    (f: any) => f.filterType === "PRICE_FILTER",
  );
  const lotSize = symbolInfo.filters.find(
    (f: any) => f.filterType === "LOT_SIZE",
  );
  const notional = symbolInfo.filters.find(
    (f: any) => f.filterType === "NOTIONAL" || f.filterType === "MIN_NOTIONAL",
  );

  if (priceFilter && !isNaN(price) && price > 0) {
    const minPrice = parseFloat(priceFilter.minPrice);
    const maxPrice = parseFloat(priceFilter.maxPrice);

    if (price < minPrice)
      return {
        valid: false,
        reason: `Price ${price} is below minimum ${minPrice}`,
      };
    if (price > maxPrice)
      return {
        valid: false,
        reason: `Price ${price} is above maximum ${maxPrice}`,
      };
    // Strict modulo (tickSize) check removed. Let order_router.ts handle precise snapping to tick size.
  }

  if (lotSize) {
    const minQty = parseFloat(lotSize.minQty);
    const maxQty = parseFloat(lotSize.maxQty);

    if (quantity < minQty)
      return {
        valid: false,
        reason: `Quantity ${quantity} is below minimum ${minQty}`,
      };
    if (quantity > maxQty)
      return {
        valid: false,
        reason: `Quantity ${quantity} is above maximum ${maxQty}`,
      };
    // Strict modulo (stepSize) check removed. Let order_router.ts handle safe truncation.
  }

  if (notional) {
    const minNotional = parseFloat(notional.minNotional);
    let checkPrice = price;

    // For MARKET orders (price = 0), estimate notional using the orderbook
    if (checkPrice <= 0) {
      const depth = getCachedData(`depth_${symbol}`, 5000) as any;
      if (depth && depth.bids && depth.bids.length > 0) {
        checkPrice = depth.bids[0].p;
      }
    }

    if (checkPrice > 0 && checkPrice * quantity < minNotional) {
      return {
        valid: false,
        reason: `Order value (~${(checkPrice * quantity).toFixed(2)}) is below minimum notional ${minNotional}`,
      };
    }
  }

  return { valid: true };
}

// System Heartbeat for Phase 0.1
setInterval(() => {
  broker.publish(TOPICS.SYSTEM_HEARTBEAT, {
    timestamp: Date.now(),
    status: "OK",
  });
}, 5000);

async function startServer() {
  // Load dynamic keys from DB / ENV into memory
  loadKeysFromDB();

  // Pre-fetch critical metadata so order router doesn't block signals on cold starts
  getInternalExchangeInfo().then(() => {
    console.log("[Boot] Pre-fetched Binance Exchange Info.");
  }).catch((e) => {
    console.error("[Boot] WARNING: Could not pre-fetch exchange info.", e);
  });

  const app = express();
  const PORT = 3000;
  const listenPort =
    process.env.NODE_ENV === "test" || process.env.VITEST ? 0 : PORT;

  // Initialize the Database Logger (Phase 0.3)
  initializeDbLogger();

  // Initialize the Order Router (Phase 1.2)
  initializeOrderRouter();

  async function sendIntegrationsNotification(order: any, responseData: any) {
    const telegramBotToken = getSetting("TELEGRAM_BOT_TOKEN");
    const telegramChatId = getSetting("TELEGRAM_CHAT_ID");
    const webhookUrl = getSetting("WEBHOOK_URL");

    if (!telegramBotToken && !telegramChatId && !webhookUrl) return;

    const isBuy = order.side === "BUY";
    const icon = isBuy ? "🟢" : "🔴";

    const message =
      `${icon} *Shamrock Trade Execution*\n` +
      `Symbol: ${order.symbol}\n` +
      `Side: ${order.side}\n` +
      `Type: ${order.type}\n` +
      `Quantity: ${order.quantity}\n` +
      `Price: ${order.price || "Market"}\n` +
      `Status: ${responseData?.status || "NEW"}\n` +
      `Time: ${new Date().toUTCString()}`;

    // Telegram
    if (telegramBotToken && telegramChatId) {
      try {
        await axios.post(
          `https://api.telegram.org/bot${telegramBotToken}/sendMessage`,
          {
            chat_id: telegramChatId,
            text: message,
          },
        );
      } catch (e: any) {
        console.error(
          "[Integrations] Telegram notification failed:",
          e.response?.data || e.message,
        );
      }
    }

    // Webhook (Google Sheets, Make, Zapier, etc)
    if (webhookUrl) {
      try {
        const macroState = getCachedData("global_macro_snapshot", 60000) || {};
        const payload = {
          timestamp: Date.now(),
          symbol: order.symbol,
          side: order.side,
          type: order.type,
          quantity: order.quantity,
          price: order.price || responseData?.price || 0,
          status: responseData?.status || "NEW",
          internalOrderId: order.internal_order_id || "SYSTEM_EVENT",
          market: order.market,
          action: order.metadata?.action,
          macroConditions:
            Object.keys(macroState).length > 0 ? macroState : undefined,
        };

        await axios.post(webhookUrl, payload, {
          headers: { "Content-Type": "application/json" },
        });
      } catch (e: any) {
        console.error(
          "[Integrations] Webhook notification failed:",
          e.response?.data || e.message,
        );
      }
    }
  }

  // Initialize the Execution Engine (Phase 2.3)
  broker.subscribe(TOPICS.EXECUTE_ORDER, async (order) => {
    if ((!activeApiKey || !activeSecretKey) && !order.isShadow) {
      console.error(
        "[Execution Engine] API keys not configured. Cannot execute live order:",
        order.internal_order_id,
      );
      return;
    }

    // 3. Killswitch & Circuit Breaker Logic (Bypassing Execution Manager fix)
    const isKillswitchActive = getCachedData("global_killswitch", 10000);
    if (
      isKillswitchActive && 
      (!order.type || order.type !== "CANCEL") && 
      (!order.metadata?.action || (order.metadata.action !== "EXIT" && order.metadata.action !== "CLOSE_LONG" && order.metadata.action !== "CLOSE_SHORT"))
    ) {
      console.warn(`[Execution Engine] BLOCKED EXECUTION: Global Killswitch is ACTIVE. Blocking new order for ${order.symbol}`);
      return;
    }

    // 4. Algo Execution Manager (TWAP/VWAP routing for large AUTO orders like Hedge/Arbitrage)
    if (!order.algo && order.type === "MARKET" && order.quantity) {
      const qtyNum = parseFloat(order.quantity.toString());
      const assumedPrice = order.price ? parseFloat(order.price.toString()) : (order.symbol.includes("BTC") ? 95000 : (order.symbol.includes("ETH") ? 3000 : 1));
      const estimatedNotional = qtyNum * assumedPrice;
      
      if (estimatedNotional > 50000) {
        console.log(`[Execution Engine] Large MARKET order detected (Est. Notional $${estimatedNotional.toFixed(0)}), auto-slicing via TWAP to minimize slippage.`);
        (order as any).algo = "TWAP";
        (order as any).algoParams = { durationMs: 5 * 60 * 1000, sliceCount: 5 }; // 5 minute TWAP, 5 slices
      }
    }

    if (order.algo && (order.algo === "TWAP" || order.algo === "VWAP" || order.algo === "PEG_BBO")) {
      algoExecutionManager.dispatch(order);
      return;
    }

    if (order.isShadow) {
      console.log(
        `[Execution Engine] SHADOW MODE: Mocking execution for ${order.internal_order_id} on ${order.market || "SPOT"}`,
      );

      // Send a fake execution report to broker so db_logger records it
      const fakeStatus = order.type === "MARKET" ? "FILLED" : "NEW";
      broker.publish(TOPICS.USER_ORDER_UPDATE, {
        c: order.internal_order_id,
        s: order.symbol,
        S: order.side,
        o: order.type,
        q: order.quantity.toString(),
        p: (order.price || "0").toString(),
        T: Date.now(),
        X: fakeStatus,
        isShadow: true,
      });

      // Update position state in Python based on execution
      const action =
        order.metadata?.action || (order.side === "BUY" ? "BUY" : "SELL");
      let direction = "NONE";
      if (action === "BUY") direction = "LONG";
      if (action === "SELL") direction = "SHORT";
      if (action === "CLOSE_LONG" || action === "CLOSE_SHORT")
        direction = "NONE";
      const isActive = direction !== "NONE";

      sendToQuantEngine("POSITION_STATE", {
        symbol: order.symbol,
        active: isActive,
        direction: direction as "LONG" | "SHORT" | "NONE",
        isShadow: true,
      });
      broker.publish(TOPICS.POSITION_STATE, {
        symbol: order.symbol,
        active: isActive,
        direction: direction as "LONG" | "SHORT" | "NONE",
        isShadow: true,
      });
      return;
    }

    try {
      // Base URL routing
      let targetBaseUrl = binanceBaseUrl;
      let orderEndpoint = "/api/v3/order";
      let ocoEndpoint = "/api/v3/order/oco";

      if (order.market === "FUTURES") {
        targetBaseUrl = useTestnet
          ? "https://testnet.binancefuture.com"
          : "https://fapi.binance.com";
        orderEndpoint = "/fapi/v1/order";
        ocoEndpoint = ""; // Not supported directly in the same way, requires multiple orders
      } else if (order.market === "OPTIONS") {
        targetBaseUrl = useTestnet
          ? "https://testnet.binanceops.com"
          : "https://eapi.binance.com";
        orderEndpoint = "/eapi/v1/order";
        ocoEndpoint = "";
      }

      if (order.type === "CANCEL") {
        if (!order.clientOrderId)
          throw new Error("CANCEL type requires clientOrderId.");

        let targetTypeBaseUrl = binanceBaseUrl;
        let endpoint = "/api/v3/order";
        if (
          order.internal_order_id &&
          order.internal_order_id.includes("oco")
        ) {
          endpoint = "/api/v3/orderList"; // OCOs use orderList endpoint for cancellation on SPOT
        }

        const cancelParams: Record<string, string | number> = {
          symbol: order.symbol,
          origClientOrderId: order.clientOrderId,
          timestamp: Date.now(),
        };
        const signedCancel = signRequest(cancelParams);
        const url = `${targetBaseUrl}${endpoint}?${signedCancel}`;

        await withCircuitBreaker(() =>
          binanceAxios.delete(url, {
            headers: { "X-MBX-APIKEY": activeApiKey },
          }),
        );
        console.log(
          `[Execution Engine] Successfully canceled order ${order.clientOrderId}`,
        );
        return;
      }

      if (order.type === "CANCEL_REPLACE") {
        if (!order.cancelReplaceClientOrderId)
          throw new Error(
            "CANCEL_REPLACE type requires cancelReplaceClientOrderId.",
          );

        let endpoint = "/api/v3/order/cancelReplace";
        const replaceParams: Record<string, string | number> = {
          symbol: order.symbol,
          cancelReplaceMode: "ALLOW_FAILURE",
          cancelOrigClientOrderId: order.cancelReplaceClientOrderId,
          side: order.side,
          type: "LIMIT_MAKER",
          quantity: order.quantity,
          price: order.price,
          newClientOrderId: order.internal_order_id,
          timestamp: Date.now(),
        };

        const signedReplace = signRequest(replaceParams);
        const url = `${targetBaseUrl}${endpoint}?${signedReplace}`;

        await withCircuitBreaker(() =>
          binanceAxios.post(url, null, {
            headers: { "X-MBX-APIKEY": activeApiKey },
          }),
        );
        console.log(
          `[Execution Engine] Successfully cancel-replaced order ${order.cancelReplaceClientOrderId} with new order ${order.internal_order_id} at price ${order.price}`,
        );
        return;
      }

      // 1. OCO (Take Profit + Stop Loss)
      if (order.type === "OCO") {
        if (!order.price || !order.stopPrice) {
          throw new Error(
            "OCO orders require both 'price' (Take Profit) and 'stopPrice' (Stop Loss).",
          );
        }

        if (order.market === "FUTURES") {
          // Emulate OCO with 2 conditional orders
          const tpParams: Record<string, string | number> = {
            symbol: order.symbol,
            side: order.side,
            type: "TAKE_PROFIT_MARKET",
            stopPrice: order.price, // Trigger price is the TP target
            quantity: order.quantity,
            reduceOnly: "true",
            timestamp: Date.now(),
          };
          const slParams: Record<string, string | number> = {
            symbol: order.symbol,
            side: order.side,
            type: "STOP_MARKET",
            stopPrice: order.stopPrice,
            quantity: order.quantity,
            reduceOnly: "true",
            timestamp: Date.now() + 1,
          };

          const slSig = signRequest(slParams);
          await withCircuitBreaker(() =>
            binanceAxios.post(
              `${targetBaseUrl}${orderEndpoint}?${slSig}`,
              null,
              { headers: { "X-MBX-APIKEY": activeApiKey } },
            ),
          );

          const tpSig = signRequest(tpParams);
          await withCircuitBreaker(() =>
            binanceAxios.post(
              `${targetBaseUrl}${orderEndpoint}?${tpSig}`,
              null,
              { headers: { "X-MBX-APIKEY": activeApiKey } },
            ),
          );

          console.log(
            `[Execution Engine] Successfully emulated OCO ${order.internal_order_id} on FUTURES.`,
          );
          return;
        } else if (!ocoEndpoint) {
          throw new Error(
            `OCO not directly supported via single API call for market ${order.market}`,
          );
        }

        const params: Record<string, string | number> = {
          symbol: order.symbol,
          side: order.side,
          quantity: order.quantity,
          price: order.price,
          stopPrice: order.stopPrice,
          stopLimitPrice: order.stopLimitPrice || order.stopPrice,
          stopLimitTimeInForce: order.timeInForce || "GTC",
          listClientOrderId: order.internal_order_id,
          timestamp: Date.now(),
        };

        const signedQueryString = signRequest(params);
        const response = await withCircuitBreaker(() =>
          binanceAxios.post(
            `${targetBaseUrl}${ocoEndpoint}?${signedQueryString}`,
            null,
            {
              headers: { "X-MBX-APIKEY": activeApiKey },
            },
          ),
        );

        console.log(
          `[Execution Engine] Successfully executed OCO ${order.internal_order_id} on ${order.market || "SPOT"}:`,
          response.data,
        );
        sendIntegrationsNotification(order, response.data).catch(console.error);
        return;
      }

      // 2. Standard Orders (MARKET, LIMIT, LIMIT_MAKER, STOP_LOSS, etc.)
      const params: Record<string, string | number> = {
        symbol: order.symbol,
        side: order.side,
        type: order.type,
        quantity: order.quantity,
        timestamp: Date.now(),
        newClientOrderId: order.internal_order_id,
      };

      // Add reduceOnly for Futures exits
      if (order.reduce_only && order.market === "FUTURES") {
        params.reduceOnly = "true";
      }

      // Handle Price
      if (
        [
          "LIMIT",
          "LIMIT_MAKER",
          "STOP_LOSS_LIMIT",
          "TAKE_PROFIT_LIMIT",
        ].includes(order.type)
      ) {
        if (!order.price)
          throw new Error(`Order type ${order.type} requires a 'price'.`);
        params.price = order.price;
      }

      // Handle Stop Price
      if (
        [
          "STOP_LOSS",
          "STOP_LOSS_LIMIT",
          "TAKE_PROFIT",
          "TAKE_PROFIT_LIMIT",
          "STOP_MARKET",
          "TAKE_PROFIT_MARKET",
        ].includes(order.type)
      ) {
        if (!order.stopPrice)
          throw new Error(`Order type ${order.type} requires a 'stopPrice'.`);
        params.stopPrice = order.stopPrice;
      }

      // Handle Time In Force
      if (
        ["LIMIT", "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT"].includes(order.type)
      ) {
        params.timeInForce = order.timeInForce || "GTC";
      }
      // Note: LIMIT_MAKER explicitly forbids timeInForce. MARKET forbids it.

      // Handle Iceberg
      if (order.icebergQty) {
        params.icebergQty = order.icebergQty;
      }

      const signedQueryString = signRequest(params);
      const response = await withCircuitBreaker(() =>
        binanceAxios.post(
          `${targetBaseUrl}${orderEndpoint}?${signedQueryString}`,
          null,
          {
            headers: { "X-MBX-APIKEY": activeApiKey },
          },
        ),
        1,
        order.symbol
      );

      console.log(
        `[Execution Engine] Successfully executed ${order.type} order ${order.internal_order_id} on ${order.market || "SPOT"}:`,
        response.data,
      );
      
      // Feature 1: Slippage Spike Detection
      if (order.type === "MARKET" && order.price) {
         let avgFillPrice = 0;
         if (response.data.fills && response.data.fills.length > 0) {
            let totalQty = 0;
            let totalCost = 0;
            for (const fill of response.data.fills) {
               totalQty += parseFloat(fill.qty);
               totalCost += parseFloat(fill.qty) * parseFloat(fill.price);
            }
            if (totalQty > 0) avgFillPrice = totalCost / totalQty;
         } else if (response.data.cummulativeQuoteQty && response.data.executedQty) {
            const executedQty = parseFloat(response.data.executedQty);
            if (executedQty > 0) avgFillPrice = parseFloat(response.data.cummulativeQuoteQty) / executedQty;
         }
         
         if (avgFillPrice > 0) {
            globalCircuitBreaker.trackSlippage(order.symbol, parseFloat(order.price.toString()), avgFillPrice);
         }
      }

      sendIntegrationsNotification(order, response.data).catch(console.error);

      // Prompt 5: Immediately upon successful execution, place an OCO limit order using stop_loss and take_profit
      // OCOs require explicit take_profit and stop_loss.
      const sl = order.metadata?.stop_loss || order.metadata?.stopLoss;
      const tp = order.metadata?.take_profit || order.metadata?.takeProfit;

      if (sl && tp) {
        if (order.side === "BUY" || order.side === "SELL") {
          const ocoSide = order.side === "BUY" ? "SELL" : "BUY";
          const ocoParams: Record<string, string | number> = {
            symbol: order.symbol,
            side: ocoSide,
            quantity: response.data?.executedQty || order.quantity, // Use actual executed qty if available
            price: tp,
            stopPrice: sl,
            stopLimitPrice: sl,
            stopLimitTimeInForce: "GTC",
            timestamp: Date.now(),
          };

          try {
            const ocoEndpoint = "/api/v3/order/oco"; // Assuming SPOT. FUTURES needs conditional logic (handled above in explicit OCO type, but we enforce atomic spot here per prompt)
            if (!order.market || order.market === "SPOT") {
              const ocoSig = signRequest(ocoParams);
              const ocoRes = await withCircuitBreaker(() =>
                binanceAxios.post(
                  `${targetBaseUrl}${ocoEndpoint}?${ocoSig}`,
                  null,
                  {
                    headers: { "X-MBX-APIKEY": activeApiKey },
                  },
                ),
                1,
                order.symbol
              );
              console.log(
                `[Execution Engine] Successfully chained atomic OCO for ${order.internal_order_id}:`,
                ocoRes.data,
              );
            }
          } catch (ocoErr: any) {
            console.error(
              `[CRITICAL] Failed to execute chained OCO for ${order.internal_order_id}. Manual intervention required:`,
              ocoErr.response?.data || ocoErr.message,
            );
          }
        }
      }

      // Update position state in Python based on execution
      const action =
        order.metadata?.action || (order.side === "BUY" ? "BUY" : "SELL");
      const isEntering = action === "BUY" || action === "SELL"; // Assuming SPOT / Futures entering
      // Convert to LONG/SHORT for direction
      let direction = "NONE";
      if (action === "BUY") direction = "LONG";
      if (action === "SELL") direction = "SHORT"; // Only applicable for futures if we allow SHORT, otherwise SPOT sell is closing
      if (action === "CLOSE_LONG" || action === "CLOSE_SHORT")
        direction = "NONE";

      const isActive = direction !== "NONE";

      sendToQuantEngine("POSITION_STATE", {
        symbol: order.symbol,
        active: isActive,
        direction: direction as "LONG" | "SHORT" | "NONE",
      });
      broker.publish(TOPICS.POSITION_STATE, {
        symbol: order.symbol,
        active: isActive,
        direction: direction as "LONG" | "SHORT" | "NONE",
      });
    } catch (error: any) {
      console.error(
        `[Execution Engine] Failed to execute order ${order.internal_order_id}:`,
        error.response?.data || error.message,
      );
      // Feedback loop: Notify Python Quant Engine that the execution failed
      // This allows the strategy to reset its cooldown and retry or adjust.
      sendToQuantEngine("SIGNAL_REJECTED", {
        symbol: order.symbol,
        reason: `EXECUTION_ENGINE_ERROR: ${error.response?.data?.msg || error.message}`,
      });
    }
  });

  // Handle Market Data Requests from other modules (e.g. Order Router)
  broker.subscribe(TOPICS.MARKET_DATA_REQUEST, (req) => {
    connectWebSocket(req.symbol);
  });

  broker.subscribe(TOPICS.OPTIONS_FLOW, (msg) => {
    sendToQuantEngine("OPTIONS_FLOW", msg);
  });

  // Middleware
  app.use(express.json());

  // Real-time SSE Stream for Frontend
  app.get("/api/stream", (req, res) => {
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders();

    const sendEvent = (event: string, data: any) => {
      res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
    };

    // Send initial connection success
    sendEvent("connected", { timestamp: Date.now() });

    // Subscribe to broker topics
    const onTrade = (msg: any) => sendEvent("trade", msg);
    const onBalance = (msg: any) => sendEvent("balance", msg);
    const onOrder = (msg: any) => sendEvent("order", msg);
    const onVolumeSpike = (msg: any) => sendEvent("volume_spike", msg);
    const onCvd = (msg: any) => sendEvent("cvd", msg);
    const onLargeOrder = (msg: any) => sendEvent("large_order", msg);
    const onLiquidityShift = (msg: any) => sendEvent("liquidity_shift", msg);
    const onOptionsFlow = (msg: any) => sendEvent("options_flow", msg);
    const onIceberg = (msg: any) => sendEvent("iceberg", msg);
    const onSpoofing = (msg: any) => sendEvent("spoofing", msg);
    const onOptionsSweep = (msg: any) => sendEvent("options_sweep", msg);
    const onGammaExposure = (msg: any) => sendEvent("gamma_exposure", msg);
    const onMacroRegime = (msg: any) => sendEvent("macro_regime", msg);
    const onCandleClosed = (msg: any) => sendEvent("candle_closed", msg);
    const onIndicatorsUpdate = (msg: any) =>
      sendEvent("indicators_update", msg);
    const onAlphaSignal = (msg: any) => sendEvent("alpha_signal", msg);
    const onStrategySignal = (msg: any) => sendEvent("signal", msg);
    const onRiskState = (msg: any) => sendEvent("risk_state", msg);

    broker.subscribe(TOPICS.MARKET_DATA_TRADE, onTrade);
    broker.subscribe(TOPICS.USER_BALANCE_UPDATE, onBalance);
    broker.subscribe(TOPICS.USER_ORDER_UPDATE, onOrder);
    broker.subscribe(TOPICS.VOLUME_SPIKE, onVolumeSpike);
    broker.subscribe(TOPICS.CUMULATIVE_VOLUME_DELTA, onCvd);
    broker.subscribe(TOPICS.LARGE_ORDER_DETECTED, onLargeOrder);
    broker.subscribe(TOPICS.LIQUIDITY_SHIFT, onLiquidityShift);
    broker.subscribe(TOPICS.OPTIONS_FLOW, onOptionsFlow);
    broker.subscribe(TOPICS.ICEBERG_DETECTED, onIceberg);
    broker.subscribe(TOPICS.SPOOFING_DETECTED, onSpoofing);
    broker.subscribe(TOPICS.OPTIONS_SWEEP_DETECTED, onOptionsSweep);
    broker.subscribe(TOPICS.GAMMA_EXPOSURE_ALERT, onGammaExposure);
    broker.subscribe(TOPICS.MACRO_REGIME_UPDATE, onMacroRegime);
    broker.subscribe(TOPICS.CANDLE_CLOSED, onCandleClosed);
    broker.subscribe(TOPICS.INDICATORS_UPDATE, onIndicatorsUpdate);
    broker.subscribe("ALPHA_SIGNAL", onAlphaSignal);
    broker.subscribe(TOPICS.STRATEGY_SIGNAL, onStrategySignal);
    broker.subscribe(TOPICS.RISK_STATE_UPDATE, onRiskState);

    req.on("close", () => {
      broker.unsubscribe(TOPICS.MARKET_DATA_TRADE, onTrade);
      broker.unsubscribe(TOPICS.USER_BALANCE_UPDATE, onBalance);
      broker.unsubscribe(TOPICS.USER_ORDER_UPDATE, onOrder);
      broker.unsubscribe(TOPICS.VOLUME_SPIKE, onVolumeSpike);
      broker.unsubscribe(TOPICS.CUMULATIVE_VOLUME_DELTA, onCvd);
      broker.unsubscribe(TOPICS.LARGE_ORDER_DETECTED, onLargeOrder);
      broker.unsubscribe(TOPICS.LIQUIDITY_SHIFT, onLiquidityShift);
      broker.unsubscribe(TOPICS.OPTIONS_FLOW, onOptionsFlow);
      broker.unsubscribe(TOPICS.ICEBERG_DETECTED, onIceberg);
      broker.unsubscribe(TOPICS.SPOOFING_DETECTED, onSpoofing);
      broker.unsubscribe(TOPICS.OPTIONS_SWEEP_DETECTED, onOptionsSweep);
      broker.unsubscribe(TOPICS.GAMMA_EXPOSURE_ALERT, onGammaExposure);
      broker.unsubscribe(TOPICS.MACRO_REGIME_UPDATE, onMacroRegime);
      broker.unsubscribe(TOPICS.CANDLE_CLOSED, onCandleClosed);
      broker.unsubscribe(TOPICS.INDICATORS_UPDATE, onIndicatorsUpdate);
      broker.unsubscribe("ALPHA_SIGNAL", onAlphaSignal);
      broker.unsubscribe(TOPICS.STRATEGY_SIGNAL, onStrategySignal);
      broker.unsubscribe(TOPICS.RISK_STATE_UPDATE, onRiskState);
    });
  });

  // API Routes
  // Removed greedy `/api/*` circuit breaker middleware.
  // It erroneously blocked local DB API calls (indicators, settings).
  // The Axios interceptor (`binanceAxios`) already handles Binance rate limits appropriately.

  app.get("/api/ticker", async (req, res) => {
    try {
      const cacheKey = "ticker";
      const cached = getCachedData(cacheKey, CACHE_TTL.ticker);
      if (cached) return res.json(cached);

      const response = await binanceAxios.get(
        `${binanceBaseUrl}/api/v3/ticker/24hr`,
      );
      const popularPairs = [
        "BTCUSDT",
        "ETHUSDT",
        "BNBUSDT",
        "SOLUSDT",
        "ADAUSDT",
        "XRPUSDT",
        "DOTUSDT",
        "DOGEUSDT",
        "MATICUSDT",
        "AVAXUSDT",
      ];
      const filteredData = response.data.filter((item: any) =>
        popularPairs.includes(item.symbol),
      );

      setCachedData(cacheKey, filteredData);
      res.json(filteredData);
    } catch (error) {
      console.error("Error fetching ticker data:", error);
      res.status(500).json({ error: "Failed to fetch ticker data" });
    }
  });

  app.get("/api/klines/:symbol", async (req, res) => {
    const { symbol } = req.params;
    const { interval = "1h", limit = "24" } = req.query;
    try {
      const cacheKey = `klines_${symbol}_${interval}_${limit}`;
      const cached = getCachedData(cacheKey, CACHE_TTL.klines);
      if (cached) return res.json(cached);

      const response = await binanceAxios.get(
        `${binanceBaseUrl}/api/v3/klines`,
        {
          params: { symbol, interval, limit },
        },
      );

      setCachedData(cacheKey, response.data);
      res.json(response.data);
    } catch (error) {
      console.error(`Error fetching klines for ${symbol}:`, error);
      res.status(500).json({ error: "Failed to fetch kline data" });
    }
  });

  app.get("/api/orderbook/:symbol", async (req, res) => {
    const { symbol } = req.params;
    connectWebSocket(symbol); // Ensure WS is running for this symbol
    try {
      const cacheKey = `depth_${symbol}`;
      const cached = getCachedData(cacheKey, CACHE_TTL.depth);
      if (cached) return res.json(cached);

      const response = await binanceAxios.get(
        `${binanceBaseUrl}/api/v3/depth`,
        {
          params: { symbol, limit: 10 },
        },
      );

      setCachedData(cacheKey, response.data);
      res.json(response.data);
    } catch (error) {
      console.error(`Error fetching orderbook for ${symbol}:`, error);
      res.status(500).json({ error: "Failed to fetch orderbook data" });
    }
  });

  app.get("/api/trades/:symbol", async (req, res) => {
    const { symbol } = req.params;
    connectWebSocket(symbol); // Ensure WS is running for this symbol
    try {
      const cacheKey = `trades_${symbol}`;
      const cached = getCachedData(cacheKey, CACHE_TTL.trades);
      if (cached) return res.json(cached);

      const response = await binanceAxios.get(
        `${binanceBaseUrl}/api/v3/trades`,
        {
          params: { symbol, limit: 10 },
        },
      );

      setCachedData(cacheKey, response.data);
      res.json(response.data);
    } catch (error) {
      console.error(`Error fetching trades for ${symbol}:`, error);
      res.status(500).json({ error: "Failed to fetch trades data" });
    }
  });

  app.get("/api/exchangeInfo/:symbol", async (req, res) => {
    try {
      const symbol = req.params.symbol;
      const data = await getInternalExchangeInfo();
      const symbolInfo = data.symbols?.find((s: any) => s.symbol === symbol);
      if (symbolInfo) {
        res.json(symbolInfo);
      } else {
        res.status(404).json({ error: "Symbol not found" });
      }
    } catch (error) {
      console.error(
        `Error fetching exchange info for ${req.params.symbol}:`,
        error,
      );
      res
        .status(500)
        .json({ error: "Failed to fetch exchange info for symbol" });
    }
  });

  app.get("/api/exchangeInfo", async (req, res) => {
    try {
      const data = await getInternalExchangeInfo();
      res.json(data);
    } catch (error) {
      console.error("Error fetching exchange info:", error);
      res.status(500).json({ error: "Failed to fetch exchange info" });
    }
  });


  app.get("/api/deposits", async (req, res) => {
    if (!activeApiKey || !activeSecretKey)
      return res.status(401).json({ error: "API keys not configured" });
    try {
      const timestamp = Date.now();
      const queryString = `timestamp=${timestamp}`;
      const signature = crypto
        .createHmac("sha256", activeSecretKey)
        .update(queryString)
        .digest("hex");
      const response = await binanceAxios.get(
        `${binanceBaseUrl}/sapi/v1/capital/deposit/hisrec?${queryString}&signature=${signature}`,
        {
          headers: { "X-MBX-APIKEY": activeApiKey },
        },
      );
      res.json(response.data);
    } catch (e: any) {
      console.error("Fetch deposits error", e.response?.data || e.message);
      res.json([]);
    }
  });

  app.get("/api/withdrawals", async (req, res) => {
    if (!activeApiKey || !activeSecretKey)
      return res.status(401).json({ error: "API keys not configured" });
    try {
      const timestamp = Date.now();
      const queryString = `timestamp=${timestamp}`;
      const signature = crypto
        .createHmac("sha256", activeSecretKey)
        .update(queryString)
        .digest("hex");
      const response = await binanceAxios.get(
        `${binanceBaseUrl}/sapi/v1/capital/withdraw/history?${queryString}&signature=${signature}`,
        {
          headers: { "X-MBX-APIKEY": activeApiKey },
        },
      );
      res.json(response.data);
    } catch (e: any) {
      console.error("Fetch withdrawals error", e.response?.data || e.message);
      res.json([]);
    }
  });

  app.get("/api/account", async (req, res) => {
    if (!activeApiKey || !activeSecretKey) {
      return res.status(401).json({ error: "API keys not configured" });
    }
    try {
      const signedQuery = signRequest({ timestamp: Date.now() });
      const response = await binanceAxios.get(
        `${binanceBaseUrl}/api/v3/account?${signedQuery}`,
        {
          headers: { "X-MBX-APIKEY": activeApiKey },
        },
      );
      res.json(response.data);
    } catch (error: any) {
      let errorMessage = "Failed to fetch account details";
      if (error.response?.data?.msg) {
        errorMessage = `Binance Error: ${error.response.data.msg}`;
      } else if (error.message) {
        errorMessage = error.message;
      }
      console.error(
        "Error fetching account details:",
        error.response?.data || error.message,
      );
      res.status(error.response?.status || 500).json({ error: errorMessage });
    }
  });

  app.get("/api/wallet-summary", async (req, res) => {
    if (!activeApiKey || !activeSecretKey) {
      return res.status(401).json({ error: "API keys not configured" });
    }
    try {
      // Get current tickers for pricing
      const tickerCache = getCachedData("ticker", CACHE_TTL.ticker);
      let tickers = tickerCache;
      if (!tickers) {
        const response = await binanceAxios.get(
          `${binanceBaseUrl}/api/v3/ticker/price`,
        );
        tickers = response.data;
      }

      const portfolio: any[] = [];
      let totalUsdtValue = 0;

      for (const [asset, balance] of Object.entries(accountBalances)) {
        const qty = parseFloat(balance.free) + parseFloat(balance.locked);
        if (qty <= 0) continue;

        let price = 1;
        if (asset !== "USDT") {
          const pair = Array.isArray(tickers)
            ? tickers.find((t: any) => t.symbol === `${asset}USDT`)
            : null;
          price = pair ? parseFloat(pair.price || pair.lastPrice) : 0;
        }

        const value = qty * price;
        if (value > 0.1) {
          // Only show assets worth more than 10 cents
          portfolio.push({ asset, qty, price, value });
          totalUsdtValue += value;
        }
      }

      res.json({
        totalValue: totalUsdtValue,
        assets: portfolio.sort((a, b) => b.value - a.value),
      });
    } catch (error) {
      console.error("Error calculating portfolio:", error);
      res.status(500).json({ error: "Failed to calculate portfolio" });
    }
  });

  app.get("/api/openOrders/:symbol", async (req, res) => {
    const { symbol } = req.params;
    if (!activeApiKey || !activeSecretKey) {
      return res.status(401).json({ error: "API keys not configured" });
    }
    try {
      const signedQuery = signRequest({ symbol, timestamp: Date.now() });
      const response = await binanceAxios.get(
        `${binanceBaseUrl}/api/v3/openOrders?${signedQuery}`,
        {
          headers: { "X-MBX-APIKEY": activeApiKey },
        },
      );
      res.json(response.data);
    } catch (error: any) {
      console.error(
        `Error fetching open orders for ${symbol}:`,
        error.response?.data || error.message,
      );
      res.status(500).json({ error: "Failed to fetch open orders" });
    }
  });

  app.get("/api/internal-trades/:symbol", (req, res) => {
    const { symbol } = req.params;
    try {
      // Get the last 1000 executed trades (live + shadow) for this symbol from DB
      const result = db
        .prepare(
          "SELECT * FROM trades WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1000",
        )
        .all(symbol);
      res.json(result);
    } catch (error) {
      console.error(`Error fetching internal trades for ${symbol}:`, error);
      res.status(500).json({ error: "Failed to fetch internal trade history" });
    }
  });

  app.get("/api/myTrades/:symbol", async (req, res) => {
    const { symbol } = req.params;
    if (!activeApiKey || !activeSecretKey) {
      return res.status(401).json({ error: "API keys not configured" });
    }
    try {
      const signedQuery = signRequest({ symbol, timestamp: Date.now() });
      const response = await binanceAxios.get(
        `${binanceBaseUrl}/api/v3/myTrades?${signedQuery}`,
        {
          headers: { "X-MBX-APIKEY": activeApiKey },
        },
      );
      res.json(response.data);
    } catch (error: any) {
      console.error(
        `Error fetching trades for ${symbol}:`,
        error.response?.data || error.message,
      );
      res.status(500).json({ error: "Failed to fetch trade history" });
    }
  });

  app.delete("/api/order/:symbol/:orderId", async (req, res) => {
    const { symbol, orderId } = req.params;
    if (!activeApiKey || !activeSecretKey) {
      return res.status(401).json({ error: "API keys not configured" });
    }
    try {
      const signedQuery = signRequest({
        symbol,
        orderId,
        timestamp: Date.now(),
      });
      const response = await binanceAxios.delete(
        `${binanceBaseUrl}/api/v3/order?${signedQuery}`,
        {
          headers: { "X-MBX-APIKEY": activeApiKey },
        },
      );
      res.json({ success: true, data: response.data });
    } catch (error: any) {
      console.error(
        `Error cancelling order ${orderId}:`,
        error.response?.data || error.message,
      );
      res
        .status(500)
        .json({
          error: "Failed to cancel order",
          details: error.response?.data || error.message,
        });
    }
  });

  app.delete("/api/buster-call/:symbol", async (req, res) => {
    const { symbol } = req.params;
    if (!activeApiKey || !activeSecretKey) {
      return res.status(401).json({ error: "API keys not configured" });
    }
    try {
      if (symbol === "GLOBAL") {
        console.log("[BUSTER CALL] Global Halt Triggered!");

        // 1. Immediately revoke API memory variables
        const killApiKey = activeApiKey;
        const killSecretKey = activeSecretKey;
        activeApiKey = "";
        activeSecretKey = "";

        // 2. Clear Python engine to silence WS & trading logic
        if (quantEngine) {
          console.log("[BUSTER CALL] Issuing SIGKILL to Python Engine.");
          try {
            quantEngine.kill("SIGKILL");
          } catch (e) {}
        }

        // Let UI know we are stopping
        res.json({
          success: true,
          message:
            "Buster Call initiated: Python engine killed, memory wiped, open orders evaporating.",
        });

        // 3. Concurrently fetch all open orders and send DELETE to every active symbol asynchronously
        (async () => {
          try {
            const qFetch = signRequest(
              { timestamp: Date.now() },
              killSecretKey,
            );
            const openOrdersRes = await binanceAxios.get(
              `${binanceBaseUrl}/api/v3/openOrders?${qFetch}`,
              {
                headers: { "X-MBX-APIKEY": killApiKey },
              },
            );

            const symbols = [
              ...new Set<string>(openOrdersRes.data.map((o: any) => o.symbol)),
            ];
            console.log(
              `[BUSTER CALL] Found open orders on symbols: ${symbols.join(", ")}`,
            );

            await Promise.allSettled(
              symbols.map((sym) => {
                const qCancel = signRequest(
                  { symbol: sym, timestamp: Date.now() },
                  killSecretKey,
                );
                return binanceAxios
                  .delete(`${binanceBaseUrl}/api/v3/openOrders?${qCancel}`, {
                    headers: { "X-MBX-APIKEY": killApiKey },
                  })
                  .catch((e) =>
                    console.error(
                      `[BUSTER CALL] Failed to cancel ${sym}:`,
                      e.message,
                    ),
                  );
              }),
            );
            console.log("[BUSTER CALL] All remote orders eradicated.");
            // 4. Liquidate all balances
            console.log("[BUSTER CALL] Liquidating all spot active assets...");
            const accountFetch = signRequest({ timestamp: Date.now() }, killSecretKey);
            const accountRes = await binanceAxios.get(`${binanceBaseUrl}/api/v3/account?${accountFetch}`, { headers: { "X-MBX-APIKEY": killApiKey } });
            
            const exchangeInfo = getCachedData("exchangeInfo", 86400000) as any;
            
            await Promise.allSettled(
              accountRes.data.balances.map(async (bal: any) => {
                const free = parseFloat(bal.free);
                if (free > 0 && bal.asset !== "USDT" && bal.asset !== "USDC" && bal.asset !== "BNB" && bal.asset !== "FDUSD") {
                   const sym = `${bal.asset}USDT`;
                   const symInfo = exchangeInfo?.symbols?.find((s: any) => s.symbol === sym);
                   if (symInfo) {
                     const lotFilter = symInfo.filters.find((f: any) => f.filterType === 'LOT_SIZE');
                     const stepSize = parseFloat(lotFilter?.stepSize || "0.00001");
                     const finalQty = (Math.floor(free / stepSize) * stepSize).toFixed(8);
                     
                     if (parseFloat(finalQty) > 0) {
                        console.log(`[BUSTER CALL] Liquidating ${finalQty} of ${sym}...`);
                        const qOrder = signRequest({
                           symbol: sym,
                           side: "SELL",
                           type: "MARKET",
                           quantity: finalQty,
                           timestamp: Date.now()
                        }, killSecretKey);
                        await binanceAxios.post(`${binanceBaseUrl}/api/v3/order?${qOrder}`, null, { headers: { "X-MBX-APIKEY": killApiKey } }).catch((e) => console.error(`[BUSTER CALL] Liq Fail ${sym}:`, e.response?.data?.msg || e.message));
                     }
                   }
                }
              })
            );
            console.log("[BUSTER CALL] Global Liquidation completed.");
          } catch (e: any) {
            console.error(
              "[BUSTER CALL] Failed to clear open orders or liquidate:",
              e.message,
            );
          }
        })();

        return;
      }

      const signedQuery = signRequest({ symbol, timestamp: Date.now() });
      const response = await binanceAxios.delete(
        `${binanceBaseUrl}/api/v3/openOrders?${signedQuery}`,
        {
          headers: { "X-MBX-APIKEY": activeApiKey },
        },
      );

      // Liquidate the specific symbol
      console.log(`[BUSTER CALL] Liquidating spot active assets for ${symbol}...`);
      const accountFetch = signRequest({ timestamp: Date.now() });
      const accountRes = await binanceAxios.get(`${binanceBaseUrl}/api/v3/account?${accountFetch}`, { headers: { "X-MBX-APIKEY": activeApiKey } });
      const exchangeInfo = getCachedData("exchangeInfo", 86400000) as any;
      const symInfo = exchangeInfo?.symbols?.find((s: any) => s.symbol === symbol);
      if (symInfo) {
        const baseAsset = symInfo.baseAsset;
        const bal = accountRes.data.balances.find((b: any) => b.asset === baseAsset);
        const free = parseFloat(bal?.free || "0");
        if (free > 0) {
           const lotFilter = symInfo.filters.find((f: any) => f.filterType === 'LOT_SIZE');
           const stepSize = parseFloat(lotFilter?.stepSize || "0.00001");
           const finalQty = (Math.floor(free / stepSize) * stepSize).toFixed(8);
           if (parseFloat(finalQty) > 0) {
              console.log(`[BUSTER CALL] Liquidating ${finalQty} of ${symbol}...`);
              const qOrder = signRequest({
                 symbol: symbol,
                 side: "SELL",
                 type: "MARKET",
                 quantity: finalQty,
                 timestamp: Date.now()
              });
              await binanceAxios.post(`${binanceBaseUrl}/api/v3/order?${qOrder}`, null, { headers: { "X-MBX-APIKEY": activeApiKey } }).catch((e) => console.error(`[BUSTER CALL] Liq Fail ${symbol}:`, e.response?.data?.msg || e.message));
           }
        }
      }

      // Also publish a HALT signal to the broker for this symbol
      broker.publish(TOPICS.STRATEGY_SIGNAL, {
        signal_id: `buster_${Date.now()}`,
        timestamp: Date.now(),
        strategy_id: "BUSTER_CALL",
        symbol: symbol,
        action: "HALT",
        order_type: "MARKET",
        weight: 100,
      });

      sendIntegrationsNotification(
        {
          symbol: symbol,
          side: "CANCEL_ALL",
          type: "BUSTER_CALL",
          quantity: "ALL",
          market: "SPOT",
        },
        { status: "KILLED" },
      ).catch(console.error);

      res.json({ success: true, data: response.data });
    } catch (error: any) {
      console.error(
        `Error in Buster Call for ${symbol}:`,
        error.response?.data || error.message,
      );
      res
        .status(500)
        .json({
          error: "Buster Call failed",
          details: error.response?.data || error.message,
        });
    }
  });

  app.post("/api/validateOrder", async (req, res) => {
    const { symbol, price, quantity } = req.body;
    if (!symbol || price === undefined || quantity === undefined) {
      return res
        .status(400)
        .json({ error: "Missing symbol, price, or quantity" });
    }
    try {
      const result = await validateOrderConstraints(
        symbol,
        Number(price),
        Number(quantity),
      );
      res.json(result);
    } catch (error) {
      console.error("Validation error:", error);
      res.status(500).json({ error: "Validation failed" });
    }
  });

  app.post("/api/order", async (req, res) => {
    if (!activeApiKey || !activeSecretKey) {
      return res.status(401).json({ error: "API keys not configured" });
    }

    const {
      symbol,
      side,
      type,
      quantity,
      price,
      stopPrice,
      timeInForce,
      icebergQty,
    } = req.body;

    if (!symbol || !side || !type || !quantity) {
      return res
        .status(400)
        .json({ error: "Missing required order parameters" });
    }

    const numQuantity = Number(quantity);
    if (isNaN(numQuantity) || numQuantity <= 0) {
      return res.status(400).json({ error: "Invalid quantity" });
    }

    let numPrice = 0;
    if (type.includes("LIMIT") || type === "TWAP") {
      if (!price)
        return res
          .status(400)
          .json({ error: `Price is required for ${type} orders` });
      numPrice = Number(price);
      if (isNaN(numPrice) || numPrice <= 0) {
        return res.status(400).json({ error: "Invalid price" });
      }
    }

    try {
      // 1. Validate order constraints first
      const validation = await validateOrderConstraints(
        symbol,
        numPrice,
        numQuantity,
      );
      if (!validation.valid) {
        return res
          .status(400)
          .json({
            error: "Order validation failed",
            details: validation.reason,
          });
      }

      // 2. Publish as a Strategy Signal (Routes through Risk Manager)
      const signalId = `manual_${Date.now()}`;
      broker.publish(TOPICS.STRATEGY_SIGNAL, {
        signal_id: signalId,
        timestamp: Date.now(),
        strategy_id: "MANUAL_TRADE",
        symbol: symbol,
        action: side,
        order_type: type,
        price: type.includes("LIMIT") ? numPrice : undefined,
        stopPrice: stopPrice ? Number(stopPrice) : undefined,
        timeInForce: timeInForce,
        icebergQty: icebergQty ? Number(icebergQty) : undefined,
        weight: 100, // Manual trades use 100% of the requested quantity
        metadata: {
          ...req.body.metadata,
          market: req.body.market || "SPOT",
          requested_quantity: numQuantity,
          stopPrice: stopPrice ? Number(stopPrice) : undefined,
          timeInForce: timeInForce,
          icebergQty: icebergQty ? Number(icebergQty) : undefined,
        },
      });

      res.json({
        success: true,
        order: { orderId: signalId, status: "ROUTED" },
      });
    } catch (error: any) {
      console.error("Order routing error:", error.message);
      res.status(500).json({
        error: "Failed to route order",
        details: error.message,
      });
    }
  });

  app.get("/api/journal", (req, res) => {
    try {
      const { limit = 50, type } = req.query;
      let query = "SELECT * FROM system_logs";
      const params: any[] = [];

      if (type) {
        query += " WHERE level = ?";
        params.push(type);
      }

      query += " ORDER BY timestamp DESC LIMIT ?";
      params.push(Number(limit));

      const logs = db.prepare(query).all(...params);
      res.json(logs);
    } catch (error: any) {
      console.error("Failed to fetch journal:", error);
      res.status(500).json({ error: "Failed to fetch journal" });
    }
  });

  app.get("/api/settings", (req, res) => {
    try {
      res.json({
        hasApiKey: !!activeApiKey,
        hasSecretKey: !!activeSecretKey,
        apiKey: activeApiKey
          ? `${activeApiKey.substring(0, 4)}...${activeApiKey.substring(activeApiKey.length - 4)}`
          : null,
        useTestnet,
        telegramBotToken: getSetting("TELEGRAM_BOT_TOKEN") || "",
        telegramChatId: getSetting("TELEGRAM_CHAT_ID") || "",
        webhookUrl: getSetting("WEBHOOK_URL") || "",
      });
    } catch (error: any) {
      console.error("Failed to fetch settings:", error);
      res.status(500).json({ error: "Failed to fetch settings" });
    }
  });

  app.post("/api/settings", express.json(), (req, res) => {
    try {
      const {
        apiKey,
        secretKey,
        useTestnet,
        telegramBotToken,
        telegramChatId,
        webhookUrl,
      } = req.body;

      let keysChanged = false;
      if (apiKey !== undefined) {
        setSetting("BINANCE_API_KEY", apiKey, true);
        keysChanged = true;
      }
      if (secretKey !== undefined) {
        setSetting("BINANCE_SECRET_KEY", secretKey, true);
        keysChanged = true;
      }
      if (useTestnet !== undefined) {
        setSetting("BINANCE_USE_TESTNET", useTestnet ? "true" : "false", false);
        keysChanged = true;
      }
      if (telegramBotToken !== undefined)
        setSetting("TELEGRAM_BOT_TOKEN", telegramBotToken, false);
      if (telegramChatId !== undefined)
        setSetting("TELEGRAM_CHAT_ID", telegramChatId, false);
      if (webhookUrl !== undefined)
        setSetting("WEBHOOK_URL", webhookUrl, false);

      if (keysChanged) {
        loadKeysFromDB();
        restartQuantEngine(); // Restart to pick up testnet/keys differences completely

        if (activeApiKey && activeSecretKey) {
          closeUserDataStream();
          fetchInitialBalances()
            .then((success) => {
              if (success) startUserDataStream();
            })
            .catch((e) =>
              console.error("Failed to re-initialize data streams:", e),
            );
        }
      }

      res.json({ success: true, message: "Settings saved successfully" });
    } catch (error: any) {
      console.error("Failed to save settings:", error);
      res.status(500).json({ error: "Failed to save settings" });
    }
  });

  app.get("/api/settings/indicators", (req, res) => {
    try {
      const configStr = getSetting("INDICATOR_CONFIG");
      let indicators: any[] = [];
      if (configStr) {
        indicators = JSON.parse(configStr);
      } else {
        // Default
        indicators = [
          {
            id: "supertrend1",
            type: "SUPERTREND",
            length: 10,
            multiplier: 3.0,
            enabled: true,
          },
          {
            id: "rsi1",
            type: "RSI",
            length: 14,
            source: "close",
            enabled: true,
          },
          { id: "ema_7", type: "EMA", length: 7, enabled: true },
          { id: "ema_9", type: "EMA", length: 9, enabled: true },
          { id: "ema_21", type: "EMA", length: 21, enabled: true },
          { id: "ema_25", type: "EMA", length: 25, enabled: true },
          { id: "cci20", type: "CCI", length: 20, enabled: true },
          { id: "vwap1", type: "VWAP", enabled: true },
          { id: "smc1", type: "SMC", show_ob: true, enabled: true },
        ];
        setSetting("INDICATOR_CONFIG", JSON.stringify(indicators));
      }
      res.json(indicators);
    } catch (error: any) {
      console.error("Failed to fetch indicator settings:", error);
      res.status(500).json({ error: "Failed to fetch indicator settings" });
    }
  });

  app.post("/api/settings/indicators", express.json(), (req, res) => {
    try {
      const indicators = req.body;
      if (!Array.isArray(indicators)) {
        return res
          .status(400)
          .json({ error: "Expected an array of indicator configs" });
      }

      setSetting("INDICATOR_CONFIG", JSON.stringify(indicators));

      broker.publish(TOPICS.CONFIG_UPDATE, { indicators });
      sendToQuantEngine("CONFIG_UPDATE", { indicators }); // Update Python Engine directly

      res.json({
        success: true,
        message: "Indicator settings saved successfully",
      });
    } catch (error: any) {
      console.error("Failed to save indicator settings:", error);
      res.status(500).json({ error: "Failed to save indicator settings" });
    }
  });

  // --- TRADING API ---
  app.get("/api/settings/execution_config", (req, res) => {
    try {
      const configStr = getSetting("EXECUTION_CONFIG");
      let config = configStr ? JSON.parse(configStr) : [];
      res.json({ rules: config });
    } catch (error: any) {
      console.error("Failed to fetch execution config:", error);
      res.status(500).json({ error: "Failed to fetch execution config" });
    }
  });

  app.post("/api/settings/execution_config", express.json(), (req, res) => {
    try {
      const { rules } = req.body;
      if (!Array.isArray(rules)) {
        return res
          .status(400)
          .json({ error: "Expected an array of execution rules" });
      }
      setSetting("EXECUTION_CONFIG", JSON.stringify(rules));
      sendToQuantEngine("EXECUTION_RULES_UPDATE", rules);
      res.json({
        success: true,
        message: "Execution configurations saved successfully",
      });
    } catch (error: any) {
      console.error("Failed to save execution config:", error);
      res.status(500).json({ error: "Failed to save execution config" });
    }
  });

  app.get("/api/settings/mode", (req, res) => {
    try {
      const modeStr = getSetting("STRATEGY_MODE") || '["SCALP"]';
      let mode;
      try {
        mode = JSON.parse(modeStr);
      } catch (e) {
        mode = modeStr;
      }
      res.json({ mode });
    } catch (e) {
      res.status(500).json({ error: "Failed to get mode" });
    }
  });

  app.post("/api/settings/mode", express.json(), (req, res) => {
    try {
      const { mode } = req.body;
      if (mode) {
        setSetting(
          "STRATEGY_MODE",
          typeof mode === "string" ? mode : JSON.stringify(mode),
        );
        sendToQuantEngine("CONFIG_UPDATE", { mode });
      }
      res.json({ success: true, mode });
    } catch (e) {
      res.status(500).json({ error: "Failed to save mode" });
    }
  });

  isAutopilotOn = getSetting("AUTOPILOT_ENABLED") === "true";
  autopilotConvictionThreshold = parseInt(
    getSetting("AUTOPILOT_THRESHOLD") || "75",
    10,
  );

  const discoverAssets = async () => {
    if (!isAutopilotOn) return;
    try {
      const response = await binanceAxios.get(
        `${binanceBaseUrl}/api/v3/ticker/24hr`,
      );
      const tickers = response.data.filter((t: any) =>
        t.symbol.endsWith("USDT"),
      );
      // Sort by quoteVolume to get most liquid
      tickers.sort(
        (a: any, b: any) =>
          parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume),
      );

      const topPairs = tickers.slice(0, 15).map((t: any) => t.symbol);
      console.log(
        `[AUTOPILOT] Auto-discovered top pairs: ${topPairs.join(", ")}`,
      );

      autopilotTrackedAssets.clear();
      // Boot streams for these pairs
      for (let sym of topPairs) {
        autopilotTrackedAssets.add(sym);
        connectWebSocket(sym);
      }
    } catch (e) {
      console.error("Failed to fetch top pairs for autopilot:", e);
    }
  };

  // Start initial autopilot if enabled on boot
  if (isAutopilotOn) {
    setTimeout(() => {
      const currentRisk = parseFloat(getSetting("AUTOPILOT_MAX_RISK") || "2.0");
      const currentHeat = parseFloat(
        getSetting("AUTOPILOT_MAX_HEAT") || "20.0",
      );
      const currentOrderType = getSetting("AUTOPILOT_ORDER_TYPE") || "MARKET";

      sendToQuantEngine("AUTOPILOT_START", {
        convictionThreshold: autopilotConvictionThreshold,
        maxRisk: currentRisk,
        maxHeat: currentHeat,
        orderType: currentOrderType,
      });

      discoverAssets();
      autopilotDiscoveryInterval = setInterval(discoverAssets, 30 * 60 * 1000);
    }, 5000);
  }

  app.get("/api/settings/autopilot/tracked-assets", (req, res) => {
    res.json({ trackedAssets: Array.from(activeDepthWs.keys()) });
  });

  app.get("/api/settings/autopilot", (req, res) => {
    res.json({
      isAutopilotOn,
      convictionThreshold: autopilotConvictionThreshold,
      maxRisk: parseFloat(getSetting("AUTOPILOT_MAX_RISK") || "2.0"),
      maxHeat: parseFloat(getSetting("AUTOPILOT_MAX_HEAT") || "20.0"),
      orderType: getSetting("AUTOPILOT_ORDER_TYPE") || "MARKET",
    });
  });

  app.get("/api/settings/risk-overrides", (req, res) => {
    try {
      const overrides = JSON.parse(getSetting("RISK_OVERRIDES") || "{}");
      res.json({ overrides });
    } catch (e) {
      res.json({ overrides: {} });
    }
  });

  app.post("/api/settings/risk-overrides", express.json(), (req, res) => {
    try {
      const { overrides } = req.body;
      setSetting("RISK_OVERRIDES", JSON.stringify(overrides));
      sendToQuantEngine("RISK_OVERRIDES_UPDATE", overrides);
      res.json({ success: true });
    } catch (e) {
      res.status(500).json({ error: "Failed to save overrides" });
    }
  });

  app.post("/api/settings/autopilot", express.json(), async (req, res) => {
    try {
      const { enabled, convictionThreshold, maxRisk, maxHeat, orderType } =
        req.body;
      if (convictionThreshold !== undefined) {
        autopilotConvictionThreshold = convictionThreshold;
        setSetting(
          "AUTOPILOT_THRESHOLD",
          autopilotConvictionThreshold.toString(),
        );
      }
      if (maxRisk !== undefined)
        setSetting("AUTOPILOT_MAX_RISK", maxRisk.toString());
      if (maxHeat !== undefined)
        setSetting("AUTOPILOT_MAX_HEAT", maxHeat.toString());
      if (orderType !== undefined)
        setSetting("AUTOPILOT_ORDER_TYPE", orderType);

      if (enabled !== undefined) {
        isAutopilotOn = enabled;
        setSetting("AUTOPILOT_ENABLED", isAutopilotOn ? "true" : "false");
      }

      if (isAutopilotOn) {
        const currentRisk = parseFloat(
          getSetting("AUTOPILOT_MAX_RISK") || "2.0",
        );
        const currentHeat = parseFloat(
          getSetting("AUTOPILOT_MAX_HEAT") || "20.0",
        );
        const currentOrderType = getSetting("AUTOPILOT_ORDER_TYPE") || "MARKET";

        sendToQuantEngine("AUTOPILOT_START", {
          convictionThreshold: autopilotConvictionThreshold,
          maxRisk: currentRisk,
          maxHeat: currentHeat,
          orderType: currentOrderType,
        });
        discoverAssets();
        if (!autopilotDiscoveryInterval) {
          autopilotDiscoveryInterval = setInterval(
            discoverAssets,
            30 * 60 * 1000,
          ); // 30 minutes
        }
      } else {
        autopilotTrackedAssets.clear();
        sendToQuantEngine("AUTOPILOT_STOP", {});
        if (autopilotDiscoveryInterval) {
          clearInterval(autopilotDiscoveryInterval);
          autopilotDiscoveryInterval = null;
        }
      }

      res.json({
        success: true,
        isAutopilotOn,
        convictionThreshold: autopilotConvictionThreshold,
      });
    } catch (error) {
      res.status(500).json({ error: "Failed to toggle autopilot" });
    }
  });

  app.post("/api/reconnect", (req, res) => {
    try {
      console.log("🔄 Reconnection requested by user UI...");

      // Clear cache to ensure fresh exchange info for the new environment/account
      clearCache();

      loadKeysFromDB();

      // Close user data stream if active
      closeUserDataStream();

      // Restart Python quant engine so it reads the new keys from DB
      spawnEngine();

      // Restart user data stream with new keys
      if (activeApiKey && activeSecretKey) {
        fetchInitialBalances()
          .then((success) => {
            if (success) {
              console.log(
                "✅ Initial balances fetched, starting user data stream...",
              );
              return startUserDataStream();
            } else {
              console.warn("⚠️ Skipping user data stream due to authentication failure.");
            }
          })
          .catch((err) => {
            console.error(
              "❌ Failed to initialize user data stream after reconnect:",
              err.message,
            );
          });
      }

      res.json({
        success: true,
        message: "Reconnected successfully with new keys.",
      });
    } catch (error: any) {
      console.error("Failed to reconnect:", error);
      res.status(500).json({ error: "Failed to reconnect with new keys." });
    }
  });

  app.use("/api", (err: any, req: any, res: any, next: any) => {
    console.error("API Error:", err);
    res
      .status(500)
      .json({ error: "Internal Server Error", message: err.message });
  });

  // Vite middleware for development
  if (
    process.env.NODE_ENV !== "production" &&
    process.env.NODE_ENV !== "test" &&
    !process.env.VITEST
  ) {
    const vite = await createViteServer({
      server: { middlewareMode: true, hmr: false },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else if (process.env.NODE_ENV === "production") {
    const distPath = path.join(process.cwd(), "dist");
    app.use(express.static(distPath));
    app.get("*", (req, res) => {
      res.sendFile(path.join(distPath, "index.html"));
    });
  }

  const server = app.listen(listenPort, "0.0.0.0", () => {
    console.log(`Server running on http://localhost:${listenPort}`);

    // Start background processes AFTER server is listening to ensure UI availability
    console.log("[System] Initializing background services...");
    startQuantEngine();
    getInternalExchangeInfo().catch(console.error);
    startOptionsStream();
    if (activeApiKey && activeSecretKey) {
      // Phase 1: Heartbeat Disconnect Safety - Cancel all open orders on startup
      console.log(
        "[System] Boot Sequence: Checking for orphaned open orders...",
      );
      const signature = crypto
        .createHmac("sha256", activeSecretKey)
        .update(`timestamp=${Date.now()}`)
        .digest("hex");
      binanceAxios
        .get(
          `${binanceBaseUrl}/api/v3/openOrders?timestamp=${Date.now()}&signature=${signature}`,
          {
            headers: { "X-MBX-APIKEY": activeApiKey },
          },
        )
        .then((res) => {
          if (res.data && res.data.length > 0) {
            console.log(
              `[Emergency Cancel] Found ${res.data.length} orphaned orders on startup. Cancelling...`,
            );
            // Can't cancel without symbol, group by symbol
            const symbols = [
              ...new Set<string>(res.data.map((o: any) => o.symbol)),
            ];
            symbols.forEach((sym) => {
              const ts = Date.now();
              const q = `symbol=${sym}&timestamp=${ts}`;
              const sig = crypto
                .createHmac("sha256", activeSecretKey)
                .update(q)
                .digest("hex");
              binanceAxios
                .delete(
                  `${binanceBaseUrl}/api/v3/openOrders?${q}&signature=${sig}`,
                  {
                    headers: { "X-MBX-APIKEY": activeApiKey },
                  },
                )
                .catch((e) =>
                  console.error(`Failed canceling ${sym}:`, e.message),
                );
            });
          } else {
            console.log("[System] No orphaned orders found.");
          }
        })
        .catch((e) => {
          if (
            e.response &&
            (e.response.status === 401 || e.response.status === 403)
          ) {
            console.log(
              "[System] Invalid or missing API Key. Skipping open orders check.",
            );
          } else {
            console.error("Failed to check open orders wrapper:", e.message);
          }
        });

      fetchInitialBalances().then((success) => {
        if (success) startUserDataStream();
      }).catch(console.error);
    }
  });

  // Export startServer functionality for restarting or testing
  app.post("/api/restart-engine", (req, res) => {
    restartQuantEngine();
    res.json({ success: true, message: "Quant engine restart triggered." });
  });

  server.on("error", (e: NodeJS.ErrnoException) => {
    if (e.code === "EADDRINUSE") {
      console.error(`Port ${listenPort} is in use, retrying...`);
      setTimeout(() => {
        server.close();
        server.listen(listenPort, "0.0.0.0");
      }, 1000);
    } else {
      console.error(e);
    }
  });

  // Dedicated UI WebSocket Server for Strategy Signals
  const wss = new WebSocketServer({ server, path: "/ws/signals" });
  wss.on("connection", (ws) => {
    console.log("[UI WebSocket] Client connected to strategy signals");

    // Hydration: immediately send ACTIVE_SMC_CACHE
    ws.send(
      JSON.stringify({ event: "ACTIVE_SMC_CACHE", data: activeSMCCache }),
    );

    let isDiagnosticsEnabled = false;

    const onSignal = (msg: any) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ event: "LIVE_DOM_SIGNAL", data: msg }));
      }
    };

    const onDepth = (msg: any) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ event: "depth", data: msg }));
      }
    };

    const onIndicators = (msg: any) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ event: "INDICATORS_UPDATE", data: msg }));
      }
    };

    const onSMCUpdate = (msg: any) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ event: "SMC_UPDATE", data: msg }));
      }
    };

    const onAlphaSignal = (msg: any) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ event: "ALPHA_SIGNAL", data: msg }));
      }
    };

    const onExecutionReport = (msg: any) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ event: "EXECUTION_REPORT", data: msg }));
      }
    };

    const onExecutionError = (msg: any) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ event: "EXECUTION_ERROR", data: msg }));
      }
    };

    const onSystemInfo = (msg: any) => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ event: "SYSTEM_INFO_MESSAGE", data: msg }));
      }
    };

    ws.on("message", (data) => {
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === "START_DIAGNOSTICS") {
          if (!isDiagnosticsEnabled) {
            console.log("[UI WebSocket] Diagnostics ENABLED for client");
            broker.subscribe(TOPICS.MARKET_DATA_DEPTH, onDepth);
            isDiagnosticsEnabled = true;
          }
        } else if (msg.type === "STOP_DIAGNOSTICS") {
          if (isDiagnosticsEnabled) {
            console.log("[UI WebSocket] Diagnostics DISABLED for client");
            broker.unsubscribe(TOPICS.MARKET_DATA_DEPTH, onDepth);
            isDiagnosticsEnabled = false;
          }
        }
      } catch (err) {}
    });

    broker.subscribe(TOPICS.STRATEGY_SIGNAL, onSignal);
    broker.subscribe(TOPICS.INDICATORS_UPDATE, onIndicators);
    broker.subscribe(TOPICS.SMC_UPDATE, onSMCUpdate);
    broker.subscribe("ALPHA_SIGNAL", onAlphaSignal);
    broker.subscribe(TOPICS.EXECUTION_REPORT, onExecutionReport);
    broker.subscribe(TOPICS.EXECUTION_ERROR, onExecutionError);
    broker.subscribe(TOPICS.SYSTEM_INFO_MESSAGE, onSystemInfo);

    ws.on("close", () => {
      broker.unsubscribe(TOPICS.STRATEGY_SIGNAL, onSignal);
      broker.unsubscribe(TOPICS.INDICATORS_UPDATE, onIndicators);
      broker.unsubscribe(TOPICS.SMC_UPDATE, onSMCUpdate);
      broker.unsubscribe("ALPHA_SIGNAL", onAlphaSignal);
      broker.unsubscribe(TOPICS.EXECUTION_REPORT, onExecutionReport);
      broker.unsubscribe(TOPICS.EXECUTION_ERROR, onExecutionError);
      broker.unsubscribe(TOPICS.SYSTEM_INFO_MESSAGE, onSystemInfo);
      if (isDiagnosticsEnabled) {
        broker.unsubscribe(TOPICS.MARKET_DATA_DEPTH, onDepth);
      }
    });
  });

  return { app, server };
}

// Only start the server if we are not in a test environment
if (process.env.NODE_ENV !== "test") {
  startServer().catch(console.error);
}

// Graceful shutdown to kill orphaned child processes and WebSockets
function gracefulShutdown() {
  console.log("[System] Graceful Shutdown Triggered. Cleaning up...");

  if (activeApiKey && activeSecretKey) {
    console.log(
      "[System] Heartbeat Disconnect Safety: Canceling all open orders before exit for tracked symbols...",
    );
    const symbolsToCancel = Array.from(activeDepthWs.keys()).map((s) =>
      s.toUpperCase(),
    );

    // We try to cancel synchronously/quickly by sending out promises
    const promises = symbolsToCancel.map((symbol) => {
      const timestamp = Date.now();
      const signature = crypto
        .createHmac("sha256", activeSecretKey)
        .update(`symbol=${symbol}&timestamp=${timestamp}`)
        .digest("hex");
      return binanceAxios
        .delete(
          `${binanceBaseUrl}/api/v3/openOrders?symbol=${symbol}&timestamp=${timestamp}&signature=${signature}`,
          {
            headers: { "X-MBX-APIKEY": activeApiKey },
          },
        )
        .catch(() => {}); // ignore errors during shutdown
    });

    // In a shutdown hook we shouldn't await, but we can't reliably cancel everything anyway without blocking,
    // however Node handles outgoing HTTP requests gracefully if we give it a tiny bit of time before exit.
  }

  if (quantEngine) {
    console.log("[System] Killing Quant Engine process...");
    isQuantEngineIntentionalClose = true;
    try {
      quantEngine.kill("SIGKILL");
    } catch (e) {}
  }

  activeDepthWs.forEach((session, symbol) => {
    try {
      session.ws.terminate();
    } catch (e) {}
  });
  activeTradeWs.forEach((session, symbol) => {
    try {
      session.ws.terminate();
    } catch (e) {}
  });

  try {
    pubSocket.unbindSync("tcp://127.0.0.1:5555");
    pubSocket.close();
  } catch (e) {}
  try {
    pullSocket.unbindSync("tcp://127.0.0.1:5556");
    pullSocket.close();
  } catch (e) {}

  // Give it 1 second to flush network requests before dying
  setTimeout(() => process.exit(0), 1000);
}

process.on("SIGINT", gracefulShutdown);
process.on("SIGTERM", gracefulShutdown);
process.on("SIGUSR2", gracefulShutdown); // For nodemon/PM2 restarts
process.on("uncaughtException", (err) => {
  console.error("[System] Uncaught Exception:", err);
  // Do not crash loop if it's intermittent, just log it unless it's EADDRINUSE
  if (err.message.includes("EADDRINUSE")) {
    process.exit(1);
  }
});

export { startServer };

const fs = require("fs/promises");
const fssync = require("fs");
const path = require("path");
const os = require("os");
const http = require("http");
const crypto = require("crypto");

const express = require("express");
const compression = require("compression");
const axios = require("axios");
const cron = require("node-cron");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: true,
    credentials: true,
  },
});

const PORT = Number(process.env.PORT || 3000);
const NODE_ENV = process.env.NODE_ENV || "development";
const AUTH_TOKEN = process.env.AUTH_TOKEN || "change-me-stock-token";
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);
const PRICE_REFRESH_BATCH = Number(process.env.PRICE_REFRESH_BATCH || 50);
const JSON_CACHE_TTL_MS = 60 * 1000;
const NEWS_CACHE_TTL_MS = 5 * 60 * 1000;
const ALERT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const OUTPUT_LIMIT = 10_000;

const DATA_DIR = resolveWritableDirectory([
  process.env.DATA_DIR,
  path.join(process.cwd(), "data"),
  "/tmp/stock-platform-data",
]);
const TMP_DIR = resolveWritableDirectory([
  process.env.CACHE_DIR,
  "/tmp/stock-platform-cache",
  path.join(process.cwd(), ".tmp"),
]);

const DATA_FILES = {
  stocks: "stocks.json",
  prices: "prices.json",
  portfolios: "portfolios.json",
  watchlists: "watchlists.json",
  signals: "signals.json",
  news: "news.json",
  alerts: "alerts.json",
  signalsArchive: "signals_archive.json",
};

const jsonCache = new Map();
const fileQueues = new Map();
const rateLimitStore = new Map();
const sseClients = new Set();
const quoteCache = new Map();
const historyWarmCache = new Map();

const DEFAULT_STOCKS = [
  { symbol: "AAPL", name: "Apple Inc.", sector: "Technology", exchange: "NASDAQ", marketCap: 2900000000000, beta: 1.24, pe: 30.1, dividendYield: 0.005, country: "US", currency: "USD", earningsDate: "2026-07-30" },
  { symbol: "MSFT", name: "Microsoft Corporation", sector: "Technology", exchange: "NASDAQ", marketCap: 3200000000000, beta: 0.9, pe: 34.8, dividendYield: 0.007, country: "US", currency: "USD", earningsDate: "2026-07-23" },
  { symbol: "NVDA", name: "NVIDIA Corporation", sector: "Technology", exchange: "NASDAQ", marketCap: 2700000000000, beta: 1.71, pe: 52.4, dividendYield: 0.001, country: "US", currency: "USD", earningsDate: "2026-08-21" },
  { symbol: "GOOGL", name: "Alphabet Inc.", sector: "Communication Services", exchange: "NASDAQ", marketCap: 2100000000000, beta: 1.04, pe: 26.5, dividendYield: 0.003, country: "US", currency: "USD", earningsDate: "2026-07-24" },
  { symbol: "AMZN", name: "Amazon.com, Inc.", sector: "Consumer Discretionary", exchange: "NASDAQ", marketCap: 1900000000000, beta: 1.17, pe: 43.2, dividendYield: 0, country: "US", currency: "USD", earningsDate: "2026-08-01" },
  { symbol: "META", name: "Meta Platforms, Inc.", sector: "Communication Services", exchange: "NASDAQ", marketCap: 1400000000000, beta: 1.21, pe: 29.5, dividendYield: 0.004, country: "US", currency: "USD", earningsDate: "2026-07-31" },
  { symbol: "TSLA", name: "Tesla, Inc.", sector: "Consumer Discretionary", exchange: "NASDAQ", marketCap: 620000000000, beta: 2.18, pe: 55.7, dividendYield: 0, country: "US", currency: "USD", earningsDate: "2026-07-17" },
  { symbol: "BRK-B", name: "Berkshire Hathaway Inc.", sector: "Financial Services", exchange: "NYSE", marketCap: 950000000000, beta: 0.86, pe: 14.8, dividendYield: 0, country: "US", currency: "USD", earningsDate: "2026-08-03" },
  { symbol: "LLY", name: "Eli Lilly and Company", sector: "Healthcare", exchange: "NYSE", marketCap: 740000000000, beta: 0.4, pe: 58.9, dividendYield: 0.008, country: "US", currency: "USD", earningsDate: "2026-08-06" },
  { symbol: "AVGO", name: "Broadcom Inc.", sector: "Technology", exchange: "NASDAQ", marketCap: 780000000000, beta: 1.12, pe: 31.6, dividendYield: 0.012, country: "US", currency: "USD", earningsDate: "2026-09-05" },
  { symbol: "JPM", name: "JPMorgan Chase & Co.", sector: "Financial Services", exchange: "NYSE", marketCap: 590000000000, beta: 1.08, pe: 13.1, dividendYield: 0.021, country: "US", currency: "USD", earningsDate: "2026-07-12" },
  { symbol: "V", name: "Visa Inc.", sector: "Financial Services", exchange: "NYSE", marketCap: 560000000000, beta: 0.95, pe: 29.1, dividendYield: 0.007, country: "US", currency: "USD", earningsDate: "2026-07-25" },
  { symbol: "XOM", name: "Exxon Mobil Corporation", sector: "Energy", exchange: "NYSE", marketCap: 520000000000, beta: 0.88, pe: 13.4, dividendYield: 0.031, country: "US", currency: "USD", earningsDate: "2026-08-02" },
  { symbol: "UNH", name: "UnitedHealth Group Incorporated", sector: "Healthcare", exchange: "NYSE", marketCap: 470000000000, beta: 0.62, pe: 18.3, dividendYield: 0.014, country: "US", currency: "USD", earningsDate: "2026-07-16" },
  { symbol: "COST", name: "Costco Wholesale Corporation", sector: "Consumer Defensive", exchange: "NASDAQ", marketCap: 390000000000, beta: 0.79, pe: 47.9, dividendYield: 0.006, country: "US", currency: "USD", earningsDate: "2026-09-24" },
  { symbol: "PG", name: "The Procter & Gamble Company", sector: "Consumer Defensive", exchange: "NYSE", marketCap: 370000000000, beta: 0.42, pe: 26.8, dividendYield: 0.024, country: "US", currency: "USD", earningsDate: "2026-08-01" },
  { symbol: "MA", name: "Mastercard Incorporated", sector: "Financial Services", exchange: "NYSE", marketCap: 430000000000, beta: 1.03, pe: 31.2, dividendYield: 0.005, country: "US", currency: "USD", earningsDate: "2026-07-31" },
  { symbol: "HD", name: "The Home Depot, Inc.", sector: "Consumer Discretionary", exchange: "NYSE", marketCap: 330000000000, beta: 0.98, pe: 24.5, dividendYield: 0.023, country: "US", currency: "USD", earningsDate: "2026-08-20" },
  { symbol: "JNJ", name: "Johnson & Johnson", sector: "Healthcare", exchange: "NYSE", marketCap: 380000000000, beta: 0.53, pe: 16.5, dividendYield: 0.029, country: "US", currency: "USD", earningsDate: "2026-07-15" },
  { symbol: "ABBV", name: "AbbVie Inc.", sector: "Healthcare", exchange: "NYSE", marketCap: 330000000000, beta: 0.55, pe: 18.2, dividendYield: 0.034, country: "US", currency: "USD", earningsDate: "2026-08-01" },
  { symbol: "BAC", name: "Bank of America Corporation", sector: "Financial Services", exchange: "NYSE", marketCap: 300000000000, beta: 1.34, pe: 12.2, dividendYield: 0.025, country: "US", currency: "USD", earningsDate: "2026-07-17" },
  { symbol: "KO", name: "The Coca-Cola Company", sector: "Consumer Defensive", exchange: "NYSE", marketCap: 290000000000, beta: 0.47, pe: 24.4, dividendYield: 0.029, country: "US", currency: "USD", earningsDate: "2026-07-24" },
  { symbol: "PEP", name: "PepsiCo, Inc.", sector: "Consumer Defensive", exchange: "NASDAQ", marketCap: 250000000000, beta: 0.49, pe: 21.9, dividendYield: 0.031, country: "US", currency: "USD", earningsDate: "2026-07-11" },
  { symbol: "NFLX", name: "Netflix, Inc.", sector: "Communication Services", exchange: "NASDAQ", marketCap: 300000000000, beta: 1.39, pe: 39.2, dividendYield: 0, country: "US", currency: "USD", earningsDate: "2026-07-18" },
  { symbol: "AMD", name: "Advanced Micro Devices, Inc.", sector: "Technology", exchange: "NASDAQ", marketCap: 270000000000, beta: 1.82, pe: 44.1, dividendYield: 0, country: "US", currency: "USD", earningsDate: "2026-07-30" },
  { symbol: "ORCL", name: "Oracle Corporation", sector: "Technology", exchange: "NYSE", marketCap: 380000000000, beta: 1.03, pe: 27.2, dividendYield: 0.011, country: "US", currency: "USD", earningsDate: "2026-09-09" },
  { symbol: "CRM", name: "Salesforce, Inc.", sector: "Technology", exchange: "NYSE", marketCap: 280000000000, beta: 1.21, pe: 30.4, dividendYield: 0, country: "US", currency: "USD", earningsDate: "2026-08-27" },
  { symbol: "ADBE", name: "Adobe Inc.", sector: "Technology", exchange: "NASDAQ", marketCap: 230000000000, beta: 1.33, pe: 34.3, dividendYield: 0, country: "US", currency: "USD", earningsDate: "2026-09-12" },
  { symbol: "CSCO", name: "Cisco Systems, Inc.", sector: "Technology", exchange: "NASDAQ", marketCap: 220000000000, beta: 0.92, pe: 18.1, dividendYield: 0.027, country: "US", currency: "USD", earningsDate: "2026-08-14" },
  { symbol: "INTC", name: "Intel Corporation", sector: "Technology", exchange: "NASDAQ", marketCap: 170000000000, beta: 1.02, pe: 20.8, dividendYield: 0.014, country: "US", currency: "USD", earningsDate: "2026-07-24" },
  { symbol: "QCOM", name: "QUALCOMM Incorporated", sector: "Technology", exchange: "NASDAQ", marketCap: 230000000000, beta: 1.27, pe: 19.7, dividendYield: 0.02, country: "US", currency: "USD", earningsDate: "2026-08-01" },
  { symbol: "TMO", name: "Thermo Fisher Scientific Inc.", sector: "Healthcare", exchange: "NYSE", marketCap: 220000000000, beta: 0.82, pe: 23.5, dividendYield: 0.003, country: "US", currency: "USD", earningsDate: "2026-07-24" },
  { symbol: "MRK", name: "Merck & Co., Inc.", sector: "Healthcare", exchange: "NYSE", marketCap: 260000000000, beta: 0.39, pe: 17.1, dividendYield: 0.028, country: "US", currency: "USD", earningsDate: "2026-07-30" },
  { symbol: "CVX", name: "Chevron Corporation", sector: "Energy", exchange: "NYSE", marketCap: 300000000000, beta: 0.91, pe: 14.1, dividendYield: 0.039, country: "US", currency: "USD", earningsDate: "2026-08-02" },
  { symbol: "WMT", name: "Walmart Inc.", sector: "Consumer Defensive", exchange: "NYSE", marketCap: 540000000000, beta: 0.56, pe: 28.9, dividendYield: 0.011, country: "US", currency: "USD", earningsDate: "2026-08-15" },
  { symbol: "MCD", name: "McDonald's Corporation", sector: "Consumer Cyclical", exchange: "NYSE", marketCap: 230000000000, beta: 0.58, pe: 24.1, dividendYield: 0.022, country: "US", currency: "USD", earningsDate: "2026-07-29" },
  { symbol: "DIS", name: "The Walt Disney Company", sector: "Communication Services", exchange: "NYSE", marketCap: 210000000000, beta: 1.32, pe: 25.6, dividendYield: 0.006, country: "US", currency: "USD", earningsDate: "2026-08-07" },
  { symbol: "IBM", name: "International Business Machines Corporation", sector: "Technology", exchange: "NYSE", marketCap: 190000000000, beta: 0.76, pe: 21.2, dividendYield: 0.031, country: "US", currency: "USD", earningsDate: "2026-07-24" },
  { symbol: "GE", name: "GE Aerospace", sector: "Industrials", exchange: "NYSE", marketCap: 210000000000, beta: 1.18, pe: 29.7, dividendYield: 0.004, country: "US", currency: "USD", earningsDate: "2026-07-25" },
  { symbol: "CAT", name: "Caterpillar Inc.", sector: "Industrials", exchange: "NYSE", marketCap: 180000000000, beta: 1.17, pe: 18.5, dividendYield: 0.016, country: "US", currency: "USD", earningsDate: "2026-08-01" },
  { symbol: "BA", name: "The Boeing Company", sector: "Industrials", exchange: "NYSE", marketCap: 160000000000, beta: 1.54, pe: 0, dividendYield: 0, country: "US", currency: "USD", earningsDate: "2026-07-31" },
  { symbol: "NKE", name: "NIKE, Inc.", sector: "Consumer Cyclical", exchange: "NYSE", marketCap: 150000000000, beta: 1.11, pe: 24.9, dividendYield: 0.014, country: "US", currency: "USD", earningsDate: "2026-09-26" },
  { symbol: "SHOP", name: "Shopify Inc.", sector: "Technology", exchange: "NYSE", marketCap: 110000000000, beta: 2.04, pe: 0, dividendYield: 0, country: "CA", currency: "USD", earningsDate: "2026-08-08" },
  { symbol: "UBER", name: "Uber Technologies, Inc.", sector: "Technology", exchange: "NYSE", marketCap: 160000000000, beta: 1.36, pe: 0, dividendYield: 0, country: "US", currency: "USD", earningsDate: "2026-08-06" },
  { symbol: "PYPL", name: "PayPal Holdings, Inc.", sector: "Financial Services", exchange: "NASDAQ", marketCap: 95000000000, beta: 1.52, pe: 18.3, dividendYield: 0, country: "US", currency: "USD", earningsDate: "2026-08-01" },
  { symbol: "SQ", name: "Block, Inc.", sector: "Technology", exchange: "NYSE", marketCap: 52000000000, beta: 2.48, pe: 0, dividendYield: 0, country: "US", currency: "USD", earningsDate: "2026-08-01" },
  { symbol: "PLTR", name: "Palantir Technologies Inc.", sector: "Technology", exchange: "NASDAQ", marketCap: 170000000000, beta: 2.06, pe: 0, dividendYield: 0, country: "US", currency: "USD", earningsDate: "2026-08-05" },
  { symbol: "SOFI", name: "SoFi Technologies, Inc.", sector: "Financial Services", exchange: "NASDAQ", marketCap: 15000000000, beta: 1.78, pe: 0, dividendYield: 0, country: "US", currency: "USD", earningsDate: "2026-07-31" },
  { symbol: "SNOW", name: "Snowflake Inc.", sector: "Technology", exchange: "NYSE", marketCap: 65000000000, beta: 0.98, pe: 0, dividendYield: 0, country: "US", currency: "USD", earningsDate: "2026-08-21" },
  { symbol: "PANW", name: "Palo Alto Networks, Inc.", sector: "Technology", exchange: "NASDAQ", marketCap: 120000000000, beta: 1.12, pe: 41.4, dividendYield: 0, country: "US", currency: "USD", earningsDate: "2026-08-20" },
  { symbol: "VRTX", name: "Vertex Pharmaceuticals Incorporated", sector: "Healthcare", exchange: "NASDAQ", marketCap: 140000000000, beta: 0.42, pe: 27.9, dividendYield: 0, country: "US", currency: "USD", earningsDate: "2026-08-01" }
];

const DEFAULT_DATA = {
  [DATA_FILES.stocks]: DEFAULT_STOCKS,
  [DATA_FILES.prices]: {},
  [DATA_FILES.portfolios]: {
    demo_user: {
      cash: 50000,
      holdings: [],
      transactions: [],
      createdAt: Date.now(),
      updatedAt: Date.now(),
    },
  },
  [DATA_FILES.watchlists]: {
    demo_user: {
      items: ["AAPL", "MSFT", "NVDA", "GOOGL"],
      alerts: [],
      updatedAt: Date.now(),
    },
  },
  [DATA_FILES.signals]: {},
  [DATA_FILES.news]: {},
  [DATA_FILES.alerts]: [],
  [DATA_FILES.signalsArchive]: [],
};

// Resolve a writable storage location so the app can run locally and on Render.
function resolveWritableDirectory(candidates) {
  for (const candidate of candidates) {
    if (!candidate) continue;
    try {
      const absolute = path.resolve(candidate);
      fssync.mkdirSync(absolute, { recursive: true });
      fssync.accessSync(absolute, fssync.constants.W_OK);
      return absolute;
    } catch (error) {
      continue;
    }
  }

  throw new Error("Unable to resolve a writable storage directory.");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function now() {
  return Date.now();
}

function safeNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function sanitizeSymbol(value) {
  const normalized = String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9.\-]/g, "");
  if (!normalized || normalized.length > 12) {
    throw new Error("Invalid symbol.");
  }
  return normalized;
}

function sanitizeUserId(value) {
  const normalized = String(value || "")
    .trim()
    .replace(/[^a-zA-Z0-9_\-]/g, "");
  if (!normalized || normalized.length > 64) {
    throw new Error("Invalid userId.");
  }
  return normalized;
}

function sanitizeText(value, maxLength = 280) {
  return String(value || "")
    .replace(/[<>`]/g, "")
    .trim()
    .slice(0, maxLength);
}

function formatOutput(text) {
  return sanitizeText(text, OUTPUT_LIMIT);
}

function hashId(input) {
  return crypto.createHash("sha1").update(String(input)).digest("hex");
}

async function ensureDataFiles() {
  await fs.mkdir(DATA_DIR, { recursive: true });
  await fs.mkdir(TMP_DIR, { recursive: true });

  for (const [filename, defaultValue] of Object.entries(DEFAULT_DATA)) {
    const filePath = path.join(DATA_DIR, filename);
    try {
      await fs.access(filePath);
    } catch (error) {
      await writeJSON(filename, defaultValue);
    }
  }
}

// File locks and per-file queues keep concurrent JSON writes from stepping on each other.
async function withFileLock(filename, task) {
  const filePath = path.join(DATA_DIR, filename);
  const pending = fileQueues.get(filePath) || Promise.resolve();
  let releaseQueue;
  const current = new Promise((resolve) => {
    releaseQueue = resolve;
  });
  fileQueues.set(filePath, pending.then(() => current));

  await pending;
  const lockPath = `${filePath}.lock`;
  const startedAt = Date.now();
  let handle = null;

  try {
    while (!handle) {
      try {
        handle = await fs.open(lockPath, "wx");
      } catch (error) {
        if (error.code !== "EEXIST") {
          throw error;
        }

        if (Date.now() - startedAt > 5000) {
          throw new Error(`Timed out waiting for file lock: ${filename}`);
        }

        await sleep(40);
      }
    }

    return await task(filePath);
  } finally {
    releaseQueue();
    if (fileQueues.get(filePath) === current) {
      fileQueues.delete(filePath);
    }

    if (handle) {
      await handle.close().catch(() => {});
      await fs.unlink(lockPath).catch(() => {});
    }
  }
}

// JSON reads are memory-cached briefly to keep filesystem work light on free-tier instances.
async function readJSON(filename, options = {}) {
  const filePath = path.join(DATA_DIR, filename);
  const cacheKey = filePath;
  const cached = jsonCache.get(cacheKey);
  const skipCache = options.force || false;

  if (!skipCache && cached && Date.now() - cached.loadedAt < JSON_CACHE_TTL_MS) {
    return structuredClone(cached.value);
  }

  try {
    const raw = await fs.readFile(filePath, "utf8");
    const parsed = raw.trim() ? JSON.parse(raw) : {};
    jsonCache.set(cacheKey, { value: parsed, loadedAt: Date.now() });
    return structuredClone(parsed);
  } catch (error) {
    if (error.code === "ENOENT") {
      const fallback = DEFAULT_DATA[filename] ?? {};
      await writeJSON(filename, fallback);
      return structuredClone(fallback);
    }

    throw error;
  }
}

async function writeJSON(filename, data) {
  const filePath = path.join(DATA_DIR, filename);
  await withFileLock(filename, async () => {
    await fs.mkdir(path.dirname(filePath), { recursive: true });
    const tempPath = path.join(
      path.dirname(filePath),
      `.${path.basename(filename)}.${Date.now()}.${Math.random().toString(16).slice(2)}.tmp`
    );
    await fs.writeFile(tempPath, JSON.stringify(data, null, 2), "utf8");
    await fs.rename(tempPath, filePath);
    jsonCache.set(filePath, { value: data, loadedAt: Date.now() });
  });
  return data;
}

async function appendToJSON(filename, newEntry) {
  const filePath = path.join(DATA_DIR, filename);
  let updated;
  await withFileLock(filename, async () => {
    let current = DEFAULT_DATA[filename] ?? [];
    try {
      const raw = await fs.readFile(filePath, "utf8");
      current = raw.trim() ? JSON.parse(raw) : [];
    } catch (error) {
      if (error.code !== "ENOENT") {
        throw error;
      }
    }

    if (!Array.isArray(current)) {
      throw new Error(`${filename} is not appendable because the root is not an array.`);
    }

    current.push(newEntry);
    const tempPath = path.join(
      path.dirname(filePath),
      `.${path.basename(filename)}.${Date.now()}.${Math.random().toString(16).slice(2)}.tmp`
    );
    await fs.writeFile(tempPath, JSON.stringify(current, null, 2), "utf8");
    await fs.rename(tempPath, filePath);
    jsonCache.set(filePath, { value: current, loadedAt: Date.now() });
    updated = current;
  });
  return updated;
}

async function updateJSON(filename, updater) {
  const filePath = path.join(DATA_DIR, filename);
  let updated;
  await withFileLock(filename, async () => {
    let current = DEFAULT_DATA[filename] ?? {};
    try {
      const raw = await fs.readFile(filePath, "utf8");
      current = raw.trim() ? JSON.parse(raw) : {};
    } catch (error) {
      if (error.code !== "ENOENT") {
        throw error;
      }
    }

    updated = await updater(structuredClone(current));
    const tempPath = path.join(
      path.dirname(filePath),
      `.${path.basename(filename)}.${Date.now()}.${Math.random().toString(16).slice(2)}.tmp`
    );
    await fs.writeFile(tempPath, JSON.stringify(updated, null, 2), "utf8");
    await fs.rename(tempPath, filePath);
    jsonCache.set(filePath, { value: updated, loadedAt: Date.now() });
  });
  return updated;
}

function corsMiddleware(req, res, next) {
  const origin = req.headers.origin;
  const allowAny = ALLOWED_ORIGINS.length === 0;
  const allowed =
    allowAny || !origin || ALLOWED_ORIGINS.includes(origin) || ALLOWED_ORIGINS.includes("*");

  if (allowed) {
    res.setHeader("Access-Control-Allow-Origin", allowAny ? origin || "*" : origin || ALLOWED_ORIGINS[0]);
    res.setHeader("Vary", "Origin");
    res.setHeader("Access-Control-Allow-Headers", "Authorization, Content-Type");
    res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  }

  if (req.method === "OPTIONS") {
    return res.status(204).end();
  }

  return next();
}

function authMiddleware(req, res, next) {
  const publicPaths = new Set([
    "/",
    "/index.html",
    "/paper-trading.html",
    "/favicon.ico",
    "/health",
  ]);

  if (publicPaths.has(req.path) || req.path.startsWith("/assets/")) {
    return next();
  }

  const header = req.headers.authorization || "";
  const queryToken = req.query.token;
  const token = header.startsWith("Bearer ") ? header.slice("Bearer ".length).trim() : String(queryToken || "");

  if (!token || token !== AUTH_TOKEN) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  return next();
}

function rateLimitMiddleware(req, res, next) {
  const identifier =
    (req.headers["x-forwarded-for"] || "")
      .toString()
      .split(",")[0]
      .trim() || req.socket.remoteAddress || "unknown";
  const minuteBucket = Math.floor(Date.now() / 60000);
  const key = `${identifier}:${minuteBucket}`;
  const entry = rateLimitStore.get(key) || { count: 0 };
  entry.count += 1;
  rateLimitStore.set(key, entry);

  if (entry.count > 100) {
    return res.status(429).json({ error: "Rate limit exceeded" });
  }

  return next();
}

function errorBoundary(handler) {
  return async (req, res) => {
    try {
      await handler(req, res);
    } catch (error) {
      const status = error.statusCode || 500;
      res.status(status).json({
        error: status === 500 ? "Internal server error" : error.message,
        detail: NODE_ENV === "production" ? undefined : error.stack,
      });
    }
  };
}

function broadcast(event, payload) {
  io.emit(event, payload);
  for (const client of sseClients) {
    client.write(`event: ${event}\n`);
    client.write(`data: ${JSON.stringify(payload)}\n\n`);
  }
}

function getRelativeTime(timestamp) {
  const diff = Math.max(0, Date.now() - timestamp);
  const minute = 60 * 1000;
  const hour = 60 * minute;
  const day = 24 * hour;

  if (diff < minute) return "just now";
  if (diff < hour) return `${Math.round(diff / minute)}m ago`;
  if (diff < day) return `${Math.round(diff / hour)}h ago`;
  return `${Math.round(diff / day)}d ago`;
}

function rotateSeries(series, timeframe) {
  const retention = {
    "1m": 30 * 24 * 60 * 60 * 1000,
    "1h": 90 * 24 * 60 * 60 * 1000,
    "1d": 2 * 365 * 24 * 60 * 60 * 1000,
  }[timeframe] || 365 * 24 * 60 * 60 * 1000;

  const cutoff = Date.now() - retention;
  return series
    .filter((point) => safeNumber(point.t) >= cutoff)
    .sort((a, b) => a.t - b.t);
}

function movingAverage(values, period) {
  const result = [];
  for (let index = 0; index < values.length; index += 1) {
    if (index + 1 < period) {
      result.push(null);
      continue;
    }
    const slice = values.slice(index + 1 - period, index + 1);
    result.push(slice.reduce((sum, value) => sum + value, 0) / period);
  }
  return result;
}

function exponentialMovingAverage(values, period) {
  const multiplier = 2 / (period + 1);
  const ema = [];
  let previous = values[0] || 0;
  for (const value of values) {
    previous = (value - previous) * multiplier + previous;
    ema.push(previous);
  }
  return ema;
}

function calculateRSI(closes, period = 14) {
  if (closes.length <= period) return null;
  let gains = 0;
  let losses = 0;

  for (let index = 1; index <= period; index += 1) {
    const delta = closes[index] - closes[index - 1];
    if (delta >= 0) gains += delta;
    else losses += Math.abs(delta);
  }

  let avgGain = gains / period;
  let avgLoss = losses / period;

  for (let index = period + 1; index < closes.length; index += 1) {
    const delta = closes[index] - closes[index - 1];
    const gain = Math.max(delta, 0);
    const loss = Math.max(-delta, 0);
    avgGain = (avgGain * (period - 1) + gain) / period;
    avgLoss = (avgLoss * (period - 1) + loss) / period;
  }

  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return Number((100 - 100 / (1 + rs)).toFixed(2));
}

function calculateMACD(closes) {
  if (closes.length < 35) return null;
  const ema12 = exponentialMovingAverage(closes, 12);
  const ema26 = exponentialMovingAverage(closes, 26);
  const macdLine = ema12.map((value, index) => value - ema26[index]);
  const signalLine = exponentialMovingAverage(macdLine, 9);
  const histogram = macdLine[macdLine.length - 1] - signalLine[signalLine.length - 1];

  return {
    macd: Number(macdLine[macdLine.length - 1].toFixed(4)),
    signal: Number(signalLine[signalLine.length - 1].toFixed(4)),
    histogram: Number(histogram.toFixed(4)),
  };
}

function calculateBollingerBands(closes, period = 20, multiplier = 2) {
  if (closes.length < period) return null;
  const slice = closes.slice(-period);
  const middle = slice.reduce((sum, value) => sum + value, 0) / period;
  const variance =
    slice.reduce((sum, value) => sum + Math.pow(value - middle, 2), 0) / period;
  const deviation = Math.sqrt(variance);

  return {
    upper: Number((middle + multiplier * deviation).toFixed(2)),
    middle: Number(middle.toFixed(2)),
    lower: Number((middle - multiplier * deviation).toFixed(2)),
  };
}

function calculateDailyReturns(closes) {
  const returns = [];
  for (let index = 1; index < closes.length; index += 1) {
    const previous = closes[index - 1];
    const current = closes[index];
    if (previous > 0) {
      returns.push((current - previous) / previous);
    }
  }
  return returns;
}

function calculateSharpeRatio(closes) {
  const returns = calculateDailyReturns(closes);
  if (returns.length < 5) return null;
  const riskFreeDaily = 0.02 / 252;
  const avg =
    returns.reduce((sum, value) => sum + (value - riskFreeDaily), 0) / returns.length;
  const variance =
    returns.reduce((sum, value) => sum + Math.pow(value - riskFreeDaily - avg, 2), 0) /
    returns.length;
  const deviation = Math.sqrt(variance);
  if (!deviation) return null;
  return Number(((avg / deviation) * Math.sqrt(252)).toFixed(2));
}

function calculateSignalStrength({ rsi, macd, price, bands, recentChange }) {
  let strength = 50;
  const reasons = [];
  let type = "HOLD";

  if (rsi !== null && rsi < 35) {
    strength += 20;
    reasons.push("RSI oversold");
    type = "BUY";
  } else if (rsi !== null && rsi > 70) {
    strength += 20;
    reasons.push("RSI overbought");
    type = "SELL";
  }

  if (macd && macd.histogram > 0) {
    strength += 15;
    reasons.push("MACD bullish");
    if (type === "HOLD") type = "BUY";
  } else if (macd && macd.histogram < 0) {
    strength += 15;
    reasons.push("MACD bearish");
    if (type === "HOLD") type = "SELL";
  }

  if (bands) {
    if (price <= bands.lower) {
      strength += 10;
      reasons.push("Near lower Bollinger band");
      if (type === "HOLD") type = "BUY";
    } else if (price >= bands.upper) {
      strength += 10;
      reasons.push("Near upper Bollinger band");
      if (type === "HOLD") type = "SELL";
    }
  }

  if (recentChange > 0.03) {
    strength -= 5;
    reasons.push("Short-term momentum extended");
  } else if (recentChange < -0.03) {
    strength += 5;
    reasons.push("Pullback may be stabilizing");
  }

  return {
    type,
    strength: clamp(Math.round(strength), 0, 100),
    reason: reasons.length ? reasons.join(" + ") : "Balanced technical setup",
  };
}

function scoreSentiment(text) {
  const positives = ["beat", "growth", "surge", "strong", "upside", "record", "profit", "buy"];
  const negatives = ["miss", "drop", "weak", "risk", "loss", "lawsuit", "downgrade", "sell"];
  const lowered = text.toLowerCase();
  let score = 50;

  for (const word of positives) {
    if (lowered.includes(word)) score += 8;
  }
  for (const word of negatives) {
    if (lowered.includes(word)) score -= 8;
  }

  return clamp(score, 0, 100);
}

function parseRssItems(xml) {
  const items = [];
  const matches = xml.match(/<item>[\s\S]*?<\/item>/g) || [];

  for (const raw of matches.slice(0, 8)) {
    const title = decodeXml((raw.match(/<title>([\s\S]*?)<\/title>/i) || [])[1] || "");
    const link = decodeXml((raw.match(/<link>([\s\S]*?)<\/link>/i) || [])[1] || "");
    const pubDate = Date.parse((raw.match(/<pubDate>([\s\S]*?)<\/pubDate>/i) || [])[1] || "");
    const description = decodeXml(
      (raw.match(/<description>([\s\S]*?)<\/description>/i) || [])[1] || ""
    ).replace(/<[^>]+>/g, "");

    if (title && link) {
      items.push({
        title: sanitizeText(title, 160),
        link,
        publishedAt: Number.isFinite(pubDate) ? pubDate : Date.now(),
        sentiment: scoreSentiment(`${title} ${description}`),
        summary: sanitizeText(description, 320),
      });
    }
  }

  return items;
}

function decodeXml(value) {
  return value
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

async function getStocks() {
  const stocks = await readJSON(DATA_FILES.stocks);
  return Array.isArray(stocks) ? stocks : DEFAULT_STOCKS;
}

async function findStock(symbol) {
  const stocks = await getStocks();
  return stocks.find((entry) => entry.symbol === symbol) || null;
}

async function upsertPricePoint(symbol, timeframe, candle) {
  await updateJSON(DATA_FILES.prices, (prices) => {
    const symbolStore = prices[symbol] || { "1m": [], "1h": [], "1d": [], quote: null };
    const series = Array.isArray(symbolStore[timeframe]) ? symbolStore[timeframe] : [];
    const filtered = series.filter((point) => point.t !== candle.t);
    filtered.push(candle);
    symbolStore[timeframe] = rotateSeries(filtered, timeframe);
    symbolStore.quote = {
      symbol,
      price: candle.c,
      change: safeNumber(candle.c) - safeNumber(candle.o, candle.c),
      changePercent:
        safeNumber(candle.o) > 0
          ? Number((((candle.c - candle.o) / candle.o) * 100).toFixed(2))
          : 0,
      volume: safeNumber(candle.v),
      updatedAt: candle.t,
      source: "cache",
    };
    prices[symbol] = symbolStore;
    return prices;
  });
}

async function saveQuote(symbol, quote) {
  quoteCache.set(symbol, quote);
  const candle = {
    t: quote.updatedAt || Date.now(),
    o: safeNumber(quote.previousClose || quote.price),
    h: safeNumber(quote.high || quote.price),
    l: safeNumber(quote.low || quote.price),
    c: safeNumber(quote.price),
    v: safeNumber(quote.volume),
  };
  await upsertPricePoint(symbol, "1m", candle);
}

async function fetchYahooQuote(symbol) {
  const url = `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${encodeURIComponent(symbol)}`;
  const response = await axios.get(url, {
    timeout: 5000,
    headers: { "User-Agent": "stock-intelligence-platform/1.0" },
  });
  const result = response.data?.quoteResponse?.result?.[0];
  if (!result || !Number.isFinite(result.regularMarketPrice)) {
    throw new Error("Quote provider returned no data.");
  }

  return {
    symbol,
    price: Number(result.regularMarketPrice.toFixed(2)),
    change: Number((result.regularMarketChange || 0).toFixed(2)),
    changePercent: Number((result.regularMarketChangePercent || 0).toFixed(2)),
    open: safeNumber(result.regularMarketOpen, result.regularMarketPrice),
    high: safeNumber(result.regularMarketDayHigh, result.regularMarketPrice),
    low: safeNumber(result.regularMarketDayLow, result.regularMarketPrice),
    previousClose: safeNumber(result.regularMarketPreviousClose, result.regularMarketPrice),
    volume: safeNumber(result.regularMarketVolume),
    marketCap: safeNumber(result.marketCap),
    currency: result.currency || "USD",
    exchange: result.fullExchangeName || result.exchange || "",
    updatedAt: safeNumber(result.regularMarketTime) * 1000 || Date.now(),
    source: "yahoo",
  };
}

async function getCachedQuote(symbol) {
  const cacheHit = quoteCache.get(symbol);
  if (cacheHit && Date.now() - cacheHit.updatedAt < JSON_CACHE_TTL_MS) {
    return cacheHit;
  }

  const prices = await readJSON(DATA_FILES.prices);
  const stored = prices[symbol]?.quote;
  if (stored) {
    quoteCache.set(symbol, stored);
    return stored;
  }

  return null;
}

async function getLiveQuote(symbol, options = {}) {
  const stock = await findStock(symbol);
  if (!stock) {
    const error = new Error(`Unknown symbol: ${symbol}`);
    error.statusCode = 404;
    throw error;
  }

  const cached = await getCachedQuote(symbol);
  const staleMs = options.staleMs || 60 * 1000;
  if (!options.force && cached && Date.now() - cached.updatedAt < staleMs) {
    return { ...cached, meta: stock, cached: true };
  }

  try {
    const fresh = await fetchYahooQuote(symbol);
    const merged = {
      ...fresh,
      marketCap: fresh.marketCap || stock.marketCap || 0,
      beta: stock.beta || null,
      pe: stock.pe || null,
      dividendYield: stock.dividendYield || 0,
      sector: stock.sector,
      name: stock.name,
    };
    await saveQuote(symbol, merged);
    return { ...merged, meta: stock, cached: false };
  } catch (error) {
    if (cached) {
      return { ...cached, meta: stock, cached: true, stale: true };
    }
    throw error;
  }
}

async function fetchYahooHistory(symbol, days = 30, timeframe = "1d") {
  const interval = timeframe === "1m" ? "5m" : timeframe === "1h" ? "1h" : "1d";
  const range = days <= 5 ? "5d" : days <= 30 ? "1mo" : days <= 90 ? "3mo" : days <= 365 ? "1y" : "2y";
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(
    symbol
  )}?interval=${interval}&range=${range}&includePrePost=false`;
  const response = await axios.get(url, {
    timeout: 8000,
    headers: { "User-Agent": "stock-intelligence-platform/1.0" },
  });
  const result = response.data?.chart?.result?.[0];
  const timestamps = result?.timestamp || [];
  const quote = result?.indicators?.quote?.[0] || {};
  const series = [];

  for (let index = 0; index < timestamps.length; index += 1) {
    const open = quote.open?.[index];
    const high = quote.high?.[index];
    const low = quote.low?.[index];
    const close = quote.close?.[index];
    const volume = quote.volume?.[index];
    if (![open, high, low, close].every(Number.isFinite)) {
      continue;
    }
    series.push({
      t: timestamps[index] * 1000,
      o: Number(open.toFixed(2)),
      h: Number(high.toFixed(2)),
      l: Number(low.toFixed(2)),
      c: Number(close.toFixed(2)),
      v: safeNumber(volume),
    });
  }

  return rotateSeries(series, timeframe);
}

async function getHistory(symbol, days = 30, timeframe = "1d", options = {}) {
  const prices = await readJSON(DATA_FILES.prices);
  const existing = prices[symbol]?.[timeframe] || [];
  const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;
  const cachedSlice = existing.filter((point) => point.t >= cutoff);

  if (!options.force && cachedSlice.length >= Math.min(days, 5)) {
    return cachedSlice;
  }

  const warmKey = `${symbol}:${days}:${timeframe}`;
  const warm = historyWarmCache.get(warmKey);
  if (warm && Date.now() - warm.loadedAt < JSON_CACHE_TTL_MS) {
    return warm.value;
  }

  try {
    const fetched = await fetchYahooHistory(symbol, days, timeframe);
    await updateJSON(DATA_FILES.prices, (current) => {
      const symbolStore = current[symbol] || { "1m": [], "1h": [], "1d": [], quote: null };
      const existingSeries = Array.isArray(symbolStore[timeframe]) ? symbolStore[timeframe] : [];
      const merged = [...existingSeries.filter((point) => point.t < cutoff), ...fetched];
      symbolStore[timeframe] = rotateSeries(
        merged.filter(
          (point, index, array) => index === array.findIndex((entry) => entry.t === point.t)
        ),
        timeframe
      );
      current[symbol] = symbolStore;
      return current;
    });
    historyWarmCache.set(warmKey, { value: fetched, loadedAt: Date.now() });
    return fetched;
  } catch (error) {
    if (cachedSlice.length) {
      return cachedSlice;
    }
    throw error;
  }
}

async function generateSignal(symbol, options = {}) {
  const prices = await getHistory(symbol, 120, "1d", options);
  const closes = prices.map((point) => point.c);
  if (closes.length < 30) {
    return [];
  }

  const rsi = calculateRSI(closes);
  const macd = calculateMACD(closes);
  const bands = calculateBollingerBands(closes);
  const recentChange =
    closes.length >= 6 ? (closes[closes.length - 1] - closes[closes.length - 6]) / closes[closes.length - 6] : 0;
  const signalBase = calculateSignalStrength({
    rsi,
    macd,
    price: closes[closes.length - 1],
    bands,
    recentChange,
  });

  const payload = {
    type: signalBase.type,
    strength: signalBase.strength,
    price: closes[closes.length - 1],
    time: Date.now(),
    reason: signalBase.reason,
    indicators: {
      rsi,
      macd,
      bollinger: bands,
      sma20: movingAverage(closes, 20).slice(-1)[0],
      sma50: movingAverage(closes, 50).slice(-1)[0],
    },
  };

  if (!options.skipSave) {
    await updateJSON(DATA_FILES.signals, (current) => {
      const list = Array.isArray(current[symbol]) ? current[symbol] : [];
      list.unshift(payload);
      current[symbol] = list.slice(0, 50);
      return current;
    });
  }

  return [payload];
}

async function getSignals(symbol) {
  const current = await readJSON(DATA_FILES.signals);
  const existing = Array.isArray(current[symbol]) ? current[symbol] : [];
  const latest = existing[0];
  if (latest && Date.now() - latest.time < 60 * 60 * 1000) {
    return existing;
  }
  const fresh = await generateSignal(symbol);
  return [...fresh, ...existing].slice(0, 50);
}

async function getNews(symbol, options = {}) {
  const store = await readJSON(DATA_FILES.news);
  const current = store[symbol];
  if (
    current &&
    !options.force &&
    Date.now() - safeNumber(current.updatedAt) < NEWS_CACHE_TTL_MS
  ) {
    return current;
  }

  const url = `https://news.google.com/rss/search?q=${encodeURIComponent(
    `${symbol} stock`
  )}&hl=en-US&gl=US&ceid=US:en`;
  try {
    const response = await axios.get(url, {
      timeout: 6000,
      headers: { "User-Agent": "stock-intelligence-platform/1.0" },
    });
    const items = parseRssItems(response.data);
    const sentiment =
      items.length > 0
        ? Math.round(items.reduce((sum, item) => sum + item.sentiment, 0) / items.length)
        : 50;
    const payload = {
      symbol,
      updatedAt: Date.now(),
      sentiment,
      items,
    };

    await updateJSON(DATA_FILES.news, (currentNews) => {
      currentNews[symbol] = payload;
      return currentNews;
    });

    return payload;
  } catch (error) {
    if (current) {
      return { ...current, stale: true };
    }
    return {
      symbol,
      updatedAt: Date.now(),
      sentiment: 50,
      items: [],
      stale: true,
    };
  }
}

async function ensureUserPortfolio(userId) {
  const portfolios = await readJSON(DATA_FILES.portfolios);
  if (!portfolios[userId]) {
    portfolios[userId] = {
      cash: 50000,
      holdings: [],
      transactions: [],
      createdAt: Date.now(),
      updatedAt: Date.now(),
    };
    await writeJSON(DATA_FILES.portfolios, portfolios);
  }
  return portfolios[userId];
}

async function buildPortfolioSummary(userId) {
  const portfolios = await readJSON(DATA_FILES.portfolios);
  const portfolio = portfolios[userId] || (await ensureUserPortfolio(userId));
  const holdings = Array.isArray(portfolio.holdings) ? portfolio.holdings : [];
  const enriched = [];
  let holdingsValue = 0;
  let costBasis = 0;
  let weightedBeta = 0;
  let projectedDividendIncome = 0;
  const earningsCalendar = [];
  const sectorBreakdown = {};

  for (const holding of holdings) {
    const symbol = sanitizeSymbol(holding.symbol);
    const quote = await getLiveQuote(symbol).catch(() => null);
    const stock = await findStock(symbol);
    const price = quote?.price || holding.avgPrice;
    const value = price * holding.shares;
    const basis = holding.avgPrice * holding.shares;
    const pnl = value - basis;
    holdingsValue += value;
    costBasis += basis;
    weightedBeta += value * safeNumber(stock?.beta);
    projectedDividendIncome += value * safeNumber(stock?.dividendYield);
    if (stock?.sector) {
      sectorBreakdown[stock.sector] = (sectorBreakdown[stock.sector] || 0) + value;
    }
    if (stock?.earningsDate) {
      earningsCalendar.push({
        symbol,
        name: stock.name,
        earningsDate: stock.earningsDate,
      });
    }
    enriched.push({
      ...holding,
      name: stock?.name || symbol,
      currentPrice: Number(price.toFixed(2)),
      marketValue: Number(value.toFixed(2)),
      costBasis: Number(basis.toFixed(2)),
      unrealizedPnL: Number(pnl.toFixed(2)),
      unrealizedPnLPercent: basis > 0 ? Number(((pnl / basis) * 100).toFixed(2)) : 0,
      dayChangePercent: quote?.changePercent || 0,
      relativeUpdatedAt: quote?.updatedAt ? getRelativeTime(quote.updatedAt) : "unknown",
    });
  }

  const totalValue = portfolio.cash + holdingsValue;
  const beta = totalValue > 0 ? Number((weightedBeta / Math.max(holdingsValue, 1)).toFixed(2)) : 0;
  const primaryHistorySymbol = enriched[0]?.symbol;
  const sharpe =
    primaryHistorySymbol
      ? calculateSharpeRatio((await getHistory(primaryHistorySymbol, 90, "1d").catch(() => [])).map((point) => point.c))
      : null;

  return {
    userId,
    cash: Number(safeNumber(portfolio.cash).toFixed(2)),
    holdings: enriched,
    holdingsValue: Number(holdingsValue.toFixed(2)),
    totalValue: Number(totalValue.toFixed(2)),
    costBasis: Number(costBasis.toFixed(2)),
    totalUnrealizedPnL: Number((holdingsValue - costBasis).toFixed(2)),
    totalUnrealizedPnLPercent:
      costBasis > 0 ? Number((((holdingsValue - costBasis) / costBasis) * 100).toFixed(2)) : 0,
    transactionCount: Array.isArray(portfolio.transactions) ? portfolio.transactions.length : 0,
    transactions: (portfolio.transactions || []).slice(-25).reverse(),
    risk: {
      beta,
      sharpeRatio: sharpe,
    },
    dividendTracker: {
      projectedAnnualIncome: Number(projectedDividendIncome.toFixed(2)),
    },
    sectorBreakdown: Object.entries(sectorBreakdown)
      .map(([sector, value]) => ({ sector, value: Number(value.toFixed(2)) }))
      .sort((a, b) => b.value - a.value),
    earningsCalendar: earningsCalendar.sort((a, b) => a.earningsDate.localeCompare(b.earningsDate)),
  };
}

async function addPortfolioTransaction(body) {
  const userId = sanitizeUserId(body.userId || "demo_user");
  const action = sanitizeText(body.action || "buy", 16).toLowerCase();
  const symbol = body.symbol ? sanitizeSymbol(body.symbol) : null;
  const shares = safeNumber(body.shares);
  const cashAmount = safeNumber(body.cashAmount);
  const manualPrice = body.price !== undefined ? safeNumber(body.price) : null;

  return updateJSON(DATA_FILES.portfolios, async (current) => {
    const portfolio = current[userId] || {
      cash: 50000,
      holdings: [],
      transactions: [],
      createdAt: Date.now(),
      updatedAt: Date.now(),
    };
    portfolio.holdings = Array.isArray(portfolio.holdings) ? portfolio.holdings : [];
    portfolio.transactions = Array.isArray(portfolio.transactions) ? portfolio.transactions : [];

    if (action === "deposit" || action === "withdraw") {
      if (cashAmount <= 0) throw new Error("cashAmount must be greater than 0.");
      const delta = action === "deposit" ? cashAmount : -cashAmount;
      if (portfolio.cash + delta < 0) throw new Error("Insufficient cash.");
      portfolio.cash = Number((safeNumber(portfolio.cash) + delta).toFixed(2));
      portfolio.transactions.push({
        id: hashId(`${userId}:${Date.now()}:${Math.random()}`),
        type: action,
        amount: cashAmount,
        time: Date.now(),
      });
      portfolio.updatedAt = Date.now();
      current[userId] = portfolio;
      return current;
    }

    if (!symbol || shares <= 0) {
      throw new Error("symbol and shares are required.");
    }

    const liveQuote = await getLiveQuote(symbol).catch(() => null);
    const executionPrice = manualPrice && manualPrice > 0 ? manualPrice : liveQuote?.price;
    if (!executionPrice || executionPrice <= 0) {
      throw new Error("Unable to determine execution price.");
    }

    const total = shares * executionPrice;
    let holding = portfolio.holdings.find((entry) => entry.symbol === symbol);

    if (action === "buy") {
      if (portfolio.cash < total) {
        throw new Error("Insufficient cash.");
      }
      if (!holding) {
        holding = {
          symbol,
          shares: 0,
          avgPrice: 0,
          buyDate: Date.now(),
        };
        portfolio.holdings.push(holding);
      }
      const newShares = holding.shares + shares;
      holding.avgPrice = Number(
        ((holding.avgPrice * holding.shares + executionPrice * shares) / newShares).toFixed(2)
      );
      holding.shares = Number(newShares.toFixed(4));
      holding.buyDate = holding.buyDate || Date.now();
      portfolio.cash = Number((portfolio.cash - total).toFixed(2));
    } else if (action === "sell") {
      if (!holding || holding.shares < shares) {
        throw new Error("Not enough shares to sell.");
      }
      holding.shares = Number((holding.shares - shares).toFixed(4));
      portfolio.cash = Number((portfolio.cash + total).toFixed(2));
      if (holding.shares <= 0) {
        portfolio.holdings = portfolio.holdings.filter((entry) => entry.symbol !== symbol);
      }
    } else {
      throw new Error("Unsupported action.");
    }

    portfolio.transactions.push({
      id: hashId(`${userId}:${symbol}:${Date.now()}:${Math.random()}`),
      type: action,
      symbol,
      shares,
      price: Number(executionPrice.toFixed(2)),
      total: Number(total.toFixed(2)),
      time: Date.now(),
    });

    portfolio.updatedAt = Date.now();
    current[userId] = portfolio;
    return current;
  });
}

async function addToWatchlist(body) {
  const userId = sanitizeUserId(body.userId || "demo_user");
  const symbol = sanitizeSymbol(body.symbol);
  const alertPrice = body.alertPrice !== undefined ? safeNumber(body.alertPrice) : null;
  const note = sanitizeText(body.note || "", 160);

  const updated = await updateJSON(DATA_FILES.watchlists, (current) => {
    const watchlist = current[userId] || { items: [], alerts: [], updatedAt: Date.now() };
    watchlist.items = Array.isArray(watchlist.items) ? watchlist.items : [];
    watchlist.alerts = Array.isArray(watchlist.alerts) ? watchlist.alerts : [];
    if (!watchlist.items.includes(symbol)) {
      watchlist.items.push(symbol);
    }
    if (alertPrice && alertPrice > 0) {
      watchlist.alerts.push({
        id: hashId(`${userId}:${symbol}:${alertPrice}:${Date.now()}`),
        symbol,
        targetPrice: alertPrice,
        note,
        createdAt: Date.now(),
      });
    }
    watchlist.updatedAt = Date.now();
    current[userId] = watchlist;
    return current;
  });

  return updated[userId];
}

async function createAlert(body) {
  const userId = sanitizeUserId(body.userId || "demo_user");
  const symbol = sanitizeSymbol(body.symbol);
  const condition = sanitizeText(body.condition || "above", 16).toLowerCase();
  const targetPrice = safeNumber(body.targetPrice);
  const webhookUrl = sanitizeText(body.webhookUrl || "", 300);
  if (!["above", "below"].includes(condition)) {
    throw new Error("condition must be above or below.");
  }
  if (targetPrice <= 0) {
    throw new Error("targetPrice must be greater than 0.");
  }

  const alert = {
    id: hashId(`${userId}:${symbol}:${condition}:${targetPrice}:${Date.now()}`),
    userId,
    symbol,
    condition,
    targetPrice,
    webhookUrl,
    createdAt: Date.now(),
    status: "pending",
    lastCheckedAt: null,
    triggeredAt: null,
  };

  await appendToJSON(DATA_FILES.alerts, alert);
  return alert;
}

async function listAlerts(userId) {
  const alerts = await readJSON(DATA_FILES.alerts);
  return alerts
    .filter((alert) => !userId || alert.userId === userId)
    .sort((a, b) => b.createdAt - a.createdAt);
}

// Aggregate quote snapshots into overview widgets like movers and sector heatmap.
async function buildMarketOverview() {
  const stocks = await getStocks();
  const selected = stocks.slice(0, 24);
  const quotes = await Promise.all(
    selected.map((stock) =>
      getLiveQuote(stock.symbol).catch(() => ({
        symbol: stock.symbol,
        price: 0,
        changePercent: 0,
        updatedAt: 0,
        meta: stock,
        stale: true,
      }))
    )
  );

  const sectorMap = {};
  for (const quote of quotes) {
    const sector = quote.meta?.sector || "Other";
    if (!sectorMap[sector]) {
      sectorMap[sector] = { sector, changePercentTotal: 0, count: 0, marketCap: 0 };
    }
    sectorMap[sector].changePercentTotal += safeNumber(quote.changePercent);
    sectorMap[sector].marketCap += safeNumber(quote.meta?.marketCap);
    sectorMap[sector].count += 1;
  }

  return {
    updatedAt: Date.now(),
    topMovers: quotes
      .map((quote) => ({
        symbol: quote.symbol,
        name: quote.meta?.name || quote.symbol,
        price: quote.price,
        changePercent: quote.changePercent,
        sector: quote.meta?.sector || "Other",
        marketCap: safeNumber(quote.meta?.marketCap),
      }))
      .sort((a, b) => b.changePercent - a.changePercent)
      .slice(0, 10),
    sectorHeatmap: Object.values(sectorMap)
      .map((entry) => ({
        sector: entry.sector,
        changePercent: Number((entry.changePercentTotal / Math.max(entry.count, 1)).toFixed(2)),
        marketCap: entry.marketCap,
      }))
      .sort((a, b) => b.marketCap - a.marketCap),
  };
}

async function runAlertChecks() {
  const alerts = await readJSON(DATA_FILES.alerts);
  let changed = false;
  const nextAlerts = [];
  const notifications = [];

  for (const alert of alerts) {
    if (alert.status !== "pending" && Date.now() - safeNumber(alert.createdAt) > ALERT_RETENTION_MS) {
      changed = true;
      continue;
    }

    const quote = await getLiveQuote(alert.symbol).catch(() => null);
    const price = quote?.price;
    alert.lastCheckedAt = Date.now();
    if (price) {
      const matched =
        (alert.condition === "above" && price >= alert.targetPrice) ||
        (alert.condition === "below" && price <= alert.targetPrice);
      if (matched && alert.status === "pending") {
        alert.status = "triggered";
        alert.triggeredAt = Date.now();
        notifications.push({
          id: alert.id,
          symbol: alert.symbol,
          price,
          condition: alert.condition,
          targetPrice: alert.targetPrice,
          userId: alert.userId,
          relativeTriggeredAt: getRelativeTime(alert.triggeredAt),
        });
        if (alert.webhookUrl) {
          axios
            .post(
              alert.webhookUrl,
              {
                type: "price_alert",
                alertId: alert.id,
                symbol: alert.symbol,
                price,
                condition: alert.condition,
                targetPrice: alert.targetPrice,
                triggeredAt: alert.triggeredAt,
              },
              { timeout: 5000 }
            )
            .catch(() => {});
        }
        changed = true;
      }
    }
    nextAlerts.push(alert);
  }

  if (changed) {
    await writeJSON(DATA_FILES.alerts, nextAlerts);
  }

  if (notifications.length) {
    broadcast("alerts:triggered", { alerts: notifications });
  }

  return notifications;
}

// Refresh the most-watched symbols in the background to keep the UI snappy.
async function refreshTopStocks() {
  const stocks = await getStocks();
  const selected = stocks.slice(0, PRICE_REFRESH_BATCH);
  const results = [];

  for (const stock of selected) {
    const quote = await getLiveQuote(stock.symbol, { force: true, staleMs: 0 }).catch(() => null);
    if (quote) {
      results.push({
        symbol: stock.symbol,
        price: quote.price,
        changePercent: quote.changePercent,
        updatedAt: quote.updatedAt,
      });
    }
    await sleep(80);
  }

  if (results.length) {
    broadcast("prices:tick", {
      updatedAt: Date.now(),
      quotes: results,
    });
  }
}

async function refreshSignals() {
  const stocks = await getStocks();
  const selected = stocks.slice(0, 25);
  const updates = [];

  for (const stock of selected) {
    const signal = await generateSignal(stock.symbol).catch(() => null);
    if (signal?.[0]) {
      updates.push({ symbol: stock.symbol, latest: signal[0] });
    }
    await sleep(50);
  }

  if (updates.length) {
    broadcast("signals:updated", {
      updatedAt: Date.now(),
      signals: updates,
    });
  }
}

async function refreshNews() {
  const stocks = await getStocks();
  const selected = stocks.slice(0, 20);
  const updated = [];
  for (const stock of selected) {
    const news = await getNews(stock.symbol, { force: true }).catch(() => null);
    if (news) {
      updated.push({
        symbol: stock.symbol,
        sentiment: news.sentiment,
        updatedAt: news.updatedAt,
      });
    }
    await sleep(40);
  }

  if (updated.length) {
    broadcast("news:updated", { updatedAt: Date.now(), items: updated });
  }
}

async function archiveSignalsIfNeeded() {
  const day = new Date().getUTCDay();
  const hour = new Date().getUTCHours();
  if (day !== 0 || hour !== 0) {
    return;
  }

  const current = await readJSON(DATA_FILES.signals);
  const archivePayload = {
    archivedAt: Date.now(),
    signals: current,
  };
  const archive = await appendToJSON(DATA_FILES.signalsArchive, archivePayload);
  if (archive) {
    await writeJSON(DATA_FILES.signals, {});
  }
}

// Hourly cleanup trims caches and rotates stored history to stay within free-tier limits.
async function memoryCleanup() {
  const cutoff = Date.now() - 5 * JSON_CACHE_TTL_MS;

  for (const [key, value] of jsonCache.entries()) {
    if (Date.now() - value.loadedAt > cutoff) {
      jsonCache.delete(key);
    }
  }

  for (const [key, value] of quoteCache.entries()) {
    if (Date.now() - value.updatedAt > 15 * 60 * 1000) {
      quoteCache.delete(key);
    }
  }

  for (const [key, value] of historyWarmCache.entries()) {
    if (Date.now() - value.loadedAt > 15 * 60 * 1000) {
      historyWarmCache.delete(key);
    }
  }

  for (const key of rateLimitStore.keys()) {
    const bucket = Number(key.split(":").pop());
    if (Math.floor(Date.now() / 60000) - bucket > 2) {
      rateLimitStore.delete(key);
    }
  }

  const prices = await readJSON(DATA_FILES.prices);
  let dirty = false;
  for (const symbol of Object.keys(prices)) {
    for (const timeframe of ["1m", "1h", "1d"]) {
      const original = prices[symbol]?.[timeframe] || [];
      const rotated = rotateSeries(original, timeframe);
      if (rotated.length !== original.length) {
        prices[symbol][timeframe] = rotated;
        dirty = true;
      }
    }
  }
  if (dirty) {
    await writeJSON(DATA_FILES.prices, prices);
  }

  await runAlertChecks();
  await archiveSignalsIfNeeded();
}

function buildRamPayload() {
  const usage = process.memoryUsage();
  return {
    rssMb: Number((usage.rss / 1024 / 1024).toFixed(2)),
    heapUsedMb: Number((usage.heapUsed / 1024 / 1024).toFixed(2)),
    heapTotalMb: Number((usage.heapTotal / 1024 / 1024).toFixed(2)),
    externalMb: Number((usage.external / 1024 / 1024).toFixed(2)),
    uptimeSeconds: Math.round(process.uptime()),
    platform: os.platform(),
  };
}

app.use(compression());
app.use(express.json({ limit: "1mb" }));
app.use(express.urlencoded({ extended: false, limit: "1mb" }));
app.use(corsMiddleware);
app.use(rateLimitMiddleware);
app.use(authMiddleware);
app.use(express.static(path.join(process.cwd(), "public")));

app.get("/", (req, res) => {
  res.sendFile(path.join(process.cwd(), "public", "index.html"));
});

app.get("/paper-trading.html", (req, res) => {
  res.sendFile(path.join(process.cwd(), "public", "paper-trading.html"));
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    env: NODE_ENV,
    dataDir: DATA_DIR,
    cacheDir: TMP_DIR,
    time: Date.now(),
  });
});

app.get("/favicon.ico", (req, res) => {
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
      <rect width="64" height="64" rx="10" fill="#06120b"/>
      <rect x="8" y="10" width="48" height="36" rx="4" fill="#0a2215" stroke="#2aff8a" stroke-width="3"/>
      <path d="M17 28h12M17 36h18M17 20h20" stroke="#2aff8a" stroke-width="4" stroke-linecap="round"/>
      <path d="M24 52h16" stroke="#2aff8a" stroke-width="4" stroke-linecap="round"/>
    </svg>
  `;
  res.type("image/svg+xml").send(svg);
});

app.get(
  "/events",
  errorBoundary(async (req, res) => {
    const token = String(req.query.token || "");
    if (token !== AUTH_TOKEN) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders();
    res.write(`event: ready\ndata: ${JSON.stringify({ time: Date.now() })}\n\n`);
    sseClients.add(res);

    req.on("close", () => {
      sseClients.delete(res);
    });
  })
);

app.get(
  "/api/bootstrap",
  errorBoundary(async (req, res) => {
    const userId = sanitizeUserId(req.query.userId || "demo_user");
    const [overview, watchlists, portfolio, alerts] = await Promise.all([
      buildMarketOverview(),
      readJSON(DATA_FILES.watchlists),
      buildPortfolioSummary(userId),
      listAlerts(userId),
    ]);

    res.json({
      overview,
      watchlist: watchlists[userId] || { items: [], alerts: [] },
      portfolio,
      alerts: alerts.slice(0, 10),
      ram: buildRamPayload(),
      authMode: "bearer-or-query-token-for-sse",
    });
  })
);

app.get(
  "/api/quote/:symbol",
  errorBoundary(async (req, res) => {
    const symbol = sanitizeSymbol(req.params.symbol);
    const quote = await getLiveQuote(symbol, { force: req.query.force === "true" });
    res.json(quote);
  })
);

app.get(
  "/api/history/:symbol",
  errorBoundary(async (req, res) => {
    const symbol = sanitizeSymbol(req.params.symbol);
    const days = clamp(Number(req.query.days || 30), 1, 730);
    const timeframe = ["1m", "1h", "1d"].includes(req.query.timeframe) ? req.query.timeframe : "1d";
    const history = await getHistory(symbol, days, timeframe);
    res.json({
      symbol,
      timeframe,
      points: history,
    });
  })
);

app.post(
  "/api/portfolio/add",
  errorBoundary(async (req, res) => {
    const userId = sanitizeUserId(req.body.userId || "demo_user");
    await addPortfolioTransaction(req.body);
    const summary = await buildPortfolioSummary(userId);
    broadcast("portfolio:updated", { userId, summary });
    res.json(summary);
  })
);

app.get(
  "/api/portfolio/:userId",
  errorBoundary(async (req, res) => {
    const userId = sanitizeUserId(req.params.userId);
    const summary = await buildPortfolioSummary(userId);
    res.json(summary);
  })
);

app.post(
  "/api/watchlist/add",
  errorBoundary(async (req, res) => {
    const watchlist = await addToWatchlist(req.body);
    broadcast("watchlist:updated", { userId: req.body.userId || "demo_user", watchlist });
    res.json(watchlist);
  })
);

app.get(
  "/api/watchlist/:userId",
  errorBoundary(async (req, res) => {
    const userId = sanitizeUserId(req.params.userId);
    const current = await readJSON(DATA_FILES.watchlists);
    res.json(current[userId] || { items: [], alerts: [], updatedAt: null });
  })
);

app.get(
  "/api/signals/:symbol",
  errorBoundary(async (req, res) => {
    const symbol = sanitizeSymbol(req.params.symbol);
    const signals = await getSignals(symbol);
    res.json({
      symbol,
      signals,
      latest: signals[0] || null,
    });
  })
);

app.post(
  "/api/alert",
  errorBoundary(async (req, res) => {
    const alert = await createAlert(req.body);
    broadcast("alerts:updated", { alert });
    res.status(201).json(alert);
  })
);

app.get(
  "/api/alerts",
  errorBoundary(async (req, res) => {
    const userId = req.query.userId ? sanitizeUserId(req.query.userId) : null;
    const alerts = await listAlerts(userId);
    res.json(alerts);
  })
);

app.get(
  "/api/screener",
  errorBoundary(async (req, res) => {
    const stocks = await getStocks();
    const withQuotes = await Promise.all(
      stocks.map(async (stock) => {
        const quote = await getLiveQuote(stock.symbol).catch(() => null);
        const signals = await getSignals(stock.symbol).catch(() => []);
        return {
          ...stock,
          price: quote?.price || 0,
          changePercent: quote?.changePercent || 0,
          volume: quote?.volume || 0,
          rsi: signals[0]?.indicators?.rsi ?? null,
          signal: signals[0]?.type || "HOLD",
          signalStrength: signals[0]?.strength || 50,
        };
      })
    );

    const filters = {
      minPrice: req.query.minPrice,
      maxPrice: req.query.maxPrice,
      minMarketCap: req.query.minMarketCap,
      maxMarketCap: req.query.maxMarketCap,
      minDividendYield: req.query.minDividendYield,
      maxDividendYield: req.query.maxDividendYield,
      minBeta: req.query.minBeta,
      maxBeta: req.query.maxBeta,
      minPe: req.query.minPe,
      maxPe: req.query.maxPe,
      minVolume: req.query.minVolume,
      maxVolume: req.query.maxVolume,
      minChange: req.query.minChange,
      maxChange: req.query.maxChange,
      minRsi: req.query.minRsi,
      maxRsi: req.query.maxRsi,
      sector: req.query.sector,
      exchange: req.query.exchange,
      country: req.query.country,
      currency: req.query.currency,
      signal: req.query.signal,
      search: req.query.search,
      earningsBefore: req.query.earningsBefore,
      earningsAfter: req.query.earningsAfter,
    };

    let results = withQuotes.filter((entry) => {
      if (filters.sector && entry.sector.toLowerCase() !== String(filters.sector).toLowerCase()) return false;
      if (filters.exchange && entry.exchange.toLowerCase() !== String(filters.exchange).toLowerCase()) return false;
      if (filters.country && entry.country.toLowerCase() !== String(filters.country).toLowerCase()) return false;
      if (filters.currency && entry.currency.toLowerCase() !== String(filters.currency).toLowerCase()) return false;
      if (filters.signal && entry.signal.toLowerCase() !== String(filters.signal).toLowerCase()) return false;
      if (filters.search) {
        const search = String(filters.search).toLowerCase();
        if (!entry.symbol.toLowerCase().includes(search) && !entry.name.toLowerCase().includes(search)) return false;
      }
      if (filters.earningsBefore && entry.earningsDate && entry.earningsDate > filters.earningsBefore) return false;
      if (filters.earningsAfter && entry.earningsDate && entry.earningsDate < filters.earningsAfter) return false;

      const numericChecks = [
        ["price", filters.minPrice, filters.maxPrice],
        ["marketCap", filters.minMarketCap, filters.maxMarketCap],
        ["dividendYield", filters.minDividendYield, filters.maxDividendYield],
        ["beta", filters.minBeta, filters.maxBeta],
        ["pe", filters.minPe, filters.maxPe],
        ["volume", filters.minVolume, filters.maxVolume],
        ["changePercent", filters.minChange, filters.maxChange],
        ["rsi", filters.minRsi, filters.maxRsi],
      ];

      for (const [key, minValue, maxValue] of numericChecks) {
        const currentValue = safeNumber(entry[key], NaN);
        if (minValue !== undefined && currentValue < safeNumber(minValue, -Infinity)) return false;
        if (maxValue !== undefined && currentValue > safeNumber(maxValue, Infinity)) return false;
      }

      return true;
    });

    const sortBy = sanitizeText(req.query.sortBy || "marketCap", 24);
    const sortDir = String(req.query.sortDir || "desc").toLowerCase() === "asc" ? 1 : -1;
    results = results.sort((a, b) => {
      const left = a[sortBy];
      const right = b[sortBy];
      if (typeof left === "string") return left.localeCompare(String(right || "")) * sortDir;
      return (safeNumber(left) - safeNumber(right)) * sortDir;
    });

    res.json({
      count: results.length,
      filters,
      results: results.slice(0, 200),
    });
  })
);

app.get(
  "/api/news/:symbol",
  errorBoundary(async (req, res) => {
    const symbol = sanitizeSymbol(req.params.symbol);
    const news = await getNews(symbol, { force: req.query.force === "true" });
    res.json(news);
  })
);

app.get(
  "/api/market/overview",
  errorBoundary(async (req, res) => {
    const overview = await buildMarketOverview();
    res.json(overview);
  })
);

app.get(
  ["/ram", "/api/ram"],
  errorBoundary(async (req, res) => {
    res.json(buildRamPayload());
  })
);

io.use((socket, next) => {
  const token = socket.handshake.auth?.token || socket.handshake.query?.token;
  if (token !== AUTH_TOKEN) {
    return next(new Error("Unauthorized"));
  }
  return next();
});

io.on("connection", (socket) => {
  socket.emit("hello", {
    time: Date.now(),
    ram: buildRamPayload(),
  });

  socket.on("watch:quote", async (symbol) => {
    try {
      const normalized = sanitizeSymbol(symbol);
      const quote = await getLiveQuote(normalized);
      socket.emit("quote:update", quote);
    } catch (error) {
      socket.emit("app:error", { message: error.message });
    }
  });

  socket.on("refresh:overview", async () => {
    const overview = await buildMarketOverview().catch(() => null);
    if (overview) {
      socket.emit("market:overview", overview);
    }
  });
});

cron.schedule("*/5 * * * *", () => {
  refreshTopStocks().catch(() => {});
});

cron.schedule("0 * * * *", () => {
  refreshSignals().catch(() => {});
});

cron.schedule("*/15 * * * *", () => {
  refreshNews().catch(() => {});
});

cron.schedule("* * * * *", () => {
  runAlertChecks().catch(() => {});
});

cron.schedule("0 * * * *", () => {
  memoryCleanup().catch(() => {});
});

// Startup seeds JSON files, warms the quote cache, and exposes the app on Render's port.
async function start() {
  await ensureDataFiles();
  await refreshTopStocks().catch(() => {});
  server.listen(PORT, "0.0.0.0", () => {
    console.log(
      JSON.stringify({
        message: "Stock intelligence platform running",
        port: PORT,
        env: NODE_ENV,
        dataDir: DATA_DIR,
        cacheDir: TMP_DIR,
      })
    );
  });
}

start().catch((error) => {
  console.error("Failed to start server:", error);
  process.exit(1);
});

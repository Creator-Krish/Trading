# Stock Intelligence Platform

Render-ready stock market intelligence platform built with Node.js, Express, Socket.IO, scheduled jobs, and flat JSON files only. There is no SQL database, no MongoDB, and no ORM. The app creates and maintains its own JSON datastore on first boot.

## What It Includes

- JSON file storage in `data/` with automatic `/tmp` fallback when the primary directory is not writable
- Atomic JSON writes with temp-file rename and lightweight lock files for concurrent requests
- In-memory cache for quotes and JSON payloads to keep Render free-tier RAM usage low
- Live quote panel, portfolio tracker, watchlist, screener, sector heatmap, alerts, and news sentiment
- Paper trading simulator that updates `portfolios.json`
- Scheduled jobs for prices, signals, news, alert checks, and memory cleanup
- WebSocket live updates with SSE fallback
- Bearer token auth, basic rate limiting, compression, CORS, and input validation

## Output Files

- `server.js`
- `package.json`
- `render.yaml`
- `public/index.html`
- `public/paper-trading.html`
- `.gitignore`

On first startup, the server also creates these JSON files automatically:

- `data/stocks.json`
- `data/prices.json`
- `data/portfolios.json`
- `data/watchlists.json`
- `data/signals.json`
- `data/news.json`
- `data/alerts.json`
- `data/signals_archive.json`

## Local Setup

```bash
npm install
set AUTH_TOKEN=your-secret-token
npm start
```

Open:

- Dashboard: `http://localhost:3000/`
- Paper trading: `http://localhost:3000/paper-trading.html`
- Health check: `http://localhost:3000/health`

Enter the same `AUTH_TOKEN` in the dashboard login area.

## Render Deployment

1. Commit the project to a Git repository.
2. Push the repo to GitHub, GitLab, or Bitbucket.
3. In Render, create a new Blueprint and point it at the repository.
4. Render will pick up `render.yaml`.
5. After deploy, copy the generated `AUTH_TOKEN` from the service environment and use it in the dashboard.

`render.yaml` is configured for:

- `runtime: node`
- `plan: free`
- `buildCommand: npm install`
- `startCommand: node server.js`
- `healthCheckPath: /health`

## JSON Data Model

### `stocks.json`

Seeded automatically with a curated universe of large-cap US stocks and metadata:

```json
[
  {
    "symbol": "AAPL",
    "name": "Apple Inc.",
    "sector": "Technology",
    "exchange": "NASDAQ",
    "marketCap": 2900000000000
  }
]
```

### `prices.json`

```json
{
  "AAPL": {
    "1m": [
      { "t": 1704067200000, "o": 185.5, "h": 186.8, "l": 184.9, "c": 186.2, "v": 52000000 }
    ],
    "1h": [],
    "1d": [],
    "quote": {
      "symbol": "AAPL",
      "price": 186.2,
      "updatedAt": 1704067200000
    }
  }
}
```

### `portfolios.json`

```json
{
  "demo_user": {
    "cash": 50000,
    "holdings": [],
    "transactions": []
  }
}
```

### `signals.json`

```json
{
  "AAPL": [
    {
      "type": "BUY",
      "strength": 85,
      "price": 175.3,
      "time": 1704067200000,
      "reason": "RSI oversold + MACD bullish"
    }
  ]
}
```

## API Reference

All API calls require:

```http
Authorization: Bearer YOUR_AUTH_TOKEN
```

### Quote and History

```bash
curl -H "Authorization: Bearer YOUR_AUTH_TOKEN" \
  http://localhost:3000/api/quote/AAPL
```

```bash
curl -H "Authorization: Bearer YOUR_AUTH_TOKEN" \
  "http://localhost:3000/api/history/AAPL?days=30&timeframe=1d"
```

### Portfolio / Paper Trading

```bash
curl -X POST http://localhost:3000/api/portfolio/add \
  -H "Authorization: Bearer YOUR_AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"userId\":\"demo_user\",\"action\":\"buy\",\"symbol\":\"AAPL\",\"shares\":10}"
```

```bash
curl -H "Authorization: Bearer YOUR_AUTH_TOKEN" \
  http://localhost:3000/api/portfolio/demo_user
```

### Watchlist

```bash
curl -X POST http://localhost:3000/api/watchlist/add \
  -H "Authorization: Bearer YOUR_AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"userId\":\"demo_user\",\"symbol\":\"MSFT\"}"
```

### Signals

```bash
curl -H "Authorization: Bearer YOUR_AUTH_TOKEN" \
  http://localhost:3000/api/signals/NVDA
```

### Alerts

```bash
curl -X POST http://localhost:3000/api/alert \
  -H "Authorization: Bearer YOUR_AUTH_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"userId\":\"demo_user\",\"symbol\":\"AAPL\",\"condition\":\"above\",\"targetPrice\":200}"
```

### Screener

```bash
curl -H "Authorization: Bearer YOUR_AUTH_TOKEN" \
  "http://localhost:3000/api/screener?minPrice=10&sector=Technology&signal=BUY"
```

### News

```bash
curl -H "Authorization: Bearer YOUR_AUTH_TOKEN" \
  http://localhost:3000/api/news/AAPL
```

### RAM Usage

```bash
curl -H "Authorization: Bearer YOUR_AUTH_TOKEN" \
  http://localhost:3000/api/ram
```

## Scheduled Jobs

- Every 5 minutes: refreshes the top symbol batch and appends cached prices
- Every hour: regenerates technical signals
- Every 15 minutes: refreshes cached news sentiment
- Every minute: checks pending alerts
- Every hour: rotates old cached data, trims memory, and archives weekly signals

## Offline Behavior

After the first successful market and news fetch, the app continues serving from JSON cache even when upstream data sources are unreachable. The dashboard and paper trading features keep working against cached prices and saved portfolio state.

## Performance Notes

- JSON reads are memory cached for 60 seconds
- Writes use temp-file rename for atomicity
- Lock files prevent overlapping writers on the same JSON file
- Quote cache avoids repetitive upstream requests
- Data rotation keeps files bounded on free-tier storage

## Important Deployment Note

Render free-tier instances do not provide a persistent disk by default. This project uses flat JSON files exactly as requested, but on a free instance those files should be treated as ephemeral runtime storage unless you attach a persistent disk on a paid plan. The app automatically falls back to `/tmp` when needed so it still runs cleanly on free tier.

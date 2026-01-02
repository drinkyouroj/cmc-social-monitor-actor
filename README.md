# CMC Social Monitor (Apify Actor)

Monitors social media posts + comments and flags anything containing one or more configured **symbols/terms** (e.g. token symbols like `DGRAM`).

This Actor is designed as an **orchestrator**:
- It can call one or more existing Apify Store Actors (e.g., X/Twitter, YouTube, Reddit scrapers).
- It pulls their output datasets, normalizes text, filters matches, dedupes, and stores results in **its own dataset**.

> Note: scraping and automation may be restricted by platform terms. Prefer official APIs when available, or ensure you have the rights/permission to monitor the content.

## How it works (high level)

1. For each `platformRuns[]` entry in input:
   - call the referenced Actor with its `input`
   - read items from its default dataset
2. Extract text fields (post text, comment text, etc.)
3. Filter items that match:
   - symbols/terms (case-insensitive by default)
4. Dedupe using an Apify KV-store state record
5. Push matches to this Actor’s dataset (optionally notify via webhook)

## Input example

```json
{
  "symbols": ["DGRAM"],
  "caseInsensitive": true,
  "useWordBoundaries": false,
  "platformRuns": [
    {
      "name": "coinmarketcap",
      "actorId": "cmc/currency-news",
      "input": {
        "currencySlugOrUrl": "https://coinmarketcap.com/currencies/datagram-network/",
        "maxItems": 200
      }
    }
  ],
  "dedupe": {
    "enabled": true,
    "maxSeenIdsPerPlatform": 5000
  },
  "notify": {
    "webhookUrl": ""
  }
}
```

### Important notes

- **`platformRuns` cannot be empty** if you want results. This Actor only filters items produced by the scraper Actors you configure in `platformRuns`.
- **CoinMarketCap.com sources (no external scraper needed)**:
  - `cmc/headlines-news`: CoinMarketCap Headlines (general news feed)
  - `cmc/currency-news`: CoinMarketCap *token page* news for a specific currency page (provide `input.currencySlugOrUrl` or `input.coinId`)
- **Pick a valid `actorId`** from the Apify Store. Open the Actor page and copy it from the URL, e.g. `username/actor-name` (also accepted: `username~actor-name`).
- **Configure monitored symbols** via **top-level `symbols`** (recommended). For backwards compatibility, you can also use `match.symbols`.

## Local development

This repo is intended to run on Apify, but you can run it locally if you have Node.js 18+.

```bash
npm install
npm run start
```

Environment variables:
- `APIFY_TOKEN` (required if you call other Actors via the API)

## Deploy to Apify

You can create and deploy Actors using Apify templates and the Apify CLI (see Apify templates page: `https://apify.com/templates`).



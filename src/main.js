const { Actor, ApifyClient, log } = require('apify');

const STATE_KEY = 'STATE';

function escapeRegExp(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function normalizeSymbols(symbols) {
  return (symbols || []).map((k) => String(k || '').trim()).filter(Boolean);
}

function buildSymbolsRegex({ symbols, caseInsensitive, useWordBoundaries }) {
  const cleaned = normalizeSymbols(symbols);
  if (cleaned.length === 0) return null;

  const parts = cleaned.map((k) => escapeRegExp(k));
  const inner = parts.join('|');
  const pattern = useWordBoundaries ? `\\b(?:${inner})\\b` : `(?:${inner})`;
  return new RegExp(pattern, caseInsensitive ? 'i' : undefined);
}

function extractTextCandidates(item) {
  // Heuristic extraction. Different platform scrapers use different field names.
  // We grab a few common ones and also stringify a subset of the object.
  const fields = [
    'text',
    'fullText',
    'content',
    'body',
    'title',
    'caption',
    'comment',
    'commentText',
    'replyText',
    'description',
  ];

  const candidates = [];
  for (const key of fields) {
    const val = item?.[key];
    if (typeof val === 'string' && val.trim()) candidates.push(val);
  }

  // Some scrapers nest main text in objects (e.g., { tweet: { text } }).
  const nestedKeys = ['tweet', 'post', 'video', 'thread', 'data'];
  for (const nk of nestedKeys) {
    const obj = item?.[nk];
    if (obj && typeof obj === 'object') {
      for (const key of fields) {
        const val = obj?.[key];
        if (typeof val === 'string' && val.trim()) candidates.push(val);
      }
    }
  }

  return [...new Set(candidates)];
}

function pickStableId(item) {
  // Best-effort. Many datasets provide one of these.
  const keys = ['id', 'tweetId', 'postId', 'commentId', 'url', 'permalink'];
  for (const k of keys) {
    const v = item?.[k];
    if (typeof v === 'string' && v.trim()) return v.trim();
    if (typeof v === 'number' && Number.isFinite(v)) return String(v);
  }
  return null;
}

function matchTextAgainstSymbols({ text, symbols, caseInsensitive, useWordBoundaries }) {
  const cleaned = normalizeSymbols(symbols);
  if (cleaned.length === 0) return { isMatch: false, matchedSymbols: [] };

  const matchedSymbols = [];

  for (const sym of cleaned) {
    const inner = escapeRegExp(sym);
    const pattern = useWordBoundaries ? `\\b(?:${inner})\\b` : `(?:${inner})`;
    const re = new RegExp(pattern, caseInsensitive ? 'i' : undefined);
    if (re.test(text)) matchedSymbols.push(sym);
  }

  return { isMatch: matchedSymbols.length > 0, matchedSymbols };
}

function extractNextDataJson(html) {
  const m = String(html || '').match(/<script id="__NEXT_DATA__" type="application\/json"[^>]*>([\s\S]*?)<\/script>/);
  if (!m) return null;
  try {
    return JSON.parse(m[1]);
  } catch {
    return null;
  }
}

async function fetchCmcHeadlinesNews({ maxItems } = {}) {
  const url = 'https://coinmarketcap.com/headlines/news/';
  const res = await fetch(url, {
    headers: {
      // Basic UA helps avoid occasional bot-block behavior for simple HTML fetches.
      'user-agent': 'Mozilla/5.0 (compatible; CMC-Social-Monitor/0.1; +https://apify.com)',
      accept: 'text/html,application/xhtml+xml',
    },
  });
  if (!res.ok) throw new Error(`CoinMarketCap fetch failed: ${res.status} ${res.statusText}`);
  const html = await res.text();

  const nextData = extractNextDataJson(html);
  const feed = nextData?.props?.pageProps?.newsFeed;
  if (!Array.isArray(feed)) throw new Error('CoinMarketCap: could not find newsFeed in page HTML.');

  const limit = Number.isFinite(maxItems) ? Math.max(1, maxItems) : feed.length;
  return feed.slice(0, limit).map((entry) => {
    const meta = entry?.meta || {};
    const title = meta.title || entry?.slug || '';
    const description = meta.subtitle || '';
    const permalink = meta.sourceUrl || entry?.meta?.sourceUrl || '';
    const assetSymbols = Array.isArray(entry?.assets)
      ? entry.assets.map((a) => a?.symbol).filter((s) => typeof s === 'string' && s.trim()).map((s) => s.trim())
      : [];
    return {
      id: meta.id || entry?.slug || permalink || null,
      url: permalink || null,
      title,
      description,
      assets: assetSymbols,
      text: [title, description, assetSymbols.length ? `Assets: ${assetSymbols.join(', ')}` : '']
        .filter(Boolean)
        .join('\n')
        .trim(),
      sourceName: meta.sourceName || null,
      releasedAt: meta.releasedAt || null,
      raw: entry,
    };
  });
}

function normalizeCmcCurrencySlug(slugOrUrl) {
  const raw = String(slugOrUrl || '').trim();
  if (!raw) return null;
  if (raw.startsWith('http://') || raw.startsWith('https://')) {
    try {
      const u = new URL(raw);
      const m = u.pathname.match(/\/currencies\/([^/]+)\/?/);
      return m?.[1] || null;
    } catch {
      return null;
    }
  }
  return raw.replace(/^\/+|\/+$/g, '');
}

async function resolveCmcCoinId({ coinId, currencySlugOrUrl } = {}) {
  const direct = Number(coinId);
  if (Number.isFinite(direct) && direct > 0) return direct;

  const slug = normalizeCmcCurrencySlug(currencySlugOrUrl);
  if (!slug) return null;

  const url = `https://coinmarketcap.com/currencies/${slug}/`;
  const res = await fetch(url, {
    headers: {
      'user-agent': 'Mozilla/5.0 (compatible; CMC-Social-Monitor/0.1; +https://apify.com)',
      accept: 'text/html,application/xhtml+xml',
    },
  });
  if (!res.ok) throw new Error(`CoinMarketCap currency page fetch failed: ${res.status} ${res.statusText}`);
  const html = await res.text();

  const nextData = extractNextDataJson(html);
  const id = nextData?.props?.pageProps?.detailRes?.detail?.id;
  const num = Number(id);
  return Number.isFinite(num) && num > 0 ? num : null;
}

async function fetchCmcCurrencyNews({ coinId, currencySlugOrUrl, maxItems, language } = {}) {
  const resolvedId = await resolveCmcCoinId({ coinId, currencySlugOrUrl });
  if (!resolvedId) throw new Error('CoinMarketCap: could not resolve coinId from currencySlugOrUrl.');

  const size = Number.isFinite(maxItems) ? Math.max(1, Math.min(200, maxItems)) : 50;
  const lang = String(language || 'en').trim() || 'en';
  const url = `https://api.coinmarketcap.com/content/v3/news?coins=${encodeURIComponent(String(resolvedId))}&language=${encodeURIComponent(lang)}&size=${encodeURIComponent(String(size))}`;

  const res = await fetch(url, {
    headers: {
      'user-agent': 'Mozilla/5.0 (compatible; CMC-Social-Monitor/0.1; +https://apify.com)',
      accept: 'application/json,text/plain,*/*',
    },
  });
  if (!res.ok) throw new Error(`CoinMarketCap news API fetch failed: ${res.status} ${res.statusText}`);
  const body = await res.json();
  const items = Array.isArray(body?.data) ? body.data : [];

  return items.map((entry) => {
    const meta = entry?.meta || {};
    const title = meta.title || entry?.slug || '';
    const description = meta.subtitle || '';
    const permalink = meta.sourceUrl || null;
    const assets = Array.isArray(entry?.assets)
      ? entry.assets
          .map((a) => a?.name)
          .filter((s) => typeof s === 'string' && s.trim())
          .map((s) => s.trim())
      : [];

    return {
      id: meta.id || entry?.slug || permalink || null,
      url: permalink,
      title,
      description,
      assets,
      releasedAt: meta.releasedAt || null,
      sourceName: meta.sourceName || null,
      text: [title, description, assets.length ? `Assets: ${assets.join(', ')}` : ''].filter(Boolean).join('\n').trim(),
      raw: entry,
    };
  });
}

async function maybeNotify(webhookUrl, payload) {
  if (!webhookUrl) return;

  const res = await fetch(webhookUrl, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`Webhook POST failed: ${res.status} ${res.statusText} ${body}`.trim());
  }
}

async function loadState() {
  const store = await Actor.openKeyValueStore();
  const existing = await store.getValue(STATE_KEY);
  if (!existing || typeof existing !== 'object') return { seenByPlatform: {} };
  if (!existing.seenByPlatform || typeof existing.seenByPlatform !== 'object') return { seenByPlatform: {} };
  return existing;
}

async function saveState(state) {
  const store = await Actor.openKeyValueStore();
  await store.setValue(STATE_KEY, state);
}

function rememberSeenId({ state, platform, id, maxSeenIdsPerPlatform }) {
  if (!id) return;
  if (!state.seenByPlatform[platform]) state.seenByPlatform[platform] = [];

  const arr = state.seenByPlatform[platform];
  if (arr.includes(id)) return;
  arr.unshift(id);

  if (arr.length > maxSeenIdsPerPlatform) arr.length = maxSeenIdsPerPlatform;
}

function alreadySeen({ state, platform, id }) {
  if (!id) return false;
  const arr = state.seenByPlatform?.[platform];
  if (!Array.isArray(arr)) return false;
  return arr.includes(id);
}

Actor.main(async () => {
  const input = (await Actor.getInput()) || {};

  const platformRuns = Array.isArray(input.platformRuns) ? input.platformRuns : [];
  const match = input.match || {};
  const dedupe = input.dedupe || {};
  const notify = input.notify || {};
  const debug = input.debug || {};

  // Backwards-compatible input:
  // - preferred: top-level symbols/caseInsensitive/useWordBoundaries
  // - legacy/advanced: match.{symbols,caseInsensitive,useWordBoundaries}
  const symbols = normalizeSymbols(match.symbols ?? input.symbols);
  const caseInsensitive = (match.caseInsensitive ?? input.caseInsensitive) !== false;
  const useWordBoundaries = (match.useWordBoundaries ?? input.useWordBoundaries) === true;

  const symbolsRegex = buildSymbolsRegex({
    symbols,
    caseInsensitive,
    useWordBoundaries,
  });

  const dedupeEnabled = dedupe.enabled !== false;
  const maxSeenIdsPerPlatform = Number.isFinite(dedupe.maxSeenIdsPerPlatform)
    ? dedupe.maxSeenIdsPerPlatform
    : 5000;
  const maxItemsPerDataset = Number.isFinite(debug.maxItemsPerDataset) ? debug.maxItemsPerDataset : 1000;

  if (!symbolsRegex) {
    throw new Error('Invalid configuration: provide symbols (e.g. ["DGRAM"]).');
  }

  if (platformRuns.length === 0) {
    throw new Error(
      'Invalid configuration: provide platformRuns with at least one scraper Actor (e.g. [{"name":"x","actorId":"apify/twitter-scraper","input":{"searchTerms":["DGRAM"],"maxTweets":200}}]).'
    );
  }

  const state = await loadState();
  const client = new ApifyClient({ token: process.env.APIFY_TOKEN });

  log.info('Starting CMC Social Monitor', {
    platformRuns: platformRuns.map((p) => ({ name: p?.name, actorId: p?.actorId })),
    symbols,
    caseInsensitive,
    useWordBoundaries,
    dedupeEnabled,
  });

  for (const pr of platformRuns) {
    const platform = String(pr?.name || '').trim() || 'unknown';
    const actorId = String(pr?.actorId || '').trim();
    const actorInput = pr?.input || {};

    if (!actorId) {
      log.warning('Skipping platform run with missing actorId', { platform });
      continue;
    }

    // Built-in sources (no external Actor needed).
    // This keeps the same `platformRuns[]` shape but allows scraping CoinMarketCap directly.
    if (actorId === 'cmc/headlines-news') {
      log.info('Fetching CoinMarketCap Headlines (News)', { platform, actorId });

      const items = await fetchCmcHeadlinesNews({ maxItems: actorInput?.maxItems });

      let totalChecked = 0;
      let totalEmitted = 0;

      for (const item of items) {
        totalChecked += 1;
        if (totalChecked > maxItemsPerDataset) break;

        const stableId = pickStableId(item) || `cmc:${totalChecked}`;
        if (dedupeEnabled && alreadySeen({ state, platform, id: stableId })) continue;

        const texts = extractTextCandidates(item);
        const joined = texts.join('\n').trim();
        if (!joined) {
          rememberSeenId({ state, platform, id: stableId, maxSeenIdsPerPlatform });
          continue;
        }

        if (!symbolsRegex.test(joined)) {
          rememberSeenId({ state, platform, id: stableId, maxSeenIdsPerPlatform });
          continue;
        }

        const { isMatch, matchedSymbols } = matchTextAgainstSymbols({
          text: joined,
          symbols,
          caseInsensitive,
          useWordBoundaries,
        });

        if (!isMatch) {
          rememberSeenId({ state, platform, id: stableId, maxSeenIdsPerPlatform });
          continue;
        }

        const out = {
          platform,
          stableId,
          matchedAt: new Date().toISOString(),
          matchedSymbols,
          text: joined,
          source: {
            actorId,
            runId: null,
            datasetId: null,
            url: 'https://coinmarketcap.com/headlines/news/',
          },
          raw: item,
        };

        await Actor.pushData(out);
        totalEmitted += 1;

        rememberSeenId({ state, platform, id: stableId, maxSeenIdsPerPlatform });

        try {
          await maybeNotify(String(notify.webhookUrl || '').trim(), out);
        } catch (e) {
          log.exception(e, 'Notification failed');
        }
      }

      log.info('Platform run complete', { platform, checked: totalChecked, emitted: totalEmitted });
      continue;
    }

    if (actorId === 'cmc/currency-news') {
      log.info('Fetching CoinMarketCap Currency News', { platform, actorId });

      const items = await fetchCmcCurrencyNews({
        coinId: actorInput?.coinId,
        currencySlugOrUrl: actorInput?.currencySlugOrUrl,
        maxItems: actorInput?.maxItems,
        language: actorInput?.language,
      });

      let totalChecked = 0;
      let totalEmitted = 0;

      for (const item of items) {
        totalChecked += 1;
        if (totalChecked > maxItemsPerDataset) break;

        const stableId = pickStableId(item) || String(item?.id || `cmc-news:${totalChecked}`);
        if (dedupeEnabled && alreadySeen({ state, platform, id: stableId })) continue;

        const texts = extractTextCandidates(item);
        const joined = texts.join('\n').trim();
        if (!joined) {
          rememberSeenId({ state, platform, id: stableId, maxSeenIdsPerPlatform });
          continue;
        }

        if (!symbolsRegex.test(joined)) {
          rememberSeenId({ state, platform, id: stableId, maxSeenIdsPerPlatform });
          continue;
        }

        const { isMatch, matchedSymbols } = matchTextAgainstSymbols({
          text: joined,
          symbols,
          caseInsensitive,
          useWordBoundaries,
        });

        if (!isMatch) {
          rememberSeenId({ state, platform, id: stableId, maxSeenIdsPerPlatform });
          continue;
        }

        const out = {
          platform,
          stableId,
          matchedAt: new Date().toISOString(),
          matchedSymbols,
          text: joined,
          source: {
            actorId,
            runId: null,
            datasetId: null,
            url: item?.url || null,
          },
          raw: item,
        };

        await Actor.pushData(out);
        totalEmitted += 1;

        rememberSeenId({ state, platform, id: stableId, maxSeenIdsPerPlatform });

        try {
          await maybeNotify(String(notify.webhookUrl || '').trim(), out);
        } catch (e) {
          log.exception(e, 'Notification failed');
        }
      }

      log.info('Platform run complete', { platform, checked: totalChecked, emitted: totalEmitted });
      continue;
    }

    log.info('Running scraper Actor', { platform, actorId });
    let run;
    try {
      run = await client.actor(actorId).call(actorInput);
    } catch (e) {
      const statusCode = e?.statusCode;
      const type = e?.type;
      if (statusCode === 402 || type === 'not-enough-usage-to-run-paid-actor') {
        throw new Error(
          [
            `Not enough Apify usage to run paid Store Actor: "${actorId}".`,
            'This means the Actor you configured is paid and your current account/org does not have enough remaining platform usage to start it.',
            'Fix: switch to a free Actor (or your own), upgrade billing, or use the built-in CoinMarketCap.com source: platformRuns[].actorId = "cmc/headlines-news".',
          ].join(' ')
        );
      }
      if (statusCode === 404 || type === 'record-not-found') {
        throw new Error(
          [
            `Scraper Actor not found: "${actorId}".`,
            'Open the Actor in the Apify Store and copy the ID from the URL (format: username/actor-name).',
            'Then paste it into platformRuns[].actorId.',
          ].join(' ')
        );
      }
      if (statusCode === 401 || statusCode === 403) {
        throw new Error(
          [
            `Not authorized to run scraper Actor: "${actorId}".`,
            'Ensure your run has a valid APIFY_TOKEN (and access to that Actor if it is private/paid).',
          ].join(' ')
        );
      }
      throw e;
    }
    const datasetId = run?.defaultDatasetId;

    if (!datasetId) {
      log.warning('Scraper run has no defaultDatasetId; skipping', { platform, actorId, runId: run?.id });
      continue;
    }

    log.info('Reading scraper dataset', { platform, datasetId, runId: run?.id });

    let offset = 0;
    const limit = 250;
    let totalChecked = 0;
    let totalEmitted = 0;

    while (true) {
      const page = await client
        .dataset(datasetId)
        .listItems({ limit, offset, clean: true, desc: true });

      const items = Array.isArray(page?.items) ? page.items : [];
      if (items.length === 0) break;

      for (const item of items) {
        totalChecked += 1;
        if (totalChecked > maxItemsPerDataset) break;

        const stableId = pickStableId(item) || `${datasetId}:${offset}:${totalChecked}`;
        if (dedupeEnabled && alreadySeen({ state, platform, id: stableId })) continue;

        const texts = extractTextCandidates(item);
        const joined = texts.join('\n').trim();
        if (!joined) {
          rememberSeenId({ state, platform, id: stableId, maxSeenIdsPerPlatform });
          continue;
        }

        // Fast precheck (single regex). We still compute matchedSymbols for output.
        if (!symbolsRegex.test(joined)) {
          rememberSeenId({ state, platform, id: stableId, maxSeenIdsPerPlatform });
          continue;
        }

        const { isMatch, matchedSymbols } = matchTextAgainstSymbols({
          text: joined,
          symbols,
          caseInsensitive,
          useWordBoundaries,
        });

        if (!isMatch) {
          rememberSeenId({ state, platform, id: stableId, maxSeenIdsPerPlatform });
          continue;
        }

        const out = {
          platform,
          stableId,
          matchedAt: new Date().toISOString(),
          matchedSymbols,
          text: joined,
          source: {
            actorId,
            runId: run?.id,
            datasetId,
          },
          raw: item,
        };

        await Actor.pushData(out);
        totalEmitted += 1;

        rememberSeenId({ state, platform, id: stableId, maxSeenIdsPerPlatform });

        try {
          await maybeNotify(String(notify.webhookUrl || '').trim(), out);
        } catch (e) {
          log.exception(e, 'Notification failed');
        }
      }

      if (totalChecked > maxItemsPerDataset) break;
      offset += items.length;
    }

    log.info('Platform run complete', { platform, checked: totalChecked, emitted: totalEmitted });
  }

  await saveState(state);
  log.info('Done');
});



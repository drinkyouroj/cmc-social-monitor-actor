const { Actor, ApifyClient, log } = require('apify');
const { chromium } = require('playwright');

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

function normalizeCmcCommunityPostId(postIdOrUrl) {
  const raw = String(postIdOrUrl || '').trim();
  if (!raw) return null;
  if (raw.startsWith('http://') || raw.startsWith('https://')) {
    try {
      const u = new URL(raw);
      const m = u.pathname.match(/\/community\/post\/(\d+)\/?/);
      return m?.[1] || null;
    } catch {
      return null;
    }
  }
  const m = raw.match(/(\d+)/);
  return m?.[1] || null;
}

async function fetchCmcCommunityPost({ postIdOrUrl } = {}) {
  const postId = normalizeCmcCommunityPostId(postIdOrUrl);
  if (!postId) throw new Error('CoinMarketCap community post: missing/invalid postIdOrUrl.');

  const url = `https://coinmarketcap.com/community/post/${postId}/`;
  const res = await fetch(url, {
    headers: {
      'user-agent': 'Mozilla/5.0 (compatible; CMC-Social-Monitor/0.1; +https://apify.com)',
      accept: 'text/html,application/xhtml+xml',
    },
  });
  if (!res.ok) throw new Error(`CoinMarketCap community post fetch failed: ${res.status} ${res.statusText}`);
  const html = await res.text();

  const nextData = extractNextDataJson(html);
  const queries = nextData?.props?.pageProps?.dehydratedState?.queries || [];
  const postQuery = queries.find((q) => Array.isArray(q?.queryKey) && q.queryKey[0] === 'post' && String(q.queryKey[1]) === String(postId));
  const post = postQuery?.state?.data?.[0];
  if (!post || typeof post !== 'object') throw new Error('CoinMarketCap community post: could not extract post data from page.');

  const text = String(post?.textContent || '').trim();
  const owner = post?.owner || {};
  const handle = owner?.handle || null;

  return {
    kind: 'post',
    id: String(post?.gravityId || postId),
    url,
    ownerHandle: handle,
    ownerGuid: owner?.guid || null,
    createdAt: post?.postTime ? new Date(Number(post.postTime)).toISOString() : null,
    text,
    raw: post,
  };
}

async function fetchCmcCommunityComments({ rootId, maxItems, sort } = {}) {
  const rid = String(rootId || '').trim();
  if (!rid) return [];

  const size = Number.isFinite(maxItems) ? Math.max(1, Math.min(50, maxItems)) : 20;
  const sortParam = String(sort || '').trim();

  // NOTE: CoinMarketCap may change these internal endpoints. We try a reasonable default and fail gracefully.
  const base = 'https://api.coinmarketcap.com/gravity/v4/gravity/comment/list';

  const params = new URLSearchParams();
  params.set('rootId', rid);
  params.set('page', '1');
  params.set('size', String(size));
  if (sortParam) params.set('sort', sortParam);

  const url = `${base}?${params.toString()}`;

  const res = await fetch(url, {
    headers: {
      'user-agent': 'Mozilla/5.0 (compatible; CMC-Social-Monitor/0.1; +https://apify.com)',
      accept: 'application/json,text/plain,*/*',
      referer: `https://coinmarketcap.com/community/post/${encodeURIComponent(rid)}/`,
      origin: 'https://coinmarketcap.com',
    },
  });
  if (!res.ok) throw new Error(`CoinMarketCap community comments fetch failed: ${res.status} ${res.statusText}`);

  const body = await res.json().catch(() => null);
  const status = body?.status;
  const errCode = status?.error_code;
  if (errCode && String(errCode) !== '0') {
    log.warning('CoinMarketCap community comments returned non-success status', {
      rootId: rid,
      error_code: status?.error_code,
      error_message: status?.error_message,
    });
    return [];
  }

  const items = Array.isArray(body?.data) ? body.data : [];
  return items.map((c) => {
    const owner = c?.owner || {};
    return {
      kind: 'comment',
      id: String(c?.gravityId || ''),
      rootId: String(c?.rootId || rid),
      replyToGravityId: c?.replyToGravityId ? String(c.replyToGravityId) : null,
      ownerHandle: owner?.handle || null,
      ownerGuid: owner?.guid || null,
      createdAt: c?.postTime ? new Date(Number(c.postTime)).toISOString() : null,
      text: String(c?.textContent || '').trim(),
      raw: c,
    };
  });
}

async function runWithPlaywright(fn) {
  const launchAttempts = [
    { headless: true },
    // In Apify's Playwright images, system Chrome is typically available; this avoids needing `npx playwright install`.
    { headless: true, channel: 'chrome' },
    { headless: true, executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH || undefined },
    { headless: true, executablePath: '/usr/bin/google-chrome' },
    { headless: true, executablePath: '/usr/bin/google-chrome-stable' },
    { headless: true, executablePath: '/usr/bin/chromium-browser' },
    { headless: true, executablePath: '/usr/bin/chromium' },
  ].filter((o) => !('executablePath' in o) || o.executablePath);

  let browser;
  let lastErr;
  for (const opts of launchAttempts) {
    try {
      browser = await chromium.launch(opts);
      lastErr = null;
      break;
    } catch (e) {
      lastErr = e;
    }
  }
  if (!browser) throw lastErr;

  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (compatible; CMC-Social-Monitor/0.1; +https://apify.com)',
  });
  try {
    return await fn({ browser, context });
  } finally {
    await context.close().catch(() => null);
    await browser.close().catch(() => null);
  }
}

async function maybeAcceptCookies(page) {
  const candidates = [
    'button:has-text("Accept")',
    'button:has-text("I Agree")',
    'button:has-text("Agree")',
    'button:has-text("Accept all")',
    'button:has-text("Accept All")',
    'button:has-text("OK")',
    'button:has-text("Got it")',
  ];
  for (const sel of candidates) {
    try {
      const loc = page.locator(sel).first();
      if (await loc.count()) {
        await loc.click({ timeout: 1000 }).catch(() => null);
        await page.waitForTimeout(500);
        return true;
      }
    } catch {
      // ignore
    }
  }
  return false;
}

async function discoverCmcCommunityPostIdsByQuery({ query, mode, maxPosts } = {}) {
  const q = String(query || '').trim();
  if (!q) return [];

  const m = String(mode || 'latest').trim().toLowerCase();
  const safeMode = m === 'top' ? 'top' : 'latest';
  const targetMax = Number.isFinite(maxPosts) ? Math.max(1, Math.min(500, maxPosts)) : 50;

  const startUrl = `https://coinmarketcap.com/community/search/${safeMode}/${encodeURIComponent(q)}`;

  return await runWithPlaywright(async ({ context }) => {
    const page = await context.newPage();
    const found = new Set();

    page.setDefaultTimeout(60000);
    await page.goto(startUrl, { waitUntil: 'domcontentloaded' });
    await maybeAcceptCookies(page);
    // Give client-side app time to hydrate/fetch.
    await page.waitForLoadState('networkidle', { timeout: 60000 }).catch(() => null);

    const selectorCandidates = [
      'a[href*="/community/post/"]',
      'a[href^="/community/post/"]',
      'a[href*="coinmarketcap.com/community/post/"]',
    ];

    let selectorOk = false;
    for (const sel of selectorCandidates) {
      try {
        await page.waitForSelector(sel, { timeout: 15000 });
        selectorOk = true;
        break;
      } catch {
        // try next
      }
    }

    if (!selectorOk) {
      // Fallback: try extracting IDs from the page HTML (even if links aren't visible as anchors).
      const html = await page.content().catch(() => '');
      const ids = [...String(html).matchAll(/\/community\/post\/(\d+)/g)].map((m) => m[1]);
      for (const id of ids) {
        found.add(id);
        if (found.size >= targetMax) break;
      }

      // Save debug artifacts for this run so we can see what was rendered in Apify.
      try {
        const title = await page.title().catch(() => '');
        const url = page.url();
        await Actor.setValue(`DEBUG_cmc_community_search_${safeMode}_${q}_meta.json`, { url, title, found: found.size }, { contentType: 'application/json' });
        await Actor.setValue(`DEBUG_cmc_community_search_${safeMode}_${q}.html`, html || '', { contentType: 'text/html' });
        const shot = await page.screenshot({ fullPage: true }).catch(() => null);
        if (shot) await Actor.setValue(`DEBUG_cmc_community_search_${safeMode}_${q}.png`, shot, { contentType: 'image/png' });
      } catch {
        // ignore debug failures
      }

      await page.close().catch(() => null);
      return [...found];
    }

    let stableRounds = 0;
    let lastCount = 0;

    while (found.size < targetMax && stableRounds < 4) {
      const hrefs = await page.$$eval('a[href*="/community/post/"],a[href^="/community/post/"]', (as) =>
        as.map((a) => a.getAttribute('href')).filter(Boolean)
      );

      for (const href of hrefs) {
        const mm = String(href).match(/\/community\/post\/(\d+)/);
        if (mm?.[1]) found.add(mm[1]);
        if (found.size >= targetMax) break;
      }

      if (found.size === lastCount) stableRounds += 1;
      else stableRounds = 0;
      lastCount = found.size;

      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await page.waitForTimeout(1500);
    }

    await page.close().catch(() => null);
    return [...found];
  });
}

async function fetchCmcCommunityCommentsFromBrowser({ postId, maxComments, context: existingContext } = {}) {
  const pid = normalizeCmcCommunityPostId(postId);
  if (!pid) return [];

  const limit = Number.isFinite(maxComments) ? Math.max(1, Math.min(200, maxComments)) : 50;
  const url = `https://coinmarketcap.com/community/post/${pid}/`;

  const run = async ({ context }) => {
    const page = await context.newPage();
    await page.goto(url, { waitUntil: 'domcontentloaded' });

    const comments = [];
    const seen = new Set();

    await page.waitForSelector('.comment-post-item', { timeout: 45000 }).catch(() => null);

    let stableRounds = 0;
    let lastCount = 0;

    while (comments.length < limit && stableRounds < 4) {
      for (const txt of ['Show more replies', 'More Replies', 'Show more', 'See more']) {
        try {
          const btn = page.locator(`button:has-text(\"${txt}\")`).first();
          if (await btn.count()) await btn.click({ timeout: 1000 }).catch(() => null);
        } catch {
          // ignore
        }
      }

      const texts = await page.$$eval('.comment-post-item', (nodes) =>
        nodes
          .map((n) => (n instanceof HTMLElement ? n.innerText : ''))
          .map((t) => String(t || '').trim())
          .filter(Boolean)
      );

      for (const t of texts) {
        if (comments.length >= limit) break;
        if (!seen.has(t)) {
          seen.add(t);
          comments.push(t);
        }
      }

      if (comments.length === lastCount) stableRounds += 1;
      else stableRounds = 0;
      lastCount = comments.length;

      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await page.waitForTimeout(1500);
    }

    await page.close().catch(() => null);
    return comments;
  };

  if (existingContext) return await run({ context: existingContext });
  return await runWithPlaywright(async ({ context }) => run({ context }));
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

    if (actorId === 'cmc/community-post') {
      log.info('Fetching CoinMarketCap Community Post', { platform, actorId });

      const post = await fetchCmcCommunityPost({ postIdOrUrl: actorInput?.postIdOrUrl || actorInput?.postUrl || actorInput?.postId });
      const comments = actorInput?.includeComments === false
        ? []
        : await fetchCmcCommunityComments({
            rootId: post?.id,
            maxItems: actorInput?.maxComments,
            sort: actorInput?.commentsSort,
          });

      const all = [post, ...comments].filter(Boolean);

      let totalChecked = 0;
      let totalEmitted = 0;

      for (const item of all) {
        totalChecked += 1;
        if (totalChecked > maxItemsPerDataset) break;

        const stableId = `cmc-community:${item?.kind || 'item'}:${String(item?.id || totalChecked)}`;
        if (dedupeEnabled && alreadySeen({ state, platform, id: stableId })) continue;

        const joined = String(item?.text || '').trim();
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
            url: post?.url || null,
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

    if (actorId === 'cmc/community-search') {
      log.info('Fetching CoinMarketCap Community Search', { platform, actorId });

      const query = String(actorInput?.query || actorInput?.keyword || actorInput?.term || symbols?.[0] || '').trim();
      if (!query) throw new Error('cmc/community-search: provide input.query (or top-level symbols).');

      const maxPosts = Number.isFinite(actorInput?.maxPosts) ? actorInput.maxPosts : 50;
      const mode = actorInput?.mode || actorInput?.sort || 'latest';
      const includeComments = actorInput?.includeComments === true;
      const maxCommentsPerPost = Number.isFinite(actorInput?.maxCommentsPerPost) ? actorInput.maxCommentsPerPost : 50;

      let totalChecked = 0;
      let totalEmitted = 0;

      // Reuse one browser context for the whole run (much faster than launching per post).
      await runWithPlaywright(async ({ context }) => {
        const postIds = await (async () => {
          // Discover IDs using the same context by temporarily swapping runWithPlaywright.
          const page = await context.newPage();
          const m = String(mode || 'latest').trim().toLowerCase();
          const safeMode = m === 'top' ? 'top' : 'latest';
          const targetMax = Number.isFinite(maxPosts) ? Math.max(1, Math.min(500, maxPosts)) : 50;
          const startUrl = `https://coinmarketcap.com/community/search/${safeMode}/${encodeURIComponent(query)}`;
          const found = new Set();

          page.setDefaultTimeout(60000);
          await page.goto(startUrl, { waitUntil: 'domcontentloaded' });
          await maybeAcceptCookies(page);
          await page.waitForLoadState('networkidle', { timeout: 60000 }).catch(() => null);

          const selectorCandidates = [
            'a[href*="/community/post/"]',
            'a[href^="/community/post/"]',
            'a[href*="coinmarketcap.com/community/post/"]',
          ];

          let selectorOk = false;
          for (const sel of selectorCandidates) {
            try {
              await page.waitForSelector(sel, { timeout: 15000 });
              selectorOk = true;
              break;
            } catch {
              // try next
            }
          }

          if (!selectorOk) {
            const html = await page.content().catch(() => '');
            const ids = [...String(html).matchAll(/\/community\/post\/(\d+)/g)].map((mm) => mm[1]);
            for (const id of ids) {
              found.add(id);
              if (found.size >= targetMax) break;
            }

            try {
              const title = await page.title().catch(() => '');
              const currentUrl = page.url();
              await Actor.setValue(
                `DEBUG_cmc_community_search_${safeMode}_${query}_meta.json`,
                { url: currentUrl, title, found: found.size },
                { contentType: 'application/json' }
              );
              await Actor.setValue(`DEBUG_cmc_community_search_${safeMode}_${query}.html`, html || '', { contentType: 'text/html' });
              const shot = await page.screenshot({ fullPage: true }).catch(() => null);
              if (shot) await Actor.setValue(`DEBUG_cmc_community_search_${safeMode}_${query}.png`, shot, { contentType: 'image/png' });
            } catch {
              // ignore
            }

            await page.close().catch(() => null);
            return [...found];
          }

          let stableRounds = 0;
          let lastCount = 0;

          while (found.size < targetMax && stableRounds < 4) {
            const hrefs = await page.$$eval('a[href*="/community/post/"],a[href^="/community/post/"]', (as) =>
              as.map((a) => a.getAttribute('href')).filter(Boolean)
            );

            for (const href of hrefs) {
              const mm = String(href).match(/\/community\/post\/(\d+)/);
              if (mm?.[1]) found.add(mm[1]);
              if (found.size >= targetMax) break;
            }

            if (found.size === lastCount) stableRounds += 1;
            else stableRounds = 0;
            lastCount = found.size;

            await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
            await page.waitForTimeout(1500);
          }

          // If we still found nothing, persist debug artifacts so we can see what the page rendered in Apify.
          if (found.size === 0) {
            try {
              const title = await page.title().catch(() => '');
              const currentUrl = page.url();
              const html = await page.content().catch(() => '');
              await Actor.setValue(
                `DEBUG_cmc_community_search_${safeMode}_${query}_meta.json`,
                {
                  url: currentUrl,
                  title,
                  found: 0,
                  note: 'Selector(s) were present, but no /community/post/<id> hrefs were collected. Likely redirect, empty state, or bot-gate.',
                },
                { contentType: 'application/json' }
              );
              await Actor.setValue(`DEBUG_cmc_community_search_${safeMode}_${query}.html`, html || '', { contentType: 'text/html' });
              const shot = await page.screenshot({ fullPage: true }).catch(() => null);
              if (shot) await Actor.setValue(`DEBUG_cmc_community_search_${safeMode}_${query}.png`, shot, { contentType: 'image/png' });
            } catch {
              // ignore
            }
          }

          await page.close().catch(() => null);
          return [...found];
        })();

        if (postIds.length === 0) {
          log.warning('CoinMarketCap community-search returned 0 postIds; see DEBUG_* artifacts in KV store (if present).', {
            query,
            mode,
            maxPosts,
          });
        }

        for (const postId of postIds) {
          const post = await fetchCmcCommunityPost({ postIdOrUrl: postId });
          const commentsText = includeComments
            ? await fetchCmcCommunityCommentsFromBrowser({ postId, maxComments: maxCommentsPerPost, context })
            : [];

          const items = [
            post,
            ...commentsText.map((t, i) => ({
              kind: 'comment',
              id: `${postId}:${i + 1}`,
              rootId: postId,
              text: t,
            })),
          ].filter(Boolean);

          for (const item of items) {
            totalChecked += 1;
            if (totalChecked > maxItemsPerDataset) break;

            const stableId = `cmc-community:${item?.kind || 'item'}:${String(item?.id || totalChecked)}`;
            if (dedupeEnabled && alreadySeen({ state, platform, id: stableId })) continue;

            const joined = String(item?.text || '').trim();
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
                url: post?.url || null,
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
        }
      });

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



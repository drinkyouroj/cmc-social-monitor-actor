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

function collectPostIdsFromNextData(nextData) {
  const out = new Set();
  const ann = nextData?.props?.pageProps?.detailRes?.announcementNew;
  if (Array.isArray(ann)) {
    for (const a of ann) {
      const gid = a?.tweetDTO?.gravityId ?? a?.tweetDTO?.rootId;
      if (gid && String(gid).match(/^\d+$/)) out.add(String(gid));
    }
  }
  return [...out];
}

async function discoverCmcCurrencyCommunityPostIds({ coinId, currencySlugOrUrl, maxPosts } = {}) {
  await resolveCmcCoinId({ coinId, currencySlugOrUrl });
  const slug = normalizeCmcCurrencySlug(currencySlugOrUrl);
  if (!slug) throw new Error('cmc/currency-community: provide input.currencySlugOrUrl (or input.coinId).');

  const targetMax = Number.isFinite(maxPosts) ? Math.max(1, Math.min(500, maxPosts)) : 50;
  const url = `https://coinmarketcap.com/currencies/${slug}/`;

  return await runWithPlaywright(async ({ context }) => {
    const page = await context.newPage();
    const found = new Set();
    const observedFromNetwork = new Set();
    const postsById = new Map();

    page.on('response', async (res) => {
      try {
        const u = res.url();
        if (!/coinmarketcap\.com/.test(u)) return;
        const ct = String(res.headers()['content-type'] || '').toLowerCase();
        const looksJson = ct.includes('json') || /\/gravity\/|data-api|graphql/i.test(u);
        if (!looksJson) return;
        const txt = await res.text();
        collectGravityIdsFromString(txt, observedFromNetwork);
        collectGravityPostSummariesFromString(txt, postsById);
        for (const id of observedFromNetwork) {
          found.add(id);
          if (found.size >= targetMax) break;
        }
      } catch {
        // ignore
      }
    });

    await page.goto(url, { waitUntil: 'domcontentloaded' });
    await maybeAcceptCookies(page);
    await page.waitForLoadState('networkidle', { timeout: 60000 }).catch(() => null);

    // Scroll a bit to trigger the token page community module to load.
    let stableRounds = 0;
    let lastCount = postsById.size;
    while (postsById.size < targetMax && stableRounds < 4) {
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
      await page.waitForTimeout(1500);
      await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => null);
      if (postsById.size === lastCount) stableRounds += 1;
      else stableRounds = 0;
      lastCount = postsById.size;
    }

    // Extract any directly-linked community posts from the rendered DOM.
    try {
      const hrefs = await page.$$eval('a[href*="/community/post/"],a[href^="/community/post/"]', (as) =>
        as.map((a) => a.getAttribute('href')).filter(Boolean)
      );
      for (const href of hrefs) {
        const m = String(href).match(/\/community\/post\/(\d+)/);
        if (m?.[1]) found.add(m[1]);
        if (found.size >= targetMax) break;
      }
    } catch {
      // ignore
    }

    // Also extract from SSR __NEXT_DATA__ if present (announcement posts).
    try {
      const html = await page.content();
      const nextData = extractNextDataJson(html);
      const ids = collectPostIdsFromNextData(nextData);
      for (const id of ids) {
        found.add(id);
        if (found.size >= targetMax) break;
      }
    } catch {
      // ignore
    }

    await page.close().catch(() => null);
    // Prefer rich post summaries from network; fall back to IDs.
    const posts = [...postsById.values()]
      .filter((p) => p?.gravityId && typeof p?.textContent === 'string')
      .sort((a, b) => Number(b.postTime || 0) - Number(a.postTime || 0))
      .slice(0, targetMax);
    if (posts.length > 0) return posts;
    return [...found].slice(0, targetMax).map((id) => ({ gravityId: id, textContent: '', commentCount: null, postTime: null, owner: null, currencies: null, raw: null }));
  });
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

function extractCmcCommunityPostFromNextData({ nextData, postId }) {
  const queries = nextData?.props?.pageProps?.dehydratedState?.queries || [];
  const postQuery = queries.find((q) => Array.isArray(q?.queryKey) && q.queryKey[0] === 'post' && String(q.queryKey[1]) === String(postId));
  return postQuery?.state?.data?.[0] || null;
}

async function fetchCmcCommunityPostFromBrowser({ postIdOrUrl, context } = {}) {
  const postId = normalizeCmcCommunityPostId(postIdOrUrl);
  if (!postId) throw new Error('CoinMarketCap community post: missing/invalid postIdOrUrl.');
  if (!context) throw new Error('CoinMarketCap community post: missing browser context.');

  const url = `https://coinmarketcap.com/community/post/${postId}/`;
  const page = await context.newPage();
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    await maybeAcceptCookies(page);
    await page.waitForLoadState('networkidle', { timeout: 45000 }).catch(() => null);

    const html = await page.content().catch(() => '');
    const nextData = extractNextDataJson(html);
    const post = extractCmcCommunityPostFromNextData({ nextData, postId });

    if (post && typeof post === 'object') {
      const text = String(post?.textContent || '').trim();
      const owner = post?.owner || {};
      return {
        kind: 'post',
        id: String(post?.gravityId || postId),
        url,
        ownerHandle: owner?.handle || null,
        ownerGuid: owner?.guid || null,
        createdAt: post?.postTime ? new Date(Number(post.postTime)).toISOString() : null,
        text,
        raw: post,
      };
    }

    // Fallback: pull the first post-ish text from the rendered page.
    const bodyText = await page
      .locator('body')
      .innerText()
      .then((t) => String(t || '').trim())
      .catch(() => '');

    // Heuristic: the post content usually appears near the top; keep it bounded.
    const text = bodyText.split('\n').map((s) => s.trim()).filter(Boolean).slice(0, 60).join('\n').trim();

    // Save debug artifacts to KV store to understand what the post page served.
    try {
      await Actor.setValue(`DEBUG_cmc_community_post_${postId}.html`, html || '', { contentType: 'text/html' });
      const shot = await page.screenshot({ fullPage: true }).catch(() => null);
      if (shot) await Actor.setValue(`DEBUG_cmc_community_post_${postId}.png`, shot, { contentType: 'image/png' });
    } catch {
      // ignore
    }

    if (!text) throw new Error('CoinMarketCap community post: could not extract post data from page.');

    return {
      kind: 'post',
      id: String(postId),
      url,
      ownerHandle: null,
      ownerGuid: null,
      createdAt: null,
      text,
      raw: { fallback: true },
    };
  } finally {
    await page.close().catch(() => null);
  }
}

async function fetchCmcCommunityPost({ postIdOrUrl, context } = {}) {
  const postId = normalizeCmcCommunityPostId(postIdOrUrl);
  if (!postId) throw new Error('CoinMarketCap community post: missing/invalid postIdOrUrl.');

  if (context) return await fetchCmcCommunityPostFromBrowser({ postIdOrUrl: postId, context });

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
  const post = extractCmcCommunityPostFromNextData({ nextData, postId });
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
    'button:has-text("Accept cookies")',
    'button:has-text("Accept Cookies")',
    'button:has-text("Allow all")',
    'button:has-text("Allow All")',
    'button:has-text("I Accept")',
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

function collectGravityIdsFromString(str, outSet) {
  if (!str) return;
  const text = String(str);
  // Fast path: regex scan.
  const re = /\"gravityId\"\s*:\s*\"?(\d{6,})\"?/g;
  let m;
  while ((m = re.exec(text))) outSet.add(m[1]);

  // If it looks like JSON, try to parse and walk to catch other shapes (numbers, nested objects).
  const trimmed = text.trim();
  if (!(trimmed.startsWith('{') || trimmed.startsWith('['))) return;
  try {
    const j = JSON.parse(trimmed);
    const stack = [j];
    while (stack.length) {
      const cur = stack.pop();
      if (!cur || typeof cur !== 'object') continue;
      if (Array.isArray(cur)) {
        for (const v of cur) stack.push(v);
        continue;
      }
      for (const [k, v] of Object.entries(cur)) {
        if (k === 'gravityId') {
          const id = typeof v === 'number' ? String(v) : typeof v === 'string' ? v : null;
          if (id && /^\d{6,}$/.test(id)) outSet.add(id);
        }
        if (v && typeof v === 'object') stack.push(v);
      }
    }
  } catch {
    // ignore parse failures
  }
}

function collectGravityPostSummariesFromJson(obj, outById) {
  const stack = [obj];
  while (stack.length) {
    const cur = stack.pop();
    if (!cur || typeof cur !== 'object') continue;
    if (Array.isArray(cur)) {
      for (const v of cur) stack.push(v);
      continue;
    }

    const gid = cur.gravityId;
    const text = cur.textContent;
    if ((typeof gid === 'string' || typeof gid === 'number') && typeof text === 'string') {
      const id = typeof gid === 'number' ? String(gid) : gid;
      if (/^\d{6,}$/.test(id)) {
        outById.set(id, {
          gravityId: id,
          textContent: text,
          commentCount: cur.commentCount ?? null,
          postTime: cur.postTime ?? null,
          owner: cur.owner ?? null,
          currencies: cur.currencies ?? null,
          raw: cur,
        });
      }
    }

    for (const v of Object.values(cur)) if (v && typeof v === 'object') stack.push(v);
  }
}

function collectGravityPostSummariesFromString(str, outById) {
  const text = String(str || '').trim();
  if (!(text.startsWith('{') || text.startsWith('['))) return;
  try {
    const j = JSON.parse(text);
    collectGravityPostSummariesFromJson(j, outById);
  } catch {
    // ignore
  }
}

async function discoverCmcCommunityPostIdsByQuery({ query, mode, maxPosts, context } = {}) {
  const q = String(query || '').trim();
  if (!q) return [];

  const m = String(mode || 'latest').trim().toLowerCase();
  const safeMode = m === 'top' ? 'top' : 'latest';
  const targetMax = Number.isFinite(maxPosts) ? Math.max(1, Math.min(500, maxPosts)) : 50;

  const startUrl = `https://coinmarketcap.com/community/search/${safeMode}/${encodeURIComponent(q)}`;

  const runInContext = async (ctx) => {
    const page = await context.newPage();
    const found = new Set();

    page.setDefaultTimeout(60000);
    const observedFromNetwork = new Set();
    const debugNetworkUrls = [];

    // Capture post IDs from API responses even if the UI doesn't render anchor tags.
    page.on('response', async (res) => {
      try {
        const url = res.url();
        if (!/coinmarketcap\.com/.test(url)) return;
        const ct = String(res.headers()['content-type'] || '').toLowerCase();
        // Some endpoints reply with text/plain or application/*json.
        const looksJson = ct.includes('json') || /\/gravity\/|data-api|graphql/i.test(url);
        if (!looksJson) return;
        if (/\/gravity\//i.test(url) && debugNetworkUrls.length < 200) debugNetworkUrls.push(url);

        const txt = await res.text();
        collectGravityIdsFromString(txt, observedFromNetwork);
        // Merge into found (bounded)
        for (const id of observedFromNetwork) {
          found.add(id);
          if (found.size >= targetMax) break;
        }
      } catch {
        // ignore
      }
    });

    await page.goto(startUrl, { waitUntil: 'domcontentloaded' });
    await maybeAcceptCookies(page);
    // Give client-side app time to hydrate/fetch.
    await page.waitForLoadState('networkidle', { timeout: 60000 }).catch(() => null);

    // If CoinMarketCap redirects /community/search/* back to /community/, use the in-page search UI.
    const landedUrl = page.url();
    if (!/\/community\/search\//.test(landedUrl)) {
      // Introspect inputs to reliably find the community "Search posts or users" box.
      const inputs = await page.evaluate(() => {
        const isVisible = (el) => {
          const r = el.getBoundingClientRect();
          if (!r || r.width < 20 || r.height < 10) return false;
          const style = window.getComputedStyle(el);
          if (!style || style.visibility === 'hidden' || style.display === 'none' || Number(style.opacity || '1') === 0) return false;
          return true;
        };
        const out = [];
        for (const el of Array.from(document.querySelectorAll('input'))) {
          const closestOneTrust = el.closest('#onetrust-consent-sdk');
          out.push({
            id: el.id || null,
            name: el.getAttribute('name') || null,
            type: el.getAttribute('type') || null,
            placeholder: el.getAttribute('placeholder') || null,
            ariaLabel: el.getAttribute('aria-label') || null,
            role: el.getAttribute('role') || null,
            inOneTrust: !!closestOneTrust,
            visible: isVisible(el),
          });
        }
        return out;
      });

      const candidates = inputs
        .filter((i) => i.visible && !i.inOneTrust && i.id !== 'vendor-search-handler')
        .map((i) => ({
          ...i,
          text: `${i.placeholder || ''} ${i.ariaLabel || ''}`.toLowerCase(),
        }));

      const pick =
        candidates.find((c) => c.text.includes('posts') && c.text.includes('users')) ||
        candidates.find((c) => c.text.includes('posts')) ||
        candidates.find((c) => c.text.includes('users')) ||
        candidates.find((c) => (c.type || '').toLowerCase() === 'search') ||
        candidates[0] ||
        null;

      if (pick) {
        try {
          // Locate by id if possible, otherwise by placeholder/aria-label.
          let loc = null;
          if (pick.id) loc = page.locator(`#${CSS.escape(pick.id)}`).first();
          else if (pick.placeholder) loc = page.locator(`input[placeholder=${JSON.stringify(pick.placeholder)}]`).first();
          else if (pick.ariaLabel) loc = page.locator(`input[aria-label=${JSON.stringify(pick.ariaLabel)}]`).first();

          if (loc) {
            await loc.click({ timeout: 5000 });
            await loc.fill(q);
            // Some UIs search on input, some on Enter.
            await loc.press('Enter').catch(() => null);
            await page.waitForTimeout(1000);
            await page.waitForLoadState('networkidle', { timeout: 60000 }).catch(() => null);
            await page.waitForTimeout(1500);
          }
        } catch {
          // ignore; we'll still attempt HTML/network extraction
        }
      }
    }

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
        const bodyText = await page
          .locator('body')
          .innerText()
          .then((t) => String(t || '').trim())
          .catch(() => '');
        const snippet = bodyText.slice(0, 240).replace(/\s+/g, ' ').trim();

        log.warning('CoinMarketCap community-search page did not show post links; using HTML fallback', {
          query: q,
          safeMode,
          currentUrl: url,
          title,
          snippet,
          htmlLen: html.length,
          extractedIds: found.size,
          hasPostPath: /\/community\/post\/\d+/.test(html),
          looksLikeCaptcha: /captcha|cloudflare|attention required/i.test(title + ' ' + snippet),
          looksLikeAccessDenied: /access denied|forbidden|blocked/i.test(title + ' ' + snippet),
        });

        const meta = { url, title, found: found.size, snippet, htmlLen: html.length, extractedIds: found.size };
        await Actor.setValue(
          `DEBUG_cmc_community_search_${safeMode}_${q}_meta.json`,
          JSON.stringify(meta, null, 2),
          { contentType: 'application/json' }
        );
        await Actor.setValue(`DEBUG_cmc_community_search_${safeMode}_${q}.html`, html || '', { contentType: 'text/html' });
        const shot = await page.screenshot({ fullPage: true }).catch(() => null);
        if (shot) await Actor.setValue(`DEBUG_cmc_community_search_${safeMode}_${q}.png`, shot, { contentType: 'image/png' });
      } catch (e) {
        log.exception(e, 'Failed to persist community-search debug artifacts');
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
  };

  if (context) return await runInContext(context);
  return await runWithPlaywright(async ({ context: ctx }) => runInContext(ctx));
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

function getRunTimingInfo({ assumeTimeoutSecs } = {}) {
  try {
    const env = Actor.getEnv();
    const now = Date.now();

    const timeoutAtMs = env?.timeoutAt ? new Date(env.timeoutAt).getTime() : null;
    if (timeoutAtMs && !Number.isNaN(timeoutAtMs)) {
      return {
        source: 'timeoutAt',
        timeoutAt: env.timeoutAt,
        startedAt: env?.startedAt || null,
        timeoutSecs: env?.timeoutSecs ?? null,
        remainingMs: timeoutAtMs - now,
      };
    }

    // Fallback: compute using startedAt + timeoutSecs (or an explicit override).
    const startedAtMs = env?.startedAt ? new Date(env.startedAt).getTime() : null;
    const timeoutSecs =
      Number.isFinite(assumeTimeoutSecs) ? assumeTimeoutSecs : Number.isFinite(env?.timeoutSecs) ? env.timeoutSecs : null;
    if (startedAtMs && !Number.isNaN(startedAtMs) && timeoutSecs != null) {
      const timeoutAt = new Date(startedAtMs + timeoutSecs * 1000).toISOString();
      return {
        source: Number.isFinite(assumeTimeoutSecs) ? 'assumeTimeoutSecs' : 'startedAt+timeoutSecs',
        timeoutAt,
        startedAt: env.startedAt,
        timeoutSecs,
        remainingMs: startedAtMs + timeoutSecs * 1000 - now,
      };
    }

    return {
      source: 'unknown',
      timeoutAt: env?.timeoutAt || null,
      startedAt: env?.startedAt || null,
      timeoutSecs: env?.timeoutSecs ?? null,
      remainingMs: null,
    };
  } catch {
    return { source: 'error', timeoutAt: null, startedAt: null, timeoutSecs: null, remainingMs: null };
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

    if (actorId === 'cmc/currency-community') {
      log.info('Fetching CoinMarketCap Currency Community Posts', { platform, actorId });

      // v0.2-dev: configurable "X most recent posts/comments" aliases
      const maxPosts = Number.isFinite(actorInput?.maxRecentPosts)
        ? actorInput.maxRecentPosts
        : Number.isFinite(actorInput?.maxMostRecentPosts)
          ? actorInput.maxMostRecentPosts
          : Number.isFinite(actorInput?.maxPosts)
            ? actorInput.maxPosts
            : 50;
      const includeComments = actorInput?.includeComments === true;
      const maxCommentsPerPost = Number.isFinite(actorInput?.maxRecentCommentsPerPost)
        ? actorInput.maxRecentCommentsPerPost
        : Number.isFinite(actorInput?.maxMostRecentCommentsPerPost)
          ? actorInput.maxMostRecentCommentsPerPost
          : Number.isFinite(actorInput?.maxCommentsPerPost)
            ? actorInput.maxCommentsPerPost
            : 50;
      // Comment scraping is the slowest part. Keep the default small so runs fit the 360s default timeout.
      const maxPostsWithComments = Number.isFinite(actorInput?.maxPostsWithComments) ? actorInput.maxPostsWithComments : 2;
      // How much runtime must be left to start scraping comments.
      const minRemainingMsForCommentScrape = Number.isFinite(actorInput?.minRemainingMsForCommentScrape)
        ? actorInput.minRemainingMsForCommentScrape
        : 90_000;
      // Optional override if Actor.getEnv().timeoutAt does not reflect your configured timeout.
      const assumeTimeoutSecs = Number.isFinite(actorInput?.assumeTimeoutSecs) ? actorInput.assumeTimeoutSecs : null;

      const postsOrIds = await discoverCmcCurrencyCommunityPostIds({
        coinId: actorInput?.coinId,
        currencySlugOrUrl: actorInput?.currencySlugOrUrl,
        maxPosts,
      });

      let totalChecked = 0;
      let totalEmitted = 0;
      let postsWithCommentsFetched = 0;

      log.info('Discovered token community posts', {
        platform,
        count: Array.isArray(postsOrIds) ? postsOrIds.length : 0,
        includeComments,
        maxPostsWithComments,
        minRemainingMsForCommentScrape,
      });

      // Reuse one browser context for comments scraping.
      await runWithPlaywright(async ({ context }) => {
        for (const entry of postsOrIds) {
          const postId = typeof entry === 'string' ? entry : entry?.gravityId;
          if (!postId) continue;

          const postText = typeof entry === 'object' ? String(entry?.textContent || '').trim() : '';
          const postUrl = `https://coinmarketcap.com/community/post/${postId}/`;

          // Emit the post itself using the summary text (fast, no post page needed).
          const postItem = {
            kind: 'post',
            id: String(postId),
            url: postUrl,
            text: postText,
            raw: entry,
          };

          // Emit the post first; comments are fetched later (bounded by maxPostsWithComments).
          const items = [postItem];

          for (const item of items) {
            totalChecked += 1;
            if (totalChecked > maxItemsPerDataset) break;

            const stableId = `cmc-community:${platform}:${item?.kind || 'item'}:${String(item?.id || totalChecked)}`;
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
                url: postUrl,
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

          // Fetch comments only for a limited number of matched posts (keeps runtime bounded).
          if (
            includeComments &&
            postsWithCommentsFetched < maxPostsWithComments &&
            (typeof entry?.commentCount === 'number' ? entry.commentCount : 1) > 0
          ) {
            // Only fetch if the post itself matched (otherwise we'd waste time).
            if (postText && symbolsRegex.test(postText)) {
              const { isMatch } = matchTextAgainstSymbols({
                text: postText,
                symbols,
                caseInsensitive,
                useWordBoundaries,
              });
              if (isMatch) {
                const timing = getRunTimingInfo({ assumeTimeoutSecs });
                const remaining = timing?.remainingMs;
                // If we're close to timeout, skip further comment scraping.
                if (remaining != null && remaining < minRemainingMsForCommentScrape) {
                  log.warning('Skipping comment scraping due to low remaining runtime', {
                    remainingMs: remaining,
                    minRemainingMsForCommentScrape,
                    timingSource: timing?.source,
                    timeoutAt: timing?.timeoutAt,
                    timeoutSecs: timing?.timeoutSecs,
                    postId,
                  });
                } else {
                const commentsText = await fetchCmcCommunityCommentsFromBrowser({ postId, maxComments: maxCommentsPerPost, context });
                postsWithCommentsFetched += 1;
                for (let i = 0; i < commentsText.length; i++) {
                  const text = String(commentsText[i] || '').trim();
                  if (!text) continue;
                  totalChecked += 1;
                  if (totalChecked > maxItemsPerDataset) break;

                  const stableId = `cmc-community:${platform}:comment:${postId}:${i + 1}`;
                  if (dedupeEnabled && alreadySeen({ state, platform, id: stableId })) continue;
                  if (!symbolsRegex.test(text)) {
                    rememberSeenId({ state, platform, id: stableId, maxSeenIdsPerPlatform });
                    continue;
                  }

                  const { isMatch: cm, matchedSymbols } = matchTextAgainstSymbols({
                    text,
                    symbols,
                    caseInsensitive,
                    useWordBoundaries,
                  });
                  if (!cm) {
                    rememberSeenId({ state, platform, id: stableId, maxSeenIdsPerPlatform });
                    continue;
                  }

                  const out = {
                    platform,
                    stableId,
                    matchedAt: new Date().toISOString(),
                    matchedSymbols,
                    text,
                    source: { actorId, runId: null, datasetId: null, url: postUrl },
                    raw: { kind: 'comment', id: `${postId}:${i + 1}`, rootId: postId, text },
                  };
                  await Actor.pushData(out);
                  totalEmitted += 1;
                  rememberSeenId({ state, platform, id: stableId, maxSeenIdsPerPlatform });
                }
                }
              }
            }
          }

          if (totalChecked > maxItemsPerDataset) break;
        }
      });

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
        const postIds = await discoverCmcCommunityPostIdsByQuery({ query, mode, maxPosts, context });

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



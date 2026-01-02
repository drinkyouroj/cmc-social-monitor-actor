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
    throw new Error('Invalid configuration: provide match.symbols (e.g. ["DGRAM"]).');
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

    log.info('Running scraper Actor', { platform, actorId });
    const run = await client.actor(actorId).call(actorInput);
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



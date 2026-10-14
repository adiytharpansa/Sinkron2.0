#!/usr/bin/env node
/**
 * Sinkron2.0 — sinkron otomatis my-anime, dijalankan dari
 * GitHub Actions (bukan lagi dari VM / Cloudflare Worker).
 *
 * Tugasnya sama persis dengan auto_sync.py lama:
 *   - hianimes.se    : sitemap /watch/ -> MAL id -> embed Zoko
 *                      (konfigurasi __P didekode) -> m3u8 asli
 *   - gogoanime.or.at: sitemap WordPress -> halaman episode ->
 *                      MegaPlay (AES-256-CBC) -> m3u8 asli
 *   - 9anime.or.at   : sitemap WordPress -> AJAX server ->
 *                      embed Plyr -> redirect -> MP4 langsung
 * lalu episode baru dikirim ke API aplikasi
 * (POST https://my-anime-app.adiyth-arpansa.workers.dev/api/anime)
 * yang menyimpannya per judul ke katalog live.
 *
 * Kenapa bukan upsert langsung ke Supabase untuk katalog?
 * Situs my-anime membaca katalognya dari store aplikasi
 * (D1/KV lewat /api/anime) — tidak ada tabel anime/episodes
 * di Supabase. Menulis katalog ke Supabase saja tidak akan
 * pernah tampil di web. Supabase dipakai di sini sesuai
 * perannya: tabel sync_logs (+ sync_hashes) untuk log &
 * keadaan sinkron, ditulis dengan service role key yang
 * HANYA dibaca dari environment / GitHub Secrets.
 *
 * Hemat kuota (inkremental berbasis hash):
 *   - Keadaan katalog dibaca dari SATU GET /api/anime
 *     (ter-cache tepi 120 dtk) — ringkasannya sudah memuat
 *     episodeNumbers/status/totalEpisodes per judul, jadi
 *     TIDAK ada lagi ratusan GET detail per run (itulah yang
 *     dulu membakar kuota baca D1).
 *   - Hash sha256 per judul (id + episode + updatedAt +
 *     daftar episode di situs) disimpan di .sync-cache.json
 *     (di Actions dipertahankan antar-run via actions/cache)
 *     dan dicerminkan ke Supabase sync_hashes bila tersedia.
 *     Judul yang hash-nya sama persis dilewati tanpa satu pun
 *     pengambilan halaman.
 *   - Judul FINISHED yang episodenya sudah lengkap dilewati.
 *   - Metadata AniList di-cache dengan TTL 7 hari.
 *   - GET detail judul hanya diambil saat judul itu BENAR-
 *     BENAR akan ditambah episodenya (untuk rawTitle asli).
 *
 * Lingkungan:
 *   SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY  (opsional —
 *     hanya untuk sync_logs/sync_hashes; sinkron katalog
 *     tetap jalan tanpa keduanya, log cukup ke konsol)
 *   ANIME_API_URL   bawaan API live di atas
 *   SYNC_MAX        batas episode per run (bawaan 20, agar
 *                   muat di timeout workflow 10 menit)
 *   SYNC_GAP_MS     jeda sopan antar request (bawaan 1500)
 *
 * Bendera: --max N --dry-run --source hianime|gogoanime|9anime
 *          --limit-series N (uji) --help
 * Keluar non-nol bila run gagal fatal (katalog tak terbaca
 * atau semua sumber gagal) agar GitHub mengirim notifikasi.
 */
import { createHash, createDecipheriv } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';

const API = (process.env.ANIME_API_URL || 'https://my-anime-app.adiyth-arpansa.workers.dev/api/anime').replace(/\/$/, '');
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';
const GAP_MS = Number(process.env.SYNC_GAP_MS || 1500);
const CACHE_PATH = path.resolve(process.env.SYNC_CACHE_PATH || '.sync-cache.json');
const ANILIST_META_TTL_MS = 7 * 24 * 3600 * 1000;
const DETAIL_TTL_MS = 6 * 3600 * 1000;

const args = process.argv.slice(2);
function argValue(name, fallback) {
  const i = args.indexOf(name);
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback;
}
const MAX_EPISODES = Number(argValue('--max', process.env.SYNC_MAX || 20));
const DRY_RUN = args.includes('--dry-run');
const ONLY_SOURCE = argValue('--source', '');
const LIMIT_SERIES = Number(argValue('--limit-series', 0));
if (args.includes('--help')) {
  console.log('Pemakaian: node scripts/sync.mjs [--max N] [--dry-run] [--source hianime|gogoanime|9anime] [--limit-series N]');
  process.exit(0);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const sha256 = (text) => createHash('sha256').update(String(text)).digest('hex');
const slugify = (text) => String(text || 'anime').toLowerCase().normalize('NFKD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '') || 'anime';
const normTitle = (text) => String(text || '').toLowerCase().replace(/[^a-z0-9 ]/g, '').replace(/\s+/g, ' ').trim();

/** fetch dengan timeout + retry backoff eksponensial + jitter. */
async function fetchText(url, { timeoutMs = 30000, retries = 3, headers = {}, redirect = 'follow' } = {}) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const res = await fetch(url, {
        signal: ctrl.signal,
        redirect,
        headers: { 'User-Agent': UA, 'Accept-Language': 'en-US,en;q=0.9', ...headers },
      });
      clearTimeout(timer);
      if (res.status === 429 || res.status >= 500) throw new Error(`HTTP ${res.status}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return { text: await res.text(), url: res.url, status: res.status };
    } catch (err) {
      clearTimeout(timer);
      lastErr = err;
      if (attempt < retries) await sleep(Math.min(15000, 800 * 2 ** attempt) + Math.random() * 400);
    }
  }
  throw lastErr;
}
async function fetchJson(url, opts) {
  const { text } = await fetchText(url, opts);
  return JSON.parse(text);
}

/* ---------------- cache lokal (dipertahankan Actions/cache) ---------------- */
let cache = { hashes: {}, anilist: {}, details: {}, malByAnimeId: {} };
try {
  cache = { ...cache, ...JSON.parse(await readFile(CACHE_PATH, 'utf8')) };
} catch { /* run pertama: cache kosong itu wajar */ }
async function saveCache() {
  try { await writeFile(CACHE_PATH, JSON.stringify(cache)); } catch { /* best-effort */ }
}

/* ---------------- Supabase: sync_logs + sync_hashes ---------------- */
let supabase = null;
let syncLogId = null;
if (process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY) {
  try {
    const { createClient } = await import('@supabase/supabase-js');
    supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false, autoRefreshToken: false },
    });
  } catch (err) {
    console.warn(`[!] @supabase/supabase-js tidak termuat (${err.message}); log hanya ke konsol.`);
  }
} else {
  console.warn('[!] SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY belum diisi — sync_logs dilewati, sinkron katalog tetap jalan.');
}
async function logStart() {
  if (!supabase) return;
  try {
    const { data, error } = await supabase.from('sync_logs').insert({ status: 'running' }).select('id').single();
    if (error) throw error;
    syncLogId = data.id;
  } catch (err) { console.warn(`[!] sync_logs insert gagal: ${err.message}`); }
}
async function logFinish(stats, status, errorMessage) {
  if (!supabase) return;
  try {
    const row = {
      finished_at: new Date().toISOString(),
      titles_checked: stats.titlesChecked,
      titles_updated: stats.titlesUpdated,
      episodes_added: stats.episodesAdded,
      status,
      error: errorMessage || null,
    };
    if (syncLogId) await supabase.from('sync_logs').update(row).eq('id', syncLogId);
    else await supabase.from('sync_logs').insert(row);
    // Satu query ringan di akhir run: menjaga proyek Supabase
    // paket gratis tidak ter-pause karena dianggap menganggur.
    await supabase.from('sync_logs').select('id').limit(1);
  } catch (err) { console.warn(`[!] sync_logs update gagal: ${err.message}`); }
}
async function persistHash(animeId, hash) {
  cache.hashes[animeId] = hash;
  if (!supabase) return;
  try {
    await supabase.from('sync_hashes').upsert({ anime_id: animeId, hash, updated_at: new Date().toISOString() });
  } catch { /* tabel belum dibuat = lewati senyap */ }
}

/* ---------------- metadata: AniList (cache TTL) -> Kitsu ---------------- */
const ANILIST_QUERY_BY_MAL = `query ($id: Int) { Media(idMal: $id, type: ANIME) { id title { romaji english native } coverImage { extraLarge large } bannerImage description(asHtml: false) genres averageScore seasonYear season studios { edges { isMain node { name } } } episodes status format } }`;
const ANILIST_QUERY_BY_ID = `query ($id: Int) { Media(id: $id, type: ANIME) { idMal } }`;
const ANILIST_QUERY_SEARCH = `query ($search: String) { Media(search: $search, type: ANIME) { id title { romaji english native } coverImage { extraLarge large } bannerImage description(asHtml: false) genres averageScore seasonYear season studios { edges { isMain node { name } } } episodes status format } }`;

function shapeAniList(m, fallbackTitle) {
  if (!m) return null;
  const t = m.title || {};
  const display = t.english || t.romaji || fallbackTitle || '';
  const edges = (m.studios && m.studios.edges) || [];
  const studio = (edges.find((e) => e.isMain && e.node && e.node.name) || {}).node?.name
    || (edges[0] && edges[0].node && edges[0].node.name) || null;
  return {
    anilistId: m.id ?? null,
    title: display,
    altTitles: [t.romaji, t.native].filter((x) => x && x !== display),
    poster: (m.coverImage && (m.coverImage.extraLarge || m.coverImage.large)) || '',
    banner: m.bannerImage || '',
    synopsis: String(m.description || '').replace(/<br\s*\/?>/gi, '\n').replace(/<[^>]+>/g, '').trim(),
    genres: m.genres || [],
    score: typeof m.averageScore === 'number' ? Math.round(m.averageScore) / 10 : null,
    year: m.seasonYear ?? null,
    season: m.season ?? null,
    studio,
    totalEpisodes: m.episodes ?? null,
    status: m.status || '',
    format: m.format || null,
  };
}
async function anilistPost(query, variables) {
  const res = await fetch('https://graphql.anilist.co', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json', 'User-Agent': UA },
    body: JSON.stringify({ query, variables }),
    signal: AbortSignal.timeout(30000),
  });
  if (!res.ok) throw new Error(`AniList HTTP ${res.status}`);
  const json = await res.json();
  return (json.data || {}).Media || null;
}
async function anilistByMal(malId) {
  const key = `mal:${malId}`;
  const hit = cache.anilist[key];
  if (hit && Date.now() - hit.at < ANILIST_META_TTL_MS) return hit.meta;
  try {
    const meta = shapeAniList(await anilistPost(ANILIST_QUERY_BY_MAL, { id: Number(malId) }));
    if (meta) cache.anilist[key] = { at: Date.now(), meta };
    return meta;
  } catch { return null; }
}
async function anilistSearch(title) {
  const key = `search:${normTitle(title)}`;
  const hit = cache.anilist[key];
  if (hit && Date.now() - hit.at < ANILIST_META_TTL_MS) return hit.meta;
  try {
    const meta = shapeAniList(await anilistPost(ANILIST_QUERY_SEARCH, { search: title }), title);
    if (meta) cache.anilist[key] = { at: Date.now(), meta };
    return meta;
  } catch { return null; }
}
async function malOfAnilist(anilistId) {
  if (!anilistId) return null;
  if (cache.malByAnimeId[anilistId]) return cache.malByAnimeId[anilistId];
  try {
    const m = await anilistPost(ANILIST_QUERY_BY_ID, { id: Number(anilistId) });
    if (m && m.idMal) { cache.malByAnimeId[anilistId] = m.idMal; return m.idMal; }
  } catch { /* abaikan */ }
  return null;
}
const KITSU_SEASON = { 12: 'WINTER', 1: 'WINTER', 2: 'WINTER', 3: 'SPRING', 4: 'SPRING', 5: 'SPRING', 6: 'SUMMER', 7: 'SUMMER', 8: 'SUMMER', 9: 'FALL', 10: 'FALL', 11: 'FALL' };
async function kitsuSearch(title) {
  try {
    const data = await fetchJson(`https://kitsu.io/api/edge/anime?filter[text]=${encodeURIComponent(title)}&page[limit]=1`, {
      headers: { Accept: 'application/vnd.api+json' }, retries: 1,
    });
    const item = (data.data || [])[0];
    if (!item) return null;
    const a = item.attributes || {};
    const titles = a.titles || {};
    const candidates = [titles.en, titles.en_jp, titles.ja_jp, a.canonicalTitle].filter(Boolean);
    const tokens = (s) => new Set(String(s || '').toLowerCase().match(/[a-z0-9]+/g) || []);
    const qTokens = tokens(title);
    const longest = [...qTokens].sort((x, y) => y.length - x.length)[0] || '';
    const candTokens = new Set(candidates.flatMap((c) => [...tokens(c)]));
    if (longest && !candTokens.has(longest)) return null; // penjaga relevansi (kasus naruto -> shippuden)
    const display = titles.en || a.canonicalTitle || titles.en_jp || title;
    let genres = [];
    try {
      const g = await fetchJson(`https://kitsu.io/api/edge/anime/${item.id}/genres?page[limit]=20`, { headers: { Accept: 'application/vnd.api+json' }, retries: 0 });
      genres = (g.data || []).map((x) => x.attributes && x.attributes.name).filter(Boolean);
    } catch { /* genre kosong tidak fatal */ }
    const start = a.startDate || '';
    return {
      anilistId: null, title: display,
      altTitles: [titles.en_jp, titles.ja_jp].filter((x) => x && x !== display),
      poster: (a.posterImage && (a.posterImage.large || a.posterImage.original)) || '',
      banner: (a.coverImage && (a.coverImage.large || a.coverImage.original)) || '',
      synopsis: String(a.synopsis || '').trim(), genres,
      score: a.averageRating ? Math.round(Number(a.averageRating)) / 10 : null,
      year: /^\d{4}/.test(start) ? Number(start.slice(0, 4)) : null,
      season: KITSU_SEASON[Number(start.slice(5, 7))] || null, studio: null,
      totalEpisodes: a.episodeCount ?? null,
      status: ({ finished: 'FINISHED', current: 'RELEASING' })[a.status] || '',
      format: ({ tv: 'TV', movie: 'MOVIE', ova: 'OVA', ona: 'ONA', special: 'SPECIAL', music: 'MUSIC' })[String(a.subtype || '').toLowerCase()] || null,
    };
  } catch { return null; }
}
async function enrichNewTitle(malId, titleGuess) {
  if (malId) { const m = await anilistByMal(malId); if (m) return m; }
  if (titleGuess) { const m = await anilistSearch(titleGuess); if (m) return m; return kitsuSearch(titleGuess); }
  return null;
}

/* ---------------- dekode/ekstraksi stream (port scraper.py) ---------------- */
const ZOKO_KEY = Buffer.from('otaku-embed-v1');
function decodePlayerConfig(html) {
  const m = String(html || '').match(/window\.__P\s*=\s*"([^"]+)"/);
  if (!m) return null;
  try {
    const raw = Buffer.from(m[1], 'base64');
    const out = Buffer.alloc(raw.length);
    for (let i = 0; i < raw.length; i++) out[i] = raw[i] ^ ZOKO_KEY[i % ZOKO_KEY.length];
    const cfg = JSON.parse(out.toString('utf8'));
    return cfg && typeof cfg === 'object' ? cfg : null;
  } catch { return null; }
}
function mapZokoSubtitles(cfg) {
  const out = [];
  for (const s of (cfg && cfg.subtitles) || []) {
    if (!s || typeof s !== 'object') continue;
    const src = s.src || s.file || s.url;
    if (!src) continue;
    const label = String(s.label || '');
    const low = label.toLowerCase();
    let lang = String(s.lang || 'en').toLowerCase();
    for (const [name, code] of [['indonesia', 'id'], ['arab', 'ar'], ['french', 'fr'], ['italian', 'it'], ['portugu', 'pt'], ['russian', 'ru'], ['spanish', 'es'], ['german', 'de'], ['japan', 'ja'], ['english', 'en']]) {
      if (low.includes(name)) { lang = code; break; }
    }
    out.push({ label: label || lang.toUpperCase(), lang, url: src });
  }
  return out;
}
const MEGAPLAY_KEY = Buffer.concat([Buffer.from('i?LMTAx0Q6,:}50U'), Buffer.alloc(16)]);
const MEGAPLAY_IV = Buffer.from("W0;27ToaUpl_P%'c");
async function extractMegaplay(embedHtml, embedUrl) {
  const m = String(embedHtml || '').match(/data-id="(\d+)"/);
  if (!m || !String(embedHtml).includes('getSources')) return null;
  try {
    const u = new URL(embedUrl);
    const apiUrl = `${u.protocol}//${u.host}/stream/getSourcesNew?id=${m[1]}`;
    const data = await fetchJson(apiUrl, { headers: { Referer: embedUrl, 'X-Requested-With': 'XMLHttpRequest' }, retries: 1 });
    if (!data.enc) return null;
    const raw = Buffer.from(String(data.enc).replace(/-/g, '+').replace(/_/g, '/'), 'base64');
    const decipher = createDecipheriv('aes-256-cbc', MEGAPLAY_KEY, MEGAPLAY_IV);
    const plain = Buffer.concat([decipher.update(raw), decipher.final()]);
    return JSON.parse(plain.toString('utf8')).file || null;
  } catch { return null; }
}
async function extract9anime(pageHtml, pageUrl) {
  if (!String(pageHtml).includes('tsEp') && !String(pageHtml).includes('9animetv')) return null;
  const m = String(pageHtml).match(/episodeId\s*:\s*(\d+)/) || String(pageHtml).match(/data-active="(\d+)"/);
  if (!m) return null;
  try {
    const u = new URL(pageUrl);
    const origin = `${u.protocol}//${u.host}`;
    const data = await fetchJson(`${origin}/ajax/episode/servers/?id=${m[1]}`, { headers: { Referer: pageUrl }, retries: 1 });
    const embeds = [...String((data || {}).html || '').matchAll(/data-embed="([A-Za-z0-9+/=]+)"/g)];
    if (!embeds.length) return null;
    const embedUrl = Buffer.from(embeds[0][1], 'base64').toString('utf8');
    const { text: embedHtml } = await fetchText(embedUrl, { headers: { Referer: `${origin}/` }, retries: 1 });
    const sm = embedHtml.match(/(https?:\/\/[^\s'"]+\/stream\/[^\s'"]+)/);
    if (!sm) return null;
    // Ikuti redirect TANPA mengunduh badan berkas: MP4 di ujung
    // rantai berukuran ratusan MB — cukup URL akhirnya saja
    // (setara client.stream di scraper.py yang langsung ditutup).
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 20000);
    let finalUrl = '';
    try {
      const res = await fetch(sm[1], { signal: ctrl.signal, headers: { 'User-Agent': UA, Referer: embedUrl } });
      finalUrl = res.url;
      if (res.body) await res.body.cancel().catch(() => {});
    } finally { clearTimeout(timer); }
    if (!finalUrl.includes('.mp4')) return null;
    const titleM = String(pageHtml).match(/class="film-name[^"]*"[^>]*>([^<]+)</);
    const posterM = String(pageHtml).match(/src="([^"]+)"[^>]*class="film-poster-img"/) || String(pageHtml).match(/class="film-poster-img"[^>]*src="([^"]+)"/);
    return { streamUrl: finalUrl, embedUrl, title: titleM ? titleM[1].trim() : '', poster: posterM ? posterM[1] : '' };
  } catch { return null; }
}
const firstIframeSrc = (html, base) => {
  const m = String(html || '').match(/<iframe[^>]+src="([^"]+)"/i);
  if (!m) return '';
  try { return new URL(m[1].startsWith('//') ? `https:${m[1]}` : m[1], base).href; } catch { return ''; }
};
const ogImage = (html) => (String(html || '').match(/<meta[^>]+property="og:image"[^>]+content="([^"]+)"/i) || [])[1] || '';

/** Satu halaman episode WordPress (gogo/9anime) -> payload, port alur scraper.py.
 *  enrich=false untuk judul yang sudah ada (metadatanya tidak
 *  perlu diambil ulang — hemat panggilan AniList/Kitsu). */
async function scrapeWpEpisode(pageUrl, title, ep, { enrich = true } = {}) {
  const { text: html } = await fetchText(pageUrl, { retries: 2 });
  const thumbnail = ogImage(html);
  const nine = await extract9anime(html, pageUrl);
  if (nine) {
    const name = nine.title || title;
    return {
      rawTitle: name, episodeNum: ep, thumbnail: nine.poster || thumbnail,
      streamUrl: nine.streamUrl, downloadUrl: nine.streamUrl,
      headers: { Referer: nine.embedUrl }, subtitles: [],
      meta: enrich ? await enrichNewTitle(null, name) : null,
    };
  }
  let cfg = decodePlayerConfig(html);
  let embedUrl = pageUrl;
  let embedHtml = html;
  if (!cfg) {
    embedUrl = firstIframeSrc(html, pageUrl);
    if (!embedUrl) throw new Error('Iframe player tidak ditemukan.');
    embedHtml = (await fetchText(embedUrl, { headers: { Referer: pageUrl }, retries: 2 })).text;
    const inner = firstIframeSrc(embedHtml, embedUrl);
    if (inner) {
      try {
        const innerHtml = (await fetchText(inner, { headers: { Referer: embedUrl }, retries: 1 })).text;
        if (innerHtml) { embedUrl = inner; embedHtml = innerHtml; }
      } catch { /* tetap pakai embed luar */ }
    }
    cfg = decodePlayerConfig(embedHtml);
  }
  let streamUrl = '';
  let downloadUrl = null;
  if (cfg && cfg.src) {
    streamUrl = cfg.src;
    if (cfg.download_url) { try { downloadUrl = new URL(cfg.download_url, embedUrl).href; } catch { /* abaikan */ } }
  }
  if (!streamUrl) streamUrl = (await extractMegaplay(embedHtml, embedUrl)) || '';
  if (!streamUrl) {
    const m = embedHtml.match(/(https?:\/\/[^\s'"]+\.m3u8[^\s'"]*)/);
    if (!m) throw new Error('Link stream gagal diekstrak dari embed.');
    streamUrl = m[1];
  }
  if (downloadUrl && (!/^https?:\/\//.test(downloadUrl) || downloadUrl.endsWith('.apk'))) downloadUrl = null;
  return {
    rawTitle: title, episodeNum: ep, thumbnail, streamUrl, downloadUrl,
    headers: { Referer: embedUrl }, subtitles: mapZokoSubtitles(cfg),
    meta: enrich ? await enrichNewTitle(null, title) : null,
  };
}

/* ---------------- katalog live (sumber keadaan inkremental) ---------------- */
async function loadCatalog() {
  const data = await fetchJson(`${API}?fresh=1`, { timeoutMs: 60000, retries: 2 });
  if (!data || !Array.isArray(data.data)) throw new Error('Katalog API tidak berbentuk daftar.');
  return data.data;
}
async function loadDetail(id) {
  const hit = cache.details[id];
  if (hit && Date.now() - hit.at < DETAIL_TTL_MS) return hit.data;
  const data = await fetchJson(`${API}?id=${encodeURIComponent(id)}&fresh=1`, { timeoutMs: 45000, retries: 1 });
  const detail = (data && data.data) || null;
  if (detail) cache.details[id] = { at: Date.now(), data: detail };
  return detail;
}
async function apiPost(payload) {
  if (DRY_RUN) { console.log(`  [dry-run] POST ${payload.rawTitle} ep${payload.episodeNum}`); return { ok: true, dry: true }; }
  try {
    const res = await fetch(API, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'User-Agent': UA },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(60000),
    });
    const out = await res.json().catch(() => ({}));
    if (res.status === 422) return { ok: false, error: out.error || 'HTTP 422', rejected: true };
    if (!res.ok || !out.ok) return { ok: false, error: out.error || `HTTP ${res.status}` };
    return out;
  } catch (err) { return { ok: false, error: err.message }; }
}

const stats = { titlesChecked: 0, titlesUpdated: 0, episodesAdded: 0 };
let budgetLeft = MAX_EPISODES;
const addedLabels = [];
async function take(label) {
  budgetLeft -= 1;
  stats.episodesAdded += 1;
  addedLabels.push(label);
  console.log(`DITAMBAH ${label}`);
  await sleep(GAP_MS);
}

/* ---------------- hianimes.se ---------------- */
async function syncHianime(catalog) {
  console.log('=== hianimes.se ===');
  const sitemap = (await fetchText('https://hianimes.se/sitemap.xml', { timeoutMs: 60000 })).text;
  const watchUrls = [...sitemap.matchAll(/<loc>(https:\/\/hianimes\.se\/watch\/[^<]+)<\/loc>/g)].map((m) => m[1]);
  const site = new Map();
  for (const url of watchUrls) {
    const slug = url.split('/watch/')[1] || '';
    const m = slug.match(/^(.+)-episode-(\d+)-/);
    if (!m || m[1].includes('hentai')) continue;
    if (!site.has(m[1])) site.set(m[1], { eps: new Set(), url });
    site.get(m[1]).eps.add(Number(m[2]));
  }
  const byTitle = new Map();
  const byId = new Map(catalog.map((a) => [a.id, a]));
  for (const a of catalog) byTitle.set(normTitle(a.title), a);
  const match = (sslug) => byTitle.get(normTitle(sslug.replace(/-/g, ' ')));
  const items = [...site.entries()].sort((a, b) => {
    const ma = match(a[0]); const mb = match(b[0]);
    if (!ma && !mb) return 0;
    if (!ma) return 1;
    if (!mb) return -1;
    return (ma.episodeNumbers || []).length - (mb.episodeNumbers || []).length;
  });
  let seriesSeen = 0;
  for (const [sslug, info] of items) {
    if (budgetLeft <= 0) break;
    if (LIMIT_SERIES && seriesSeen >= LIMIT_SERIES) break;
    seriesSeen += 1;
    stats.titlesChecked += 1;
    const anime = match(sslug);
    const siteEps = [...info.eps].sort((x, y) => x - y);
    if (anime) {
      // Hash inkremental: judul sama + episode situs sama +
      // updatedAt katalog sama = tidak ada yang berubah.
      const fingerprint = sha256(`${anime.id}|${anime.updatedAt || ''}|${siteEps.join(',')}|${(anime.episodeNumbers || []).join(',')}`);
      if (cache.hashes[anime.id] === fingerprint) continue;
      const have = new Set(anime.episodeNumbers || []);
      const missing = siteEps.filter((n) => !have.has(n));
      const finished = String(anime.status || '').toUpperCase() === 'FINISHED'
        || (anime.totalEpisodes && have.size >= anime.totalEpisodes);
      if (finished && missing.length === 0) { await persistHash(anime.id, fingerprint); continue; }
      if (!missing.length) { await persistHash(anime.id, fingerprint); continue; }
      const detail = await loadDetail(anime.id);
      if (!detail) continue;
      if (!(detail.episodes || []).some((e) => String(e.streamUrl).includes('dramahot'))) continue; // sumber campuran: jangan campur
      let mal = cache.malByAnimeId[anime.id] || null;
      if (!mal && detail.anilistId) mal = await malOfAnilist(detail.anilistId);
      if (!mal) continue;
      let updated = false;
      for (const ep of missing) {
        if (budgetLeft <= 0) break;
        const embed = `https://zokoanime.video/stream/mal/${mal}/${ep}/sub`;
        const cfg = decodePlayerConfig((await fetchText(embed, { retries: 1 })).text);
        const src = cfg && cfg.src;
        if (!src) continue;
        const out = await apiPost({
          rawTitle: detail.rawTitle, episodeNum: ep, streamUrl: src,
          downloadUrl: cfg.download_url || null, headers: { Referer: embed },
          subtitles: mapZokoSubtitles(cfg), meta: null,
        });
        if (out.ok) { await take(`${anime.title} ep${ep} (hianime)`); updated = true; }
        else console.log(`  gagal POST ${anime.title} ep${ep}: ${out.error || ''}`);
      }
      if (updated) stats.titlesUpdated += 1;
      await persistHash(anime.id, fingerprint);
    } else {
      const titleGuess = sslug.replace(/-/g, ' ').replace(/\s+/g, ' ').trim();
      const html = (await fetchText(info.url, { retries: 1 })).text;
      const m = html.match(/zokoanime\.video\/stream\/mal\/(\d+)\//);
      if (!m) continue;
      const mal = m[1];
      const meta = await enrichNewTitle(mal, titleGuess);
      if (!meta || !meta.poster) { console.log(`  tunda judul baru ${titleGuess}: thumbnail belum tersedia`); continue; }
      let updated = false;
      for (const ep of siteEps.slice(0, 3)) {
        if (budgetLeft <= 0) break;
        const embed = `https://zokoanime.video/stream/mal/${mal}/${ep}/sub`;
        const cfg = decodePlayerConfig((await fetchText(embed, { retries: 1 })).text);
        const src = cfg && cfg.src;
        if (!src) break;
        const out = await apiPost({
          rawTitle: titleGuess, episodeNum: ep, streamUrl: src,
          downloadUrl: cfg.download_url || null, headers: { Referer: embed },
          subtitles: mapZokoSubtitles(cfg), meta,
        });
        if (out.ok) { await take(`${meta.title || titleGuess} ep${ep} (hianime, BARU)`); updated = true; }
        else break;
      }
      if (updated) stats.titlesUpdated += 1;
    }
  }
}

/* ---------------- WordPress: gogoanime / 9anime ---------------- */
async function wpEpisodeMap(home, pattern) {
  const rx = new RegExp(pattern);
  const urls = new Set();
  const index = (await fetchText(`${home}/sitemap.xml`, { timeoutMs: 60000 })).text;
  const subs = [...index.matchAll(/<loc>([^<]*sitemap[^<]*\.xml)<\/loc>/g)].map((m) => m[1]);
  for (const sm of subs.length ? subs : [`${home}/sitemap.xml`]) {
    const body = sm === `${home}/sitemap.xml` && subs.length === 0 ? index : (await fetchText(sm, { timeoutMs: 60000, retries: 1 })).text;
    for (const m of body.matchAll(/<loc>([^<]+)<\/loc>/g)) urls.add(m[1]);
  }
  const series = new Map();
  for (const u of urls) {
    const m = u.match(rx);
    if (m) {
      if (!series.has(m[1])) series.set(m[1], {});
      series.get(m[1])[Number(m[2])] = u;
    }
  }
  if (series.size === 0) {
    const body = (await fetchText(`${home}/`, { retries: 1 })).text;
    for (const m of body.matchAll(/href="([^"]+)"/g)) {
      const mm = m[1].match(rx);
      if (mm) {
        if (!series.has(mm[1])) series.set(mm[1], {});
        series.get(mm[1])[Number(mm[2])] = m[1];
      }
    }
  }
  return series;
}
async function syncWp(name, home, pattern, hostMarker, catalog) {
  console.log(`=== ${name} ===`);
  const site = await wpEpisodeMap(home, pattern);
  const bySlug = new Map(catalog.map((a) => [a.id, a]));
  const ordered = [...site.entries()].sort((a, b) => {
    const ma = bySlug.get(a[0]); const mb = bySlug.get(b[0]);
    if (!ma && !mb) return 0;
    if (!ma) return 1;
    if (!mb) return -1;
    return (ma.episodeNumbers || []).length - (mb.episodeNumbers || []).length;
  });
  let seriesSeen = 0;
  for (const [sslug, eps] of ordered) {
    if (budgetLeft <= 0) break;
    if (LIMIT_SERIES && seriesSeen >= LIMIT_SERIES) break;
    seriesSeen += 1;
    if (sslug.includes('hentai')) continue;
    stats.titlesChecked += 1;
    const epNums = Object.keys(eps).map(Number);
    const template = eps[Math.max(...epNums)];
    const urlFor = (n) => eps[n] || template.replace(/episode-\d+/, `episode-${n}`);
    const anime = bySlug.get(sslug);
    if (anime) {
      const have = new Set(anime.episodeNumbers || []);
      const targets = [];
      for (let n = 1; n <= Math.max(...epNums); n++) if (!have.has(n)) targets.push(n);
      if (!targets.length) continue;
      const detail = await loadDetail(anime.id);
      if (!detail) continue;
      if ((detail.episodes || []).length && !(detail.episodes || []).some((e) => String(e.streamUrl).includes(hostMarker))) continue;
      let updated = false;
      for (const ep of targets) {
        if (budgetLeft <= 0) break;
        try {
          const payload = await scrapeWpEpisode(urlFor(ep), detail.rawTitle, ep, { enrich: false });
          payload.rawTitle = detail.rawTitle;
          payload.meta = null; // judul lama: metadata server dipertahankan
          const out = await apiPost(payload);
          if (out.ok) { await take(`${anime.title} ep${ep} (${name})`); updated = true; }
          else console.log(`  gagal POST ${anime.title} ep${ep}: ${out.error || ''}`);
        } catch (err) { console.log(`  gagal ${name} ${anime.title} ep${ep}: ${err.message}`); await sleep(3000); }
      }
      if (updated) stats.titlesUpdated += 1;
    } else {
      const title = sslug.replace(/-/g, ' ').replace(/\s+/g, ' ').trim();
      for (let ep = 1; ep <= 2; ep++) {
        if (budgetLeft <= 0) break;
        try {
          const payload = await scrapeWpEpisode(urlFor(ep), title, ep);
          if (!(payload.meta && payload.meta.poster) && !payload.thumbnail) {
            console.log(`  tunda judul baru ${title}: thumbnail belum tersedia`);
            break;
          }
          const out = await apiPost(payload);
          if (out.ok) { await take(`${title} ep${ep} (${name}, BARU)`); stats.titlesUpdated += 1; }
          else break;
        } catch { break; }
      }
    }
  }
}

/* ---------------- main ---------------- */
async function main() {
  console.log(`--- Sinkron2.0 mulai (batas ${MAX_EPISODES} episode${DRY_RUN ? ', dry-run' : ''}) ---`);
  await logStart();
  const catalog = await loadCatalog();
  console.log(`katalog saat ini: ${catalog.length} judul`);
  const sources = [
    ['hianime', () => syncHianime(catalog)],
    ['gogoanime', () => syncWp('gogoanime', 'https://gogoanime.or.at', '/([a-z0-9-]+)-episode-(\\d+)/?$', 'nexabloom', catalog)],
    ['9anime', () => syncWp('9anime', 'https://9anime.or.at', '/([a-z0-9-]+)-episode-(\\d+)-english-subbed/?$', '1anime', catalog)],
  ];
  let sourcesOk = 0;
  let lastError = null;
  for (const [name, run] of sources) {
    if (ONLY_SOURCE && ONLY_SOURCE !== name) continue;
    if (budgetLeft <= 0) break;
    try { await run(); sourcesOk += 1; }
    catch (err) { lastError = err; console.error(`[!] Sumber ${name} gagal: ${err.message}`); }
  }
  await saveCache();
  console.log(`--- Sinkron2.0 selesai: ${stats.episodesAdded} episode ditambahkan ---`);
  for (const label of addedLabels) console.log(`  + ${label}`);
  if (sourcesOk === 0) throw new Error(lastError ? lastError.message : 'semua sumber gagal');
}

try {
  await main();
  await logFinish(stats, 'success', null);
} catch (err) {
  console.error(`[x] Sinkron gagal fatal: ${err.message}`);
  await saveCache().catch(() => {});
  await logFinish(stats, 'failed', err.message);
  process.exit(1);
}

#!/usr/bin/env node
/**
 * Sinkron2.0 — sinkron otomatis my-anime, dijalankan dari
 * GitHub Actions (bukan dari VM / Cloudflare Worker). Versi 2:
 * ter-shard + paralel + dua mode (fast/full).
 *
 * Sumber (satu-satunya yang terverifikasi hidup):
 *   - hianimes.se    : sitemap /watch/ -> MAL id -> embed Zoko
 *                      (konfigurasi __P didekode) -> m3u8 asli
 *   - gogoanime.or.at: sitemap WordPress -> halaman episode ->
 *                      MegaPlay (AES-256-CBC) -> m3u8 asli
 *   - 9anime.or.at   : sitemap WordPress -> AJAX server ->
 *                      embed Plyr -> redirect -> MP4 langsung
 * Metadata (isi celah, penjaga relevansi token): AniList ->
 * Kitsu -> Bangumi (poster Asia/donghua) -> TVmaze (sinopsis
 * Inggris/data episode) -> Shikimori (basis MAL, poster).
 * Episode baru dikirim ke API aplikasi
 * (POST https://my-anime-app.adiyth-arpansa.workers.dev/api/anime)
 * yang menyimpannya per judul ke katalog live (D1/KV aplikasi).
 * Supabase HANYA untuk sync_logs + sync_hashes (service role
 * key dibaca dari environment / GitHub Secrets saja) — katalog
 * tidak punya tabel di Supabase, situs membacanya dari store
 * aplikasi lewat /api/anime.
 *
 * Arsitektur shard (workflow matrix):
 *   - Daftar kandidat judul dibangun deterministik (union
 *     katalog + peta situs ketiga sumber, diurutkan per kunci)
 *     lalu tiap shard hanya memproses kandidat dengan
 *     index % SHARD_TOTAL === SHARD_INDEX. Semua shard
 *     menghitung daftar yang sama dari masukan yang sama,
 *     jadi partisi selalu saling lepas & lengkap.
 *   - Di dalam shard, judul diproses paralel dengan p-limit;
 *     konkurensi dari env CONCURRENCY (bawaan 8; tahap awal
 *     penyebaran memakai 5 — lihat README, bagian ramp-up).
 *   - Mode fast (SYNC_MODE=fast): hanya judul RELEASING
 *     (+ judul baru yang belum ada di katalog). Mode full:
 *     semua judul.
 *
 * Hemat kerja:
 *   - Hash sha256 per judul (fingerprint katalog+situs) di
 *     .sync-cache.json (dipertahankan antar-run via
 *     actions/cache) + cermin Supabase sync_hashes: judul tak
 *     berubah dilewati tanpa mengambil satu halaman pun.
 *   - Hash per EPISODE (streamUnduh+subtitle): episode yang
 *     payload-nya identik dengan yang pernah terkirim tidak
 *     dikirim ulang walau fingerprint judul berubah.
 *   - Judul FINISHED yang episodenya lengkap dilewati.
 *   - Metadata AniList di-cache TTL 7 hari (cache yang sama).
 *   - Keadaan katalog dari SATU GET /api/anime per shard.
 *
 * Ketahanan request: timeout per request, retry dengan
 * backoff eksponensial + jitter, HTTP 429 dihormati lewat
 * header Retry-After, dan pelacak kesehatan per host: bila
 * satu host membalas 403/429 beruntun, jeda antar-request ke
 * host itu dinaikkan otomatis dan peringatan dicetak (sinyal
 * untuk menurunkan CONCURRENCY/SHARD_TOTAL di workflow).
 *
 * Revalidate: bila ada episode baru dan REVALIDATE_SECRET
 * terisi, di akhir run shard memanggil
 * POST {origin API}/api/revalidate {slugs:[...]} agar memo
 * katalog Worker dibersihkan dan halaman terkait langsung
 * segar (tanpa menunggu TTL 120 detik).
 *
 * Lingkungan:
 *   SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY  (opsional —
 *     hanya sync_logs/sync_hashes; tanpa keduanya sinkron
 *     katalog tetap jalan, log cukup ke konsol)
 *   ANIME_API_URL    bawaan API live di atas
 *   SYNC_MODE        fast | full (bawaan full)
 *   SHARD_INDEX      0..SHARD_TOTAL-1 (bawaan 0)
 *   SHARD_TOTAL      jumlah shard (bawaan 1)
 *   CONCURRENCY      paralel per shard (bawaan 8)
 *   SYNC_MAX         batas episode per shard per run
 *   SYNC_GAP_MS      jeda sopan dasar antar request sumber
 *   REVALIDATE_SECRET  token bersama endpoint revalidate
 *
 * Bendera: --mode fast|full --shard I --shards N
 *          --concurrency N --max N --dry-run
 *          --source hianime|gogoanime|9anime
 *          --limit-series N (uji) --self-test-shard --help
 * Keluar non-nol bila run gagal fatal agar GitHub memberi
 * notifikasi kegagalan.
 */
import { createHash, createDecipheriv } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import pLimit from 'p-limit';

const API = (process.env.ANIME_API_URL || 'https://my-anime-app.adiyth-arpansa.workers.dev/api/anime').replace(/\/$/, '');
const SITE_ORIGIN = new URL(API).origin;
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';
const GAP_MS = Number(process.env.SYNC_GAP_MS || 400);
const CACHE_PATH = path.resolve(process.env.SYNC_CACHE_PATH || '.sync-cache.json');
const ANILIST_META_TTL_MS = 7 * 24 * 3600 * 1000;
const DETAIL_TTL_MS = 6 * 3600 * 1000;

const args = process.argv.slice(2);
function argValue(name, fallback) {
  const i = args.indexOf(name);
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback;
}
const MODE = String(argValue('--mode', process.env.SYNC_MODE || 'full')).toLowerCase() === 'fast' ? 'fast' : 'full';
const SHARD_TOTAL = Math.max(1, Number(argValue('--shards', process.env.SHARD_TOTAL || 1)) || 1);
const SHARD_INDEX = Math.min(SHARD_TOTAL - 1, Math.max(0, Number(argValue('--shard', process.env.SHARD_INDEX || 0)) || 0));
const CONCURRENCY = Math.max(1, Number(argValue('--concurrency', process.env.CONCURRENCY || 8)) || 8);
const MAX_EPISODES = Number(argValue('--max', process.env.SYNC_MAX || 20));
const DRY_RUN = args.includes('--dry-run');
const ONLY_SOURCE = argValue('--source', '');
const LIMIT_SERIES = Number(argValue('--limit-series', 0));
if (args.includes('--help')) {
  console.log('Pemakaian: node scripts/sync.mjs [--mode fast|full] [--shard I --shards N] [--concurrency N] [--max N] [--dry-run] [--source hianime|gogoanime|9anime] [--limit-series N] [--self-test-shard]');
  process.exit(0);
}

/** Partisi shard murni: kandidat terurut -> milik shard ini. */
export function shardSlice(sortedKeys, shardIndex, shardTotal) {
  return sortedKeys.filter((_, i) => i % shardTotal === shardIndex);
}
if (args.includes('--self-test-shard')) {
  const keys = Array.from({ length: 101 }, (_, i) => `judul-${String(i).padStart(3, '0')}`);
  const seen = new Set();
  for (let s = 0; s < 4; s++) {
    for (const k of shardSlice(keys, s, 4)) {
      if (seen.has(k)) { console.error(`GAGAL: ${k} muncul di dua shard`); process.exit(1); }
      seen.add(k);
    }
  }
  if (seen.size !== keys.length) { console.error('GAGAL: partisi tidak lengkap'); process.exit(1); }
  console.log('self-test shard LOLOS: 4 shard saling lepas & lengkap atas 101 kunci');
  process.exit(0);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const sha256 = (text) => createHash('sha256').update(String(text)).digest('hex');
const slugify = (text) => String(text || 'anime').toLowerCase().normalize('NFKD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '') || 'anime';
const normTitle = (text) => String(text || '').toLowerCase().replace(/[^a-z0-9 ]/g, '').replace(/\s+/g, ' ').trim();

/* ------- kesehatan host: rem otomatis saat 403/429 ------- */
const hostHealth = new Map(); // host -> { strikes, penaltyUntil }
function hostStrike(host, status) {
  if (status !== 403 && status !== 429) return;
  const h = hostHealth.get(host) || { strikes: 0, penaltyUntil: 0 };
  h.strikes += 1;
  h.penaltyUntil = Date.now() + Math.min(120000, 5000 * h.strikes);
  hostHealth.set(host, h);
  if (h.strikes >= 3) {
    console.warn(`[!] Host ${host} membalas ${status} beruntun (${h.strikes}x) — jeda otomatis dinaikkan. Bila berlanjut, turunkan CONCURRENCY / SHARD_TOTAL (lihat README ramp-down).`);
  }
}
async function hostGate(host) {
  const h = hostHealth.get(host);
  if (h && h.penaltyUntil > Date.now()) await sleep(Math.min(30000, h.penaltyUntil - Date.now()));
}

/** fetch dengan timeout + retry backoff eksponensial + jitter;
 *  HTTP 429 dihormati lewat Retry-After. */
async function fetchText(url, { timeoutMs = 30000, retries = 3, headers = {}, redirect = 'follow' } = {}) {
  let host = '';
  try { host = new URL(url).host; } catch { /* abaikan */ }
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    if (host) await hostGate(host);
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), timeoutMs);
    try {
      const res = await fetch(url, {
        signal: ctrl.signal,
        redirect,
        headers: { 'User-Agent': UA, 'Accept-Language': 'en-US,en;q=0.9', ...headers },
      });
      clearTimeout(timer);
      if (res.status === 429 || res.status >= 500 || res.status === 403) {
        if (host) hostStrike(host, res.status);
        const ra = Number(res.headers.get('retry-after'));
        const wait = res.status === 429 && Number.isFinite(ra) && ra > 0
          ? Math.min(60000, ra * 1000)
          : Math.min(30000, 1000 * 2 ** attempt) + Math.random() * 500;
        throw Object.assign(new Error(`HTTP ${res.status}`), { retryWaitMs: wait, retryable: res.status !== 403 || attempt < 1 });
      }
      if (!res.ok) throw Object.assign(new Error(`HTTP ${res.status}`), { retryable: false });
      if (host) { const h = hostHealth.get(host); if (h && h.strikes > 0) { h.strikes = 0; hostHealth.set(host, h); } }
      return { text: await res.text(), url: res.url, status: res.status };
    } catch (err) {
      clearTimeout(timer);
      lastErr = err;
      if (err.retryable === false) break;
      if (attempt < retries) await sleep(err.retryWaitMs || (Math.min(15000, 800 * 2 ** attempt) + Math.random() * 400));
    }
  }
  throw lastErr;
}
async function fetchJson(url, opts) {
  const { text } = await fetchText(url, opts);
  return JSON.parse(text);
}

/* ---------------- cache lokal (dipertahankan Actions/cache) ---------------- */
let cache = { hashes: {}, epHashes: {}, anilist: {}, details: {}, malByAnimeId: {} };
try {
  cache = { ...cache, ...JSON.parse(await readFile(CACHE_PATH, 'utf8')) };
} catch { /* run pertama: cache kosong itu wajar */ }
cache.epHashes = cache.epHashes || {};
let cacheDirty = false;
async function saveCache() {
  if (!cacheDirty && !DRY_RUN) { /* tetap tulis: murah & aman */ }
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
    const { data, error } = await supabase.from('sync_logs')
      .insert({ status: 'running', shard: SHARD_INDEX, mode: MODE }).select('id').single();
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
      shard: SHARD_INDEX,
      mode: MODE,
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
  cacheDirty = true;
  if (!supabase) return;
  try {
    await supabase.from('sync_hashes').upsert({ anime_id: animeId, hash, updated_at: new Date().toISOString() });
  } catch { /* tabel belum dibuat = lewati senyap */ }
}
/** Hash per episode: payload identik -> tidak dikirim ulang. */
function episodeHash(payload) {
  return sha256([payload.streamUrl || '', payload.downloadUrl || '', (payload.subtitles || []).map((s) => s.url).join(',')].join('|'));
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
  if (res.status === 429) {
    const ra = Number(res.headers.get('retry-after'));
    await sleep(Number.isFinite(ra) && ra > 0 ? Math.min(60000, ra * 1000) : 5000);
    throw new Error('AniList HTTP 429');
  }
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
    if (meta) { cache.anilist[key] = { at: Date.now(), meta }; cacheDirty = true; }
    return meta;
  } catch { return null; }
}
async function anilistSearch(title) {
  const key = `search:${normTitle(title)}`;
  const hit = cache.anilist[key];
  if (hit && Date.now() - hit.at < ANILIST_META_TTL_MS) return hit.meta;
  try {
    const meta = shapeAniList(await anilistPost(ANILIST_QUERY_SEARCH, { search: title }), title);
    if (meta) { cache.anilist[key] = { at: Date.now(), meta }; cacheDirty = true; }
    return meta;
  } catch { return null; }
}
async function malOfAnilist(anilistId) {
  if (!anilistId) return null;
  if (cache.malByAnimeId[anilistId]) return cache.malByAnimeId[anilistId];
  try {
    const m = await anilistPost(ANILIST_QUERY_BY_ID, { id: Number(anilistId) });
    if (m && m.idMal) { cache.malByAnimeId[anilistId] = m.idMal; cacheDirty = true; return m.idMal; }
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
async function fetchPostJson(url, body, { timeoutMs = 25000 } = {}) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method: 'POST', signal: ctrl.signal, redirect: 'follow',
      headers: { 'User-Agent': UA, Accept: 'application/json', 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return JSON.parse(await res.text());
  } finally { clearTimeout(timer); }
}
const metaTokens = (x) => new Set(String(x || '').toLowerCase().match(/[a-z0-9]+/g) || []);
function metaGuard(title, candidates) {
  const q = metaTokens(title);
  if (!q.size) return false;
  const longest = [...q].sort((a, b) => b.length - a.length)[0];
  const cand = new Set((candidates || []).flatMap((c) => [...metaTokens(c)]));
  return cand.has(longest);
}
const SEASON_BY_MONTH_JS = KITSU_SEASON;
function yearSeason(dateText) {
  const start = String(dateText || '');
  return {
    year: /^\d{4}/.test(start) ? Number(start.slice(0, 4)) : null,
    season: SEASON_BY_MONTH_JS[Number(start.slice(5, 7))] || null,
  };
}
function emptyMeta(title) {
  return {
    anilistId: null, title, altTitles: [], poster: '', banner: '', synopsis: '',
    genres: [], score: null, year: null, season: null, studio: null,
    totalEpisodes: null, status: '', format: null,
  };
}
async function bangumiSearch(title) {
  // Poster Asia/donghua; sinopsis Mandarin/Jepang SENGAJA tidak diambil.
  try {
    const data = await fetchPostJson('https://api.bgm.tv/v0/search/subjects?limit=5',
      { keyword: title, sort: 'match', filter: { type: [2] } });
    for (const item of (data && data.data) || []) {
      const aliases = [item.name, item.name_cn];
      for (const row of item.infobox || []) {
        const v = row && row.value;
        if (typeof v === 'string') aliases.push(v);
        else if (Array.isArray(v)) for (const x of v) { if (x && typeof x === 'object') { aliases.push(x.v, x.k); } else if (typeof x === 'string') aliases.push(x); }
      }
      if (!metaGuard(title, aliases)) continue;
      const images = item.images || {};
      const { year, season } = yearSeason(item.date);
      const m = emptyMeta(title);
      m.altTitles = aliases.filter((a) => a && a !== title).slice(0, 6);
      m.poster = images.large || item.image || images.common || '';
      m.score = item.rating && item.rating.score ? Math.round(Number(item.rating.score) * 10) / 10 : null;
      m.year = year; m.season = season;
      m.totalEpisodes = item.eps || item.total_episodes || null;
      m.format = String(item.platform || '').toUpperCase() === 'TV' ? 'TV' : null;
      return m;
    }
  } catch { /* gagal senyap: rantai lanjut */ }
  return null;
}
async function tvmazeSearch(title) {
  try {
    const data = await fetchJson(`https://api.tvmaze.com/search/shows?q=${encodeURIComponent(title)}`, { retries: 1 });
    for (const hit of data || []) {
      const show = (hit && hit.show) || {};
      if (!metaGuard(title, [show.name])) continue;
      const image = show.image || {};
      const { year, season } = yearSeason(show.premiered);
      let totalEpisodes = null;
      try {
        const eps = await fetchJson(`https://api.tvmaze.com/shows/${show.id}/episodes`, { retries: 0 });
        if (Array.isArray(eps)) totalEpisodes = eps.filter((e) => (e.season || 0) >= 1).length || null;
      } catch { /* total episode kosong tidak fatal */ }
      const m = emptyMeta(show.name || title);
      m.poster = image.original || image.medium || '';
      m.synopsis = String(show.summary || '').replace(/<[^>]+>/g, '').trim();
      m.genres = show.genres || [];
      m.score = (show.rating && show.rating.average) ?? null;
      m.year = year; m.season = season;
      m.totalEpisodes = totalEpisodes;
      m.status = ({ Running: 'RELEASING', Ended: 'FINISHED' })[show.status] || '';
      m.format = String(show.type || '').toLowerCase() === 'animation' ? 'TV' : null;
      return m;
    }
  } catch { /* gagal senyap */ }
  return null;
}
async function shikimoriSearch(title) {
  // Basis MyAnimeList; poster missing_* dilewati; sinopsis Rusia tidak diambil.
  try {
    const data = await fetchJson(`https://shikimori.one/api/animes?search=${encodeURIComponent(title)}&limit=5&order=ranked`, { retries: 1 });
    for (const item of data || []) {
      if (!metaGuard(title, [item.name])) continue;
      const image = item.image || {};
      const rel = image.original || image.preview || '';
      if (!rel || rel.includes('missing_')) continue;
      const { year, season } = yearSeason(item.aired_on);
      const m = emptyMeta(item.name || title);
      m.poster = rel.startsWith('http') ? rel : `https://shikimori.io${rel}`;
      m.score = item.score ? Math.round(Number(item.score) * 10) / 10 : null;
      m.year = year; m.season = season;
      m.totalEpisodes = item.episodes || item.episodes_aired || null;
      m.status = ({ released: 'FINISHED', ongoing: 'RELEASING', anons: 'NOT_YET_RELEASED' })[item.status] || '';
      m.format = ({ tv: 'TV', movie: 'MOVIE', ova: 'OVA', ona: 'ONA', special: 'SPECIAL' })[String(item.kind || '').toLowerCase()] || null;
      return m;
    }
  } catch { /* gagal senyap */ }
  return null;
}
const META_CHAIN = [
  ['kitsu', kitsuSearch], ['bangumi', bangumiSearch],
  ['tvmaze', tvmazeSearch], ['shikimori', shikimoriSearch],
];
async function chainSearch(title) {
  // Penggabungan ISI CELAH antar provider, seperti enrich.py.
  let merged = null;
  for (const [, fn] of META_CHAIN) {
    let meta = null;
    try { meta = await fn(title); } catch { meta = null; }
    if (!meta) continue;
    if (!merged) { merged = emptyMeta(meta.title || title); merged.title = meta.title || title; }
    for (const key of ['poster', 'banner', 'synopsis', 'score', 'year', 'season', 'studio', 'totalEpisodes', 'status', 'format']) {
      const cur = merged[key];
      const val = meta[key];
      if ((cur === null || cur === '' || (Array.isArray(cur) && !cur.length)) && val !== null && val !== '' && val !== undefined) merged[key] = val;
    }
    for (const g of meta.genres || []) if (g && !merged.genres.includes(g)) merged.genres.push(g);
    for (const a of meta.altTitles || []) if (a && a !== merged.title && !merged.altTitles.includes(a)) merged.altTitles.push(a);
    if (merged.poster && merged.synopsis && merged.score) break;
  }
  return merged && (merged.poster || merged.synopsis) ? merged : null;
}
async function enrichNewTitle(malId, titleGuess) {
  if (malId) { const m = await anilistByMal(malId); if (m) return m; }
  if (titleGuess) { const m = await anilistSearch(titleGuess); if (m) return m; return chainSearch(titleGuess); }
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
    // rantai berukuran ratusan MB — cukup URL akhirnya saja.
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

/** Satu halaman episode WordPress (gogo/9anime) -> payload.
 *  enrich=false untuk judul yang sudah ada (hemat AniList). */
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
  if (detail) { cache.details[id] = { at: Date.now(), data: detail }; cacheDirty = true; }
  return detail;
}
async function apiPost(payload) {
  if (DRY_RUN) { console.log(`  [dry-run] POST ${payload.rawTitle} ep${payload.episodeNum}`); return { ok: true, dry: true }; }
  let lastErr = '';
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const res = await fetch(API, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'User-Agent': UA },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(60000),
      });
      const out = await res.json().catch(() => ({}));
      if (res.status === 422) return { ok: false, error: out.error || 'HTTP 422', rejected: true };
      if (res.status === 429 || res.status >= 500) { lastErr = `HTTP ${res.status}`; await sleep(2000 * (attempt + 1)); continue; }
      if (!res.ok || !out.ok) return { ok: false, error: out.error || `HTTP ${res.status}` };
      return out;
    } catch (err) { lastErr = err.message; await sleep(1500); }
  }
  return { ok: false, error: lastErr };
}

/* ---------------- keadaan run (aman paralel: JS satu utas) ---------------- */
const stats = { titlesChecked: 0, titlesUpdated: 0, episodesAdded: 0 };
let budgetLeft = MAX_EPISODES;
const addedLabels = [];
const touchedSlugs = new Set();
/** Cadangkan 1 slot anggaran episode (sinkron, tanpa celah balapan). */
function reserve() {
  if (budgetLeft <= 0) return false;
  budgetLeft -= 1;
  return true;
}
/** Kirim satu episode: dilewati bila hash episode identik
 *  dengan yang pernah terkirim (tidak ada upsert percuma). */
async function postEpisode(animeKey, payload, label) {
  const hashKey = `${animeKey}:${payload.episodeNum}`;
  const hash = episodeHash(payload);
  if (!DRY_RUN && cache.epHashes[hashKey] === hash) return 'unchanged';
  const out = await apiPost(payload);
  if (!out.ok) { console.log(`  gagal POST ${label}: ${out.error || ''}`); return 'failed'; }
  cache.epHashes[hashKey] = hash;
  cacheDirty = true;
  stats.episodesAdded += 1;
  addedLabels.push(label);
  touchedSlugs.add(animeKey);
  console.log(`DITAMBAH ${label}`);
  return 'added';
}

/* ---------------- peta situs (penemuan kandidat) ---------------- */
async function hianimeMap() {
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
  return site;
}
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

/** Bangun daftar kandidat gabungan (deterministik).
 *  Item: { key, catalog|null, hianime?, gogoanime?, '9anime'? } */
async function buildWork(catalog) {
  const byId = new Map(catalog.map((a) => [a.id, a]));
  const byTitle = new Map(catalog.map((a) => [normTitle(a.title), a]));
  const items = new Map();
  const getItem = (key, anime) => {
    if (!items.has(key)) items.set(key, { key, catalog: anime || null });
    const it = items.get(key);
    if (anime && !it.catalog) it.catalog = anime;
    return it;
  };
  const sourcesState = { hianime: false, gogoanime: false, '9anime': false };
  if (!ONLY_SOURCE || ONLY_SOURCE === 'hianime') {
    try {
      const site = await hianimeMap();
      sourcesState.hianime = true;
      for (const [sslug, info] of site) {
        const anime = byTitle.get(normTitle(sslug.replace(/-/g, ' '))) || null;
        getItem(anime ? anime.id : `h:${sslug}`, anime).hianime = { sslug, ...info };
      }
    } catch (err) { console.error(`[!] Peta hianime gagal: ${err.message}`); }
  }
  const wpDefs = [
    ['gogoanime', 'https://gogoanime.or.at', '/([a-z0-9-]+)-episode-(\\d+)/?$'],
    ['9anime', 'https://9anime.or.at', '/([a-z0-9-]+)-episode-(\\d+)-english-subbed/?$'],
  ];
  for (const [name, home, pattern] of wpDefs) {
    if (ONLY_SOURCE && ONLY_SOURCE !== name) continue;
    try {
      const site = await wpEpisodeMap(home, pattern);
      sourcesState[name] = true;
      for (const [sslug, eps] of site) {
        if (sslug.includes('hentai')) continue;
        const anime = byId.get(sslug) || null;
        getItem(anime ? anime.id : `w:${sslug}`, anime)[name] = { sslug, eps };
      }
    } catch (err) { console.error(`[!] Peta ${name} gagal: ${err.message}`); }
  }
  let all = [...items.values()];
  if (MODE === 'fast') {
    // Mode cepat: hanya yang sedang tayang. Status kosong
    // (belum diketahui) dan judul baru ikut — full harian
    // tetap memeriksa semuanya.
    all = all.filter((it) => {
      if (!it.catalog) return true;
      const st = String(it.catalog.status || '').toUpperCase();
      return st === 'RELEASING' || st === '';
    });
  }
  all.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  const keys = all.map((it) => it.key);
  const mine = new Set(shardSlice(keys, SHARD_INDEX, SHARD_TOTAL));
  return { items: all.filter((it) => mine.has(it.key)), sourcesState, totalCandidates: all.length };
}

/* ---------------- proses per judul: hianime ---------------- */
async function processHianime(item) {
  const { hianime: info, catalog: anime } = item;
  if (!info) return;
  stats.titlesChecked += 1;
  const siteEps = [...info.eps].sort((x, y) => x - y);
  if (anime) {
    const fingerprint = sha256(`${anime.id}|${anime.updatedAt || ''}|${siteEps.join(',')}|${(anime.episodeNumbers || []).join(',')}`);
    if (cache.hashes[anime.id] === fingerprint) return;
    const have = new Set(anime.episodeNumbers || []);
    const missing = siteEps.filter((n) => !have.has(n));
    const finished = String(anime.status || '').toUpperCase() === 'FINISHED'
      || (anime.totalEpisodes && have.size >= anime.totalEpisodes);
    if (!missing.length || finished) { await persistHash(anime.id, fingerprint); return; }
    const detail = await loadDetail(anime.id);
    if (!detail) return;
    if (!(detail.episodes || []).some((e) => String(e.streamUrl).includes('dramahot'))) return; // jangan campur sumber
    let mal = cache.malByAnimeId[anime.id] || null;
    if (!mal && detail.anilistId) mal = await malOfAnilist(detail.anilistId);
    if (!mal) return;
    let updated = false;
    for (const ep of missing) {
      if (!reserve()) break;
      const embed = `https://zokoanime.video/stream/mal/${mal}/${ep}/sub`;
      const cfg = decodePlayerConfig((await fetchText(embed, { retries: 1 })).text);
      const src = cfg && cfg.src;
      if (!src) continue;
      const r = await postEpisode(anime.id, {
        rawTitle: detail.rawTitle, episodeNum: ep, streamUrl: src,
        downloadUrl: cfg.download_url || null, headers: { Referer: embed },
        subtitles: mapZokoSubtitles(cfg), meta: null,
      }, `${anime.title} ep${ep} (hianime)`);
      if (r === 'added') updated = true;
    }
    if (updated) stats.titlesUpdated += 1;
    await persistHash(anime.id, fingerprint);
  } else {
    const titleGuess = info.sslug.replace(/-/g, ' ').replace(/\s+/g, ' ').trim();
    const html = (await fetchText(info.url, { retries: 1 })).text;
    const m = html.match(/zokoanime\.video\/stream\/mal\/(\d+)\//);
    if (!m) return;
    const mal = m[1];
    const meta = await enrichNewTitle(mal, titleGuess);
    if (!meta || !meta.poster) { console.log(`  tunda judul baru ${titleGuess}: thumbnail belum tersedia`); return; }
    const key = slugify(meta.title || titleGuess);
    let updated = false;
    for (const ep of siteEps.slice(0, 3)) {
      if (!reserve()) break;
      const embed = `https://zokoanime.video/stream/mal/${mal}/${ep}/sub`;
      const cfg = decodePlayerConfig((await fetchText(embed, { retries: 1 })).text);
      const src = cfg && cfg.src;
      if (!src) break;
      const r = await postEpisode(key, {
        rawTitle: titleGuess, episodeNum: ep, streamUrl: src,
        downloadUrl: cfg.download_url || null, headers: { Referer: embed },
        subtitles: mapZokoSubtitles(cfg), meta,
      }, `${meta.title || titleGuess} ep${ep} (hianime, BARU)`);
      if (r === 'added') updated = true; else if (r === 'failed') break;
    }
    if (updated) stats.titlesUpdated += 1;
  }
}

/* ---------------- proses per judul: WordPress ---------------- */
async function processWp(item, name, hostMarker) {
  const data = item[name];
  if (!data) return;
  stats.titlesChecked += 1;
  const eps = data.eps;
  const epNums = Object.keys(eps).map(Number);
  if (!epNums.length) return;
  const template = eps[Math.max(...epNums)];
  const urlFor = (n) => eps[n] || template.replace(/episode-\d+/, `episode-${n}`);
  const anime = item.catalog;
  if (anime) {
    const have = new Set(anime.episodeNumbers || []);
    const targets = [];
    for (let n = 1; n <= Math.max(...epNums); n++) if (!have.has(n)) targets.push(n);
    if (!targets.length) return;
    const detail = await loadDetail(anime.id);
    if (!detail) return;
    if ((detail.episodes || []).length && !(detail.episodes || []).some((e) => String(e.streamUrl).includes(hostMarker))) return;
    let updated = false;
    for (const ep of targets) {
      if (!reserve()) break;
      try {
        const payload = await scrapeWpEpisode(urlFor(ep), detail.rawTitle, ep, { enrich: false });
        payload.rawTitle = detail.rawTitle;
        payload.meta = null;
        const r = await postEpisode(anime.id, payload, `${anime.title} ep${ep} (${name})`);
        if (r === 'added') updated = true;
      } catch (err) { console.log(`  gagal ${name} ${anime.title} ep${ep}: ${err.message}`); }
    }
    if (updated) stats.titlesUpdated += 1;
  } else {
    const title = data.sslug.replace(/-/g, ' ').replace(/\s+/g, ' ').trim();
    const key = slugify(title);
    for (let ep = 1; ep <= 2; ep++) {
      if (!reserve()) break;
      try {
        const payload = await scrapeWpEpisode(urlFor(ep), title, ep);
        if (!(payload.meta && payload.meta.poster) && !payload.thumbnail) {
          console.log(`  tunda judul baru ${title}: thumbnail belum tersedia`);
          break;
        }
        const r = await postEpisode(key, payload, `${title} ep${ep} (${name}, BARU)`);
        if (r === 'added') stats.titlesUpdated += 1;
        else if (r === 'failed') break;
      } catch { break; }
    }
  }
}

/* ---------------- revalidate situs ---------------- */
async function revalidateTouched() {
  if (DRY_RUN || stats.episodesAdded === 0) return;
  const secret = process.env.REVALIDATE_SECRET || '';
  if (!secret) { console.log('[i] REVALIDATE_SECRET belum diisi — revalidate dilewati (halaman segar sendiri <=120 dtk).'); return; }
  const slugs = [...touchedSlugs].slice(0, 100);
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const res = await fetch(`${SITE_ORIGIN}/api/revalidate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-revalidate-secret': secret, 'User-Agent': UA },
        body: JSON.stringify({ slugs }),
        signal: AbortSignal.timeout(30000),
      });
      const out = await res.json().catch(() => ({}));
      if (res.ok && out.ok) { console.log(`revalidate OK untuk ${slugs.length} judul`); return; }
      console.warn(`[!] revalidate balas HTTP ${res.status}: ${out.error || ''}`);
      return;
    } catch (err) {
      if (attempt === 1) console.warn(`[!] revalidate gagal: ${err.message}`);
      else await sleep(2000);
    }
  }
}

/* ---------------- main ---------------- */
async function main() {
  console.log(`--- Sinkron2.0 v2 mulai | mode=${MODE} shard=${SHARD_INDEX}/${SHARD_TOTAL} konkurensi=${CONCURRENCY} batas=${MAX_EPISODES}/shard${DRY_RUN ? ' DRY-RUN' : ''} ---`);
  await logStart();
  const catalog = await loadCatalog();
  console.log(`katalog saat ini: ${catalog.length} judul`);
  const { items, sourcesState, totalCandidates } = await buildWork(catalog);
  console.log(`kandidat mode ${MODE}: ${totalCandidates} judul -> shard ini: ${items.length} judul`);
  const sourcesOk = Object.values(sourcesState).filter(Boolean).length;
  const limit = pLimit(CONCURRENCY);
  const runSource = async (name, itemsForSource, fn) => {
    const ordered = [...itemsForSource].sort((a, b) => {
      const ea = a.catalog ? (a.catalog.episodeNumbers || []).length : Number.POSITIVE_INFINITY;
      const eb = b.catalog ? (b.catalog.episodeNumbers || []).length : Number.POSITIVE_INFINITY;
      return ea - eb;
    });
    const slice = LIMIT_SERIES ? ordered.slice(0, LIMIT_SERIES) : ordered;
    console.log(`=== ${name}: ${slice.length} judul di shard ini ===`);
    await Promise.all(slice.map((item) => limit(async () => {
      try { await fn(item); } catch (err) { console.log(`  gagal ${name} ${item.key}: ${err.message}`); }
    })));
  };
  if (sourcesState.hianime) await runSource('hianime', items.filter((i) => i.hianime), processHianime);
  if (sourcesState.gogoanime) await runSource('gogoanime', items.filter((i) => i.gogoanime), (it) => processWp(it, 'gogoanime', 'nexabloom'));
  if (sourcesState['9anime']) await runSource('9anime', items.filter((i) => i['9anime']), (it) => processWp(it, '9anime', '1anime'));
  await saveCache();
  await revalidateTouched();
  console.log(`--- Sinkron2.0 selesai: ${stats.episodesAdded} episode ditambahkan (shard ${SHARD_INDEX}/${SHARD_TOTAL}) ---`);
  for (const label of addedLabels) console.log(`  + ${label}`);
  if (sourcesOk === 0) throw new Error('semua peta sumber gagal dimuat');
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

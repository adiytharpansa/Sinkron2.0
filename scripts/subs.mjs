#!/usr/bin/env node
/**
 * Sapuan Subtitle — penangkapan subtitle dari sumber (MegaPlay)
 * untuk episode yang SUDAH tersimpan tetapi trek subtitle-nya
 * masih kosong.
 *
 * Latar: scraper utama (sync.mjs) dulu hanya mengambil berkas
 * video dari respons getSourcesNew MegaPlay dan membuang bidang
 * `tracks` pada respons yang sama — padahal di situlah subtitle
 * resmi sumber berada (berkas .vtt, kind "captions"). Sapuan ini
 * menambal katalog yang sudah ada: untuk tiap episode ber-referer
 * megaplay.buzz dengan subtitles kosong, embed dibuka sekali,
 * tracks dipetakan, lalu episode dikirim ulang ke API dengan
 * payload yang sama persis + subtitle terisi (hash episode di
 * sync.mjs memang sudah menyertakan URL subtitle, jadi kiriman
 * ini tercatat sebagai pembaruan yang sah).
 *
 * URL subtitle disimpan TERBUNGKUS proksi API
 * (<origin>/api/proxy?u=...&ref=https://megaplay.buzz/) karena
 * berkasnya tinggal di host CDN berputar yang sama dengan host
 * segmen video — host itu tidak terjangkau langsung dari
 * operator seluler pengguna (pelajaran saga putar), sedangkan
 * proksi API terbukti melayaninya (200, WEBVTT utuh).
 *
 * Idempoten secara konstruksi: episode yang subtitle-nya sudah
 * terisi dilewati dari data detail itu sendiri; tidak ada cache
 * atau keadaan lokal yang perlu dijaga antar-run.
 *
 * Lingkungan:
 *   ANIME_API_URL  bawaan API live
 *   SUBS_MAX       batas episode diperbarui per run (bawaan 300)
 *   CONCURRENCY    paralel judul (bawaan 4)
 *   SHARD_INDEX / SHARD_TOTAL  partisi katalog (bawaan 0/1)
 * Bendera: --dry-run --only <id> --max N --concurrency N
 *          --shard I --shards N
 */
import pLimit from 'p-limit';

const args = process.argv.slice(2);
const argValue = (name, fallback = '') => {
  const i = args.indexOf(name);
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback;
};
const API = (process.env.ANIME_API_URL || 'https://my-anime-app.adiyth-arpansa.workers.dev/api/anime').replace(/\/$/, '');
const SITE_ORIGIN = new URL(API).origin;
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';
const DRY_RUN = args.includes('--dry-run');
const ONLY_ID = argValue('--only', '');
const MAX_EPISODES = Number(argValue('--max', process.env.SUBS_MAX || 300)) || 300;
const CONCURRENCY = Math.max(1, Number(argValue('--concurrency', process.env.CONCURRENCY || 4)) || 4);
const SHARD_TOTAL = Math.max(1, Number(argValue('--shards', process.env.SHARD_TOTAL || 1)) || 1);
const SHARD_INDEX = Math.min(SHARD_TOTAL - 1, Math.max(0, Number(argValue('--shard', process.env.SHARD_INDEX || 0)) || 0));

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchText(url, { timeoutMs = 30000, retries = 2, headers = {} } = {}) {
  let lastErr = '';
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await fetch(url, { headers: { 'User-Agent': UA, ...headers }, signal: AbortSignal.timeout(timeoutMs) });
      if (res.status === 429 || res.status >= 500) { lastErr = `HTTP ${res.status}`; await sleep(1500 * (attempt + 1)); continue; }
      if (!res.ok) return { text: '', status: res.status };
      return { text: await res.text(), status: res.status };
    } catch (err) { lastErr = err.message; await sleep(1200 * (attempt + 1)); }
  }
  return { text: '', status: 0, error: lastErr };
}
async function fetchJson(url, opts) {
  const { text, status } = await fetchText(url, opts);
  try { return { data: JSON.parse(text), status }; } catch { return { data: null, status }; }
}

/* Bahasa trek ditentukan pertama-tama dari NAMA BERKAS sumber:
 * berkas DSNP memakai kode ISO 639-2 yang tegas (chi-21.vtt,
 * eng-*.vtt, fre-8.vtt, ...) — jauh lebih bisa dipercaya daripada
 * label bebas. Label hanya cadangan, dan yang tak dikenal TIDAK
 * ditebak lagi sebagai Inggris: bug 20 Okt 2026 — trek Tionghoa
 * berlabel "Chinese (- DSNP ...)" tak cocok tabel lama, jatuh ke
 * bawaan 'en', lalu dedupe membuang trek Inggris asli karena
 * "sudah ada en". Trek tanpa bahasa yang jelas kini dibuang. */
const LANG_BY_NAME = [['indonesia', 'id'], ['arab', 'ar'], ['french', 'fr'], ['italian', 'it'], ['portugu', 'pt'], ['russian', 'ru'], ['spanish', 'es'], ['german', 'de'], ['japan', 'ja'], ['english', 'en'], ['chinese', 'zh'], ['mandarin', 'zh'], ['cantonese', 'zh'], ['korean', 'ko'], ['thai', 'th'], ['vietnam', 'vi'], ['malay', 'ms'], ['dutch', 'nl'], ['polish', 'pl'], ['turk', 'tr'], ['swedish', 'sv'], ['danish', 'da'], ['finnish', 'fi'], ['norweg', 'no'], ['czech', 'cs'], ['greek', 'el'], ['hebrew', 'he'], ['hindi', 'hi'], ['ukrain', 'uk'], ['filipino', 'fil']];
const LANG_BY_FILE = { eng: 'en', ind: 'id', msa: 'ms', may: 'ms', zho: 'zh', chi: 'zh', jpn: 'ja', kor: 'ko', spa: 'es', fre: 'fr', fra: 'fr', ger: 'de', deu: 'de', ita: 'it', por: 'pt', rus: 'ru', ara: 'ar', tha: 'th', vie: 'vi', dut: 'nl', nld: 'nl', pol: 'pl', tur: 'tr', swe: 'sv', dan: 'da', fin: 'fi', nor: 'no', ces: 'cs', cze: 'cs', ell: 'el', gre: 'el', heb: 'he', hin: 'hi', ukr: 'uk', fil: 'fil' };
function langFromFile(file) {
  const name = String(file || '').split('/').pop() || '';
  const m = name.match(/^([a-z]{3})[-_.]/i) || name.match(/[-_]([a-z]{3})\.(?:vtt|srt|ass)$/i) || name.match(/^([a-z]{3})\.(?:vtt|srt|ass)$/i);
  if (m) return LANG_BY_FILE[m[1].toLowerCase()] || null;
  const two = name.match(/^([a-z]{2})\.(?:vtt|srt|ass)$/i);
  return two ? two[1].toLowerCase() : null;
}
function langOfTrack(label, file) {
  const fromFile = langFromFile(file);
  if (fromFile) return fromFile;
  const low = String(label || '').toLowerCase();
  for (const [name, code] of LANG_BY_NAME) if (low.includes(name)) return code;
  return null;
}
const proxyWrap = (file) => `${SITE_ORIGIN}/api/proxy?u=${encodeURIComponent(file)}&ref=${encodeURIComponent('https://megaplay.buzz/')}`;

/** Trek subtitle dari satu embed MegaPlay: bidang `tracks`
 *  (teks biasa, BUKAN bagian terenkripsi) pada getSourcesNew. */
async function resolveMegaplayTracks(embedUrl) {
  const { text: html } = await fetchText(embedUrl, { headers: { Referer: 'https://megaplay.buzz/' }, retries: 1 });
  const m = String(html || '').match(/data-id="(\d+)"/);
  if (!m) return [];
  const u = new URL(embedUrl);
  const { data } = await fetchJson(`${u.protocol}//${u.host}/stream/getSourcesNew?id=${m[1]}`, {
    headers: { Referer: embedUrl, 'X-Requested-With': 'XMLHttpRequest' }, retries: 1,
  });
  const out = [];
  for (const t of (data && data.tracks) || []) {
    if (!t || typeof t !== 'object') continue;
    if (t.kind !== 'captions' && t.kind !== 'subtitles') continue; // buang "thumbnails" dll.
    if (!t.file) continue;
    const label = String(t.label || 'English');
    const lang = langOfTrack(label, t.file);
    if (!lang) continue; // bahasa tak jelas — jangan tebak
    out.push({ label, lang, url: proxyWrap(String(t.file)), isDefault: t.default === true });
  }
  return dedupeByLang(out).map(({ isDefault, ...rest }) => rest);
}

/** Satu trek per bahasa: sumber bisa mengembalikan belasan
 *  varian "English" (server/kualitas berbeda) — menu subtitle
 *  aplikasi cukup satu per bahasa. Pilihan: yang ditandai
 *  default oleh sumber, lalu label yang persis nama bahasanya,
 *  lalu yang pertama. */
function dedupeByLang(tracks) {
  const byLang = new Map();
  for (const t of tracks) {
    const cur = byLang.get(t.lang);
    if (!cur) { byLang.set(t.lang, t); continue; }
    const score = (x) => (x.isDefault ? 2 : 0) + (LANG_BY_NAME.some(([n, c]) => c === x.lang && x.label.toLowerCase() === n) ? 1 : 0);
    if (score(t) > score(cur)) byLang.set(t.lang, t);
  }
  return [...byLang.values()];
}

/** Rapikan daftar tersimpan yang berisi bahasa ganda
 *  (warisan sapuan pertama sebelum dedupe ada): pertahankan
 *  satu per bahasa, urutan asli dipertahankan. */
function dedupeStored(tracks) {
  const seen = new Set();
  return tracks.filter((t) => {
    const lang = String((t && t.lang) || '').toLowerCase();
    if (!lang || seen.has(lang)) return false;
    seen.add(lang);
    return true;
  });
}

/** Perbaiki label bahasa trek TERSIMPAN dari nama berkas di URL
 *  trek itu sendiri (membuka bungkus proksi lebih dulu) — nama
 *  berkas adalah otoritas; label teks sumber tidak. Mengembalikan
 *  daftar baru + penanda berubah. Trek yang nama berkasnya tak
 *  berkode bahasa dibiarkan apa adanya. */
function relabelStored(tracks) {
  let changed = false;
  const out = tracks.map((t) => {
    if (!t || !t.url) return t;
    let inner = String(t.url);
    try {
      const u = new URL(inner);
      const wrapped = u.searchParams.get('u');
      if (wrapped) inner = wrapped;
    } catch { /* abaikan */ }
    const code = langFromFile(inner);
    if (code && code !== t.lang) { changed = true; return { ...t, lang: code }; }
    return t;
  });
  const deduped = dedupeStored(out);
  if (deduped.length !== out.length) changed = true;
  return { tracks: deduped, changed };
}

async function apiPost(payload) {
  if (DRY_RUN) return { ok: true, dry: true };
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
      if (res.status === 422) return { ok: false, error: out.error || 'HTTP 422' };
      if (res.status === 429 || res.status >= 500) { lastErr = `HTTP ${res.status}`; await sleep(2000 * (attempt + 1)); continue; }
      if (!res.ok || !out.ok) return { ok: false, error: out.error || `HTTP ${res.status}` };
      return out;
    } catch (err) { lastErr = err.message; await sleep(1500); }
  }
  return { ok: false, error: lastErr };
}

const stats = { titles: 0, epsScanned: 0, epsTarget: 0, epsFilled: 0, epsNoTracks: 0, failed: 0 };
let budget = MAX_EPISODES;

async function processTitle(anime) {
  stats.titles += 1;
  const { data } = await fetchJson(`${API}?id=${encodeURIComponent(anime.id)}&fresh=1`, { timeoutMs: 60000, retries: 2 });
  const detail = (data && data.data) || null;
  if (!detail) return;
  const targets = (detail.episodes || []).filter((e) => String(e.referer || '').includes('megaplay.buzz'));
  stats.epsScanned += (detail.episodes || []).length;
  for (const ep of targets) {
    const stored = ep.subtitles || [];
    // Perbaikan label dari nama berkas (kasus trek Tionghoa
    // tersimpan sebagai 'en'): daftar yang berubah perlu ditulis
    // ulang walau jumlah treknya sama.
    const relabeled = stored.length ? relabelStored(stored) : { tracks: [], changed: false };
    const needsRepair = stored.length > 0 && relabeled.changed;
    const lacksEnglish = stored.length > 0 && !relabeled.tracks.some((t) => t.lang === 'en');
    if (stored.length && !needsRepair && !lacksEnglish) continue; // sudah terisi, rapi, ada Inggris
    if (budget <= 0) return;
    budget -= 1;
    stats.epsTarget += 1;
    try {
      // Episode yang labelnya rusak / tak punya Inggris: coba
      // tangkap ulang dari sumber (daftar segar kini dipetakan
      // dengan benar); bila sumber memberi Inggris, daftar segar
      // yang otoritatif dipakai. Selain itu cukup perapian data
      // tersimpan — tak perlu ke sumber.
      let tracks = relabeled.tracks;
      if (!stored.length || needsRepair || lacksEnglish) {
        const fresh = await resolveMegaplayTracks(ep.referer).catch(() => []);
        if (fresh.length && (!stored.length || fresh.some((t) => t.lang === 'en'))) tracks = fresh;
      }
      if (!tracks.length) { stats.epsNoTracks += 1; continue; }
      const out = await apiPost({
        rawTitle: detail.rawTitle, episodeNum: ep.number,
        streamUrl: ep.streamUrl, downloadUrl: ep.downloadUrl ?? null,
        thumbnail: ep.thumbnail || undefined,
        headers: { Referer: ep.referer }, subtitles: tracks, meta: null,
      });
      if (!out.ok) { stats.failed += 1; console.log(`  gagal POST ${anime.id} ep${ep.number}: ${out.error || ''}`); continue; }
      stats.epsFilled += 1;
      console.log(`SUBTITLE ${anime.title} ep${ep.number}: ${tracks.map((t) => t.lang).join(', ')}${DRY_RUN ? ' (dry-run)' : ''}`);
    } catch (err) {
      stats.failed += 1;
      console.log(`  gagal ${anime.id} ep${ep.number}: ${err.message}`);
    }
    await sleep(120);
  }
}

async function main() {
  console.log(`--- Sapuan Subtitle mulai | shard=${SHARD_INDEX}/${SHARD_TOTAL} konkurensi=${CONCURRENCY} batas=${MAX_EPISODES}${DRY_RUN ? ' DRY-RUN' : ''}${ONLY_ID ? ` hanya=${ONLY_ID}` : ''} ---`);
  // Daftar judul cukup dari cache tepi API (cepat & stabil);
  // kesegaran dijaga di bacaan detail per judul (fresh=1 di sana).
  const { data } = await fetchJson(`${API}?lite=1`, { timeoutMs: 90000, retries: 3 });
  let items = (data && data.data) || [];
  if (!items.length) throw new Error('Katalog API kosong/gagal dimuat.');
  items.sort((a, b) => (a.id < b.id ? -1 : 1));
  if (ONLY_ID) items = items.filter((a) => a.id === ONLY_ID);
  else items = items.filter((_, i) => i % SHARD_TOTAL === SHARD_INDEX);
  console.log(`kandidat judul shard ini: ${items.length}`);
  const limit = pLimit(CONCURRENCY);
  await Promise.all(items.map((a) => limit(() => processTitle(a).catch((err) => console.log(`  gagal judul ${a.id}: ${err.message}`)))));
  console.log(`--- Sapuan Subtitle selesai: ${stats.epsFilled} episode terisi subtitle | ${stats.epsTarget} episode megaplay diperiksa | ${stats.epsNoTracks} tanpa trek di sumber | ${stats.failed} gagal | ${stats.epsScanned} episode teramati dari ${stats.titles} judul ---`);
}

try { await main(); } catch (err) { console.error(`[x] Sapuan Subtitle gagal fatal: ${err.message}`); process.exit(1); }

// Perbaikan stream massal (GitHub Actions): untuk judul-judul target (episode mati hasil audit),
// petakan AniList->MAL via GraphQL AniList (dapat diakses dari runner), lalu perbaiki tiap episode
// mati lewat embed MegaPlay pola mal/<MAL>/<ep>/sub (dekripsi + trek) dan POST ulang ke API.
// Pakai: node repair-stream.mjs [--only slug] [--dry]
import { createDecipheriv } from 'node:crypto';
import { readFileSync } from 'node:fs';
const DRY = process.argv.includes('--dry');
const onlyIdx = process.argv.indexOf('--only');
const ONLY = onlyIdx >= 0 ? process.argv[onlyIdx + 1] : '';
const KEY = Buffer.concat([Buffer.from('i?LMTAx0Q6,:}50U', 'utf8'), Buffer.alloc(16)]);
const IV = Buffer.from("W0;27ToaUpl_P%'c", 'utf8');
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';
const API = 'https://my-anime-app.adiyth-arpansa.workers.dev/api/anime';
const ORIGIN = new URL(API).origin;
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const LOG = null;
const PROG = null;
const log = (s) => console.log(s);
const LANG_BY_NAME = [['indonesia','id'],['arab','ar'],['french','fr'],['italian','it'],['portugu','pt'],['russian','ru'],['spanish','es'],['german','de'],['japan','ja'],['english','en'],['chinese','zh'],['mandarin','zh'],['cantonese','zh'],['korean','ko'],['thai','th'],['vietnam','vi'],['malay','ms'],['dutch','nl'],['polish','pl'],['turk','tr'],['swedish','sv'],['danish','da'],['finnish','fi'],['norweg','no'],['czech','cs'],['greek','el'],['hebrew','he'],['hindi','hi'],['ukrain','uk'],['filipino','fil']];
const LANG_BY_FILE = { eng:'en', ind:'id', msa:'ms', may:'ms', zho:'zh', chi:'zh', jpn:'ja', kor:'ko', spa:'es', fre:'fr', fra:'fr', ger:'de', deu:'de', ita:'it', por:'pt', rus:'ru', ara:'ar', tha:'th', vie:'vi', dut:'nl', nld:'nl', pol:'pl', tur:'tr', swe:'sv', dan:'da', fin:'fi', nor:'no', ces:'cs', cze:'cs', ell:'el', gre:'el', heb:'he', hin:'hi', ukr:'uk', fil:'fil' };
function langFromFile(file) {
  const name = String(file || '').split('/').pop() || '';
  const m = name.match(/^([a-z]{3})[-_.]/i) || name.match(/[-_]([a-z]{3})\.(?:vtt|srt|ass)$/i) || name.match(/^([a-z]{3})\.(?:vtt|srt|ass)$/i);
  if (m) return LANG_BY_FILE[m[1].toLowerCase()] || null;
  const two = name.match(/^([a-z]{2})\.(?:vtt|srt|ass)$/i);
  return two ? two[1].toLowerCase() : null;
}
const langOf = (label, file) => langFromFile(file) || (() => { const low = String(label || '').toLowerCase(); for (const [n, c] of LANG_BY_NAME) if (low.includes(n)) return c; return null; })();
const wrap = (f) => `${ORIGIN}/api/proxy?u=${encodeURIComponent(f)}&ref=${encodeURIComponent('https://megaplay.buzz/')}`;
async function resolveEp(ep, MAL) {
  const embed = `https://megaplay.buzz/stream/mal/${MAL}/${ep}/sub?autostart=true`;
  const html = await (await fetch(embed, { headers: { 'User-Agent': UA, Referer: 'https://hianime.to/' }, signal: AbortSignal.timeout(25000) })).text();
  const m = html.match(/data-id="(\d+)"/);
  if (!m) return null;
  const r = await fetch(`https://megaplay.buzz/stream/getSourcesNew?id=${m[1]}`, { headers: { 'User-Agent': UA, Referer: embed, 'X-Requested-With': 'XMLHttpRequest' }, signal: AbortSignal.timeout(25000) });
  const data = await r.json();
  if (!data.enc) return null;
  const raw = Buffer.from(String(data.enc).replace(/-/g, '+').replace(/_/g, '/'), 'base64');
  const d = createDecipheriv('aes-256-cbc', KEY, IV);
  const file = JSON.parse(Buffer.concat([d.update(raw), d.final()]).toString('utf8')).file;
  if (!file) return null;
  const byLang = new Map();
  for (const t of data.tracks || []) {
    if (!t || (t.kind !== 'captions' && t.kind !== 'subtitles') || !t.file) continue;
    const lang = langOf(t.label, t.file);
    if (!lang) continue;
    const cand = { label: String(t.label || 'English'), lang, url: wrap(String(t.file)), isDefault: t.default === true };
    const cur = byLang.get(lang);
    if (!cur || (cand.isDefault && !cur.isDefault)) byLang.set(lang, cand);
  }
  const tracks = [...byLang.values()].map(({ isDefault, ...rest }) => rest);
  return { file, tracks, embed };
}
async function apiPost(payload) {
  for (let att = 0; att < 2; att++) {
    try {
      const res = await fetch(API, { method: 'POST', headers: { 'Content-Type': 'application/json', 'User-Agent': UA }, body: JSON.stringify(payload), signal: AbortSignal.timeout(60000) });
      const out = await res.json().catch(() => ({}));
      if (res.status === 429 || res.status >= 500) { await sleep(2500 * (att + 1)); continue; }
      return { ok: res.ok && !!out.ok, error: out.error || `HTTP ${res.status}` };
    } catch (e) { await sleep(2000); if (att === 1) return { ok: false, error: e.message }; }
  }
  return { ok: false, error: 'gagal sesudah coba ulang' };
}
async function fetchDetail(slug) {
  for (let att = 0; att < 6; att++) {
    for (const q of [`?id=${encodeURIComponent(slug)}`, `?id=${encodeURIComponent(slug)}&fresh=1`]) {
      try {
        const r = await fetch(`${API}${q}`, { headers: { 'User-Agent': UA }, signal: AbortSignal.timeout(60000) });
        const t = await r.text();
        if (t.startsWith('{')) { const jj = JSON.parse(t); if (jj?.data) return jj.data; }
      } catch { /* ulang */ }
    }
    await sleep(3000 * (att + 1));
  }
  return null;
}
async function anilistMal(ids) {
  const out = {};
  for (let i = 0; i < ids.length; i += 50) {
    const chunk = ids.slice(i, i + 50);
    try {
      const r = await fetch('https://graphql.anilist.co', { method: 'POST', headers: { 'Content-Type': 'application/json', 'User-Agent': UA }, body: JSON.stringify({ query: 'query($ids:[Int]){ Page(perPage:50){ media(id_in:$ids){ id idMal } } }', variables: { ids: chunk } }) });
      const jj = await r.json();
      for (const m of jj?.data?.Page?.media || []) if (m.idMal) out[m.id] = m.idMal;
    } catch (e) { console.log('AniList batch gagal:', e.message); }
    await sleep(800);
  }
  return out;
}
const targets = JSON.parse(readFileSync(new URL(process.env.TARGETS_FILE || './repair-targets.json', import.meta.url), 'utf8'));
const slugs = Object.keys(targets).filter(s => !ONLY || s === ONLY);
console.log(`--- repair-stream mulai | judul ${slugs.length}${DRY ? ' DRY' : ''}${ONLY ? ' hanya=' + ONLY : ''} ---`);
const aniIds = {};
for (const slug of slugs) {
  const d = await fetchDetail(slug);
  if (!d) { console.log(`DETAIL GAGAL ${slug}`); continue; }
  aniIds[slug] = { detail: d, anilist: d.anilistId || null };
  await sleep(300);
}
const malMap = await anilistMal([...new Set(Object.values(aniIds).map(v => v.anilist).filter(Boolean))]);
console.log('idMal terpetakan:', Object.keys(malMap).length);
// Cadangan: judul tanpa anilistId/idMal -> cari AniList berdasar judul katalog (gerbang kemiripan ketat).
const norm = (t) => String(t || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
const sim = (a, b) => { const A = new Set(norm(a).split(' ')), B = new Set(norm(b).split(' ')); const inter = [...A].filter(x => B.has(x)).length; return inter / Math.max(1, Math.max(A.size, B.size)); };
async function malByTitle(detail) {
  const cands = [detail.title, detail.rawTitle, ...(detail.altTitles || [])].filter(Boolean);
  for (const q of cands) {
    try {
      const r = await fetch('https://graphql.anilist.co', { method: 'POST', headers: { 'Content-Type': 'application/json', 'User-Agent': UA }, body: JSON.stringify({ query: 'query($s:String){ Page(perPage:5){ media(search:$s, type:ANIME){ id idMal title{ romaji english native } seasonYear } } }', variables: { s: q } }) });
      const jj = await r.json();
      for (const m of jj?.data?.Page?.media || []) {
        if (!m.idMal) continue;
        const names = [m.title?.romaji, m.title?.english, m.title?.native].filter(Boolean);
        if (names.some(n => sim(n, q) >= 0.72)) return m.idMal;
      }
    } catch { /* lanjut */ }
    await sleep(500);
  }
  return null;
}
for (const slug of slugs) {
  const info = aniIds[slug];
  if (!info || malMap[info.anilist]) continue;
  const found = await malByTitle(info.detail);
  if (found) { malMap['title:' + slug] = found; console.log(`MAL via judul ${slug} -> ${found}`); }
}

const totals = { filled: 0, noSrc: 0, failed: 0, noMal: 0 };
for (const slug of slugs) {
  const info = aniIds[slug];
  if (!info) continue;
  const detail = info.detail;
  const MAL = malMap[info.anilist] || malMap['title:' + slug];
  const want = new Set(targets[slug]);
  if (!MAL) { totals.noMal += want.size; console.log(`TANPA MAL ${slug} (${want.size} ep dilewati)`); continue; }
  globalThis.__MAL = MAL;
  const eps = (detail.episodes || []).filter(e => want.has(e.number)).sort((a, b) => a.number - b.number);
  let f = 0, n = 0, x = 0;
  for (const ep of eps) {
    let res = null;
    try { res = await resolveEp(ep.number, MAL); } catch { res = null; }
    if (!res) { n += 1; continue; }
    const subs = res.tracks.length ? res.tracks : (ep.subtitles || []);
    if (DRY) { f += 1; console.log(`  [dry] ${slug} ep${ep.number} -> stream + [${subs.map(s2 => s2.lang).join(',')}]`); continue; }
    const out = await apiPost({ rawTitle: detail.rawTitle, episodeNum: ep.number, streamUrl: res.file, downloadUrl: ep.downloadUrl ?? null, thumbnail: ep.thumbnail || undefined, headers: { Referer: res.embed }, subtitles: subs, meta: null });
    if (!out.ok) { x += 1; console.log(`  GAGAL POST ${slug} ep${ep.number}: ${out.error}`); }
    else { f += 1; console.log(`REPAIR ${slug} ep${ep.number}: [${subs.map(s2 => s2.lang).join(',')}]`); }
    await sleep(150);
  }
  totals.filled += f; totals.noSrc += n; totals.failed += x;
  console.log(`== ${slug}: terisi ${f} | tanpa sumber ${n} | gagal ${x} (MAL ${MAL})`);
}
console.log(`--- repair-stream selesai: terisi ${totals.filled} | tanpa sumber ${totals.noSrc} | gagal POST ${totals.failed} | tanpa MAL ${totals.noMal} ep ---`);

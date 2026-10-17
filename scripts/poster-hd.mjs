// Poster HD — bangun peta extraLarge TERVERIFIKASI dari AniList.
// Sekali jalan via workflow poster-hd.yml (GitHub Actions).
// Ambil daftar anilistId dari katalog live, tanya AniList GraphQL
// Page per 50 id, jeda 2 detik antar permintaan, hormati 429.
// Hanya extraLarge non-null dari API yang disimpan. Tidak menebak URL.
import fs from 'node:fs';

const CATALOG_URL = 'https://my-anime-app.adiyth-arpansa.workers.dev/api/anime';
const ANILIST_URL = 'https://graphql.anilist.co';
const OUT = 'poster-extra-large.json';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchCatalog() {
  for (let i = 0; i < 5; i++) {
    try {
      const res = await fetch(CATALOG_URL, { headers: { 'user-agent': 'poster-hd/1.0' } });
      console.log('catalog HTTP', res.status);
      if (res.ok) {
        const j = await res.json();
        if (Array.isArray(j.data) && j.data.length) return j.data;
      }
    } catch (e) { console.log('catalog error', String(e)); }
    await sleep(4000);
  }
  throw new Error('katalog tidak terbaca');
}

const QUERY = `query ($page: Int, $perPage: Int, $ids: [Int]) { Page(page: $page, perPage: $perPage) { media(id_in: $ids, type: ANIME) { id coverImage { extraLarge large } } } }`;

async function queryChunk(ids) {
  for (let attempt = 0; attempt < 4; attempt++) {
    const res = await fetch(ANILIST_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/json', accept: 'application/json', 'user-agent': 'poster-hd/1.0' },
      body: JSON.stringify({ query: QUERY, variables: { page: 1, perPage: ids.length, ids } }),
    });
    console.log('anilist HTTP', res.status, 'chunk size', ids.length, 'attempt', attempt);
    if (res.status === 429) {
      const ra = Number(res.headers.get('retry-after') || 60);
      console.log('429 retry-after', ra);
      await sleep(Math.min(Math.max(ra, 5), 120) * 1000);
      continue;
    }
    if (res.status === 403) {
      const txt = await res.text();
      throw new Error('BLOCKED_403 ' + txt.slice(0, 300));
    }
    if (!res.ok) {
      const txt = await res.text();
      throw new Error('ANILIST_' + res.status + ' ' + txt.slice(0, 300));
    }
    const j = await res.json();
    if (j.errors) throw new Error('GRAPHQL ' + JSON.stringify(j.errors).slice(0, 500));
    return j.data.Page.media || [];
  }
  throw new Error('429_terus_menerus');
}

async function main() {
  const catalog = await fetchCatalog();
  console.log('catalog titles', catalog.length);
  const ids = [...new Set(catalog.map((a) => a.anilistId).filter((x) => Number.isFinite(x)))];
  console.log('distinct anilistIds', ids.length);
  const map = {};
  const missing = [];
  const errors = [];
  let blocked = false;
  const chunks = [];
  for (let i = 0; i < ids.length; i += 50) chunks.push(ids.slice(i, i + 50));
  for (let ci = 0; ci < chunks.length; ci++) {
    if (ci > 0) await sleep(2000);
    const chunk = chunks[ci];
    try {
      const media = await queryChunk(chunk);
      const seen = new Set();
      for (const m of media) {
        seen.add(m.id);
        const xl = m.coverImage && m.coverImage.extraLarge;
        if (xl) map[String(m.id)] = xl;
        else missing.push({ anilistId: m.id, reason: 'extraLarge null dari API' });
      }
      for (const id of chunk) if (!seen.has(id)) missing.push({ anilistId: id, reason: 'tidak dikembalikan AniList' });
      console.log('chunk', ci + 1, '/', chunks.length, 'ok, map kini', Object.keys(map).length);
    } catch (e) {
      const msg = String(e && e.message || e);
      console.error('chunk gagal', ci, msg);
      errors.push({ chunk: ci, ids: chunk, error: msg });
      if (msg.includes('BLOCKED_403') || msg.includes('manually blocked')) { blocked = true; break; }
      // lanjut chunk lain untuk kegagalan non-blokir
    }
  }
  const out = {
    generatedAt: new Date().toISOString(),
    catalogTitles: catalog.length,
    requestedIds: ids.length,
    verifiedCount: Object.keys(map).length,
    blocked,
    missing,
    errors,
    map,
  };
  fs.writeFileSync(OUT, JSON.stringify(out, null, 1));
  console.log('SELESAI verified', out.verifiedCount, 'missing', missing.length, 'errors', errors.length, 'blocked', blocked);
  if (blocked) process.exitCode = 2;
}

main().catch((e) => { console.error(e); process.exit(1); });

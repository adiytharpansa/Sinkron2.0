// Verifikasi extraLarge untuk 3 anilistId yang hanya ada di D1
// (tidak tampil di daftar GET /api/anime): 176642, 163146, 16870.
// ID diambil dari query D1 langsung 17 Okt 2026 sesi poster-hd.
import fs from 'node:fs';
const IDS = [176642, 163146, 16870];
const QUERY = `query ($page: Int, $perPage: Int, $ids: [Int]) { Page(page: $page, perPage: $perPage) { media(id_in: $ids, type: ANIME) { id coverImage { extraLarge large } } } }`;
const res = await fetch('https://graphql.anilist.co', {
  method: 'POST',
  headers: { 'content-type': 'application/json', accept: 'application/json', 'user-agent': 'poster-extra/1.0' },
  body: JSON.stringify({ query: QUERY, variables: { page: 1, perPage: IDS.length, ids: IDS } }),
});
console.log('anilist HTTP', res.status);
const j = await res.json();
if (j.errors) throw new Error(JSON.stringify(j.errors).slice(0, 400));
const map = {};
for (const m of j.data.Page.media || []) {
  if (m.coverImage && m.coverImage.extraLarge) map[String(m.id)] = m.coverImage.extraLarge;
}
fs.writeFileSync('poster-extra.json', JSON.stringify({ generatedAt: new Date().toISOString(), requestedIds: IDS.length, verifiedCount: Object.keys(map).length, map }, null, 1));
console.log('verified', Object.keys(map).length, JSON.stringify(map));

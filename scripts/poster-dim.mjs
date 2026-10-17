// Ukur dimensi piksel poster (diagnostik) — hanya URL yang sudah
// ada di katalog / peta API terverifikasi. Tidak menebak URL baru.
import fs from 'node:fs';
const ITEMS = [
  ['naruto_current_eq_apiXL', 'https://s4.anilist.co/file/anilistcdn/media/anime/cover/large/bx20-dE6UHbFFg1A5.jpg'],
  ['villainess_current_extraLargePath', 'https://s4.anilist.co/file/anilistcdn/media/anime/cover/extraLarge/bx188139-1qIJfWxym8FX.jpg'],
  ['villainess_apiXL_largePath', 'https://s4.anilist.co/file/anilistcdn/media/anime/cover/large/bx188139-1qIJfWxym8FX.jpg'],
  ['daemons_current_kitsu', 'https://media.kitsu.app/anime/50023/poster_image/large-83bcc1806d52156538574d1eeb8ba4bd.jpeg'],
  ['daemons_apiXL', 'https://s4.anilist.co/file/anilistcdn/media/anime/cover/large/bx195600-moI0UFArtOme.jpg'],
  ['aharen_current_wp', 'https://v2.samehadaku.how/wp-content/uploads/2025/04/Aharen-san-wa-Hakarenai-Season-2.jpg'],
  ['aharen_apiXL', 'https://s4.anilist.co/file/anilistcdn/media/anime/cover/large/bx179979-OiIKSkhJRmvo.jpg'],
  ['jjk_culling_current_aniwaves', 'https://static.aniwaves.ru/resources/thumbnails/200x280/100/510419feaf6ab7c7897bce176c979d6f.jpg'],
  ['jjk_culling_apiXL', 'https://s4.anilist.co/file/anilistcdn/media/anime/cover/large/bx172463-LnXqHzt74SJL.jpg'],
  ['apothecary_apiXL', 'https://s4.anilist.co/file/anilistcdn/media/anime/cover/large/bx195516-MJpUZlOberqH.jpg'],
  ['medium502_current_eq_apiXL', 'https://s4.anilist.co/file/anilistcdn/media/anime/cover/medium/502-qMhEQOjbNK0w.jpg'],
  ['jjk_s2_current_eq_apiXL', 'https://s4.anilist.co/file/anilistcdn/media/anime/cover/large/bx145064-hSNRJM03pvv1.jpg'],
  ['yamishibai15_current_eq_apiXL_png', 'https://s4.anilist.co/file/anilistcdn/media/anime/cover/large/bx194088-lzp3BHNx0tnY.png'],
];
function jpegSize(buf) {
  let i = 2;
  while (i < buf.length - 9) {
    if (buf[i] !== 0xff) { i++; continue; }
    const marker = buf[i + 1];
    if (marker >= 0xc0 && marker <= 0xc3) {
      return { h: buf.readUInt16BE(i + 5), w: buf.readUInt16BE(i + 7) };
    }
    const len = buf.readUInt16BE(i + 2);
    if (!len) break;
    i += 2 + len;
  }
  return null;
}
function pngSize(buf) {
  if (buf.length > 24 && buf[0] === 0x89) return { w: buf.readUInt32BE(16), h: buf.readUInt32BE(20) };
  return null;
}
function webpSize(buf) {
  // VP8X
  if (buf.length > 30 && buf.toString('ascii', 12, 16) === 'VP8X') {
    return { w: 1 + (buf[24] | (buf[25] << 8) | (buf[26] << 16)), h: 1 + (buf[27] | (buf[28] << 8) | (buf[29] << 16)) };
  }
  return null;
}
const out = [];
for (const [name, url] of ITEMS) {
  try {
    const res = await fetch(url, { headers: { 'user-agent': 'poster-dim/1.0' } });
    const buf = Buffer.from(await res.arrayBuffer());
    const size = pngSize(buf) || jpegSize(buf) || webpSize(buf);
    out.push({ name, status: res.status, bytes: buf.length, size });
    console.log(name, res.status, buf.length, JSON.stringify(size));
  } catch (e) {
    out.push({ name, error: String(e) });
    console.log(name, 'ERR', String(e));
  }
  await new Promise((r) => setTimeout(r, 800));
}
fs.writeFileSync('poster-dim.json', JSON.stringify(out, null, 1));

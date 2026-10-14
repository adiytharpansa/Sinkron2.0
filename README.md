# Sinkron2.0

Sinkron otomatis katalog [my-anime](https://my-anime-app.adiyth-arpansa.workers.dev)
yang berjalan di **GitHub Actions** — menggantikan cron lama di VM
(`auto_sync.py` tiap 15 menit) dan tidak lagi memakai jatah request
Cloudflare Worker untuk scraping.

- `scripts/sync.mjs` — skrip sinkron mandiri (Node 20, ES module).
  Port penuh logika `auto_sync.py` + `scraper.py`:
  - **hianimes.se** — sitemap `/watch/` → MAL id → embed Zoko
    (dekode konfigurasi `__P`: base64 + XOR) → m3u8 asli + subtitle.
  - **gogoanime.or.at** — sitemap WordPress → MegaPlay
    (`getSourcesNew`, dekripsi AES-256-CBC) → m3u8 asli.
  - **9anime.or.at** — AJAX daftar server → embed Plyr →
    redirect → MP4 langsung.
  - Metadata dari **AniList** (cache TTL 7 hari), cadangan **Kitsu**
    dengan penjaga relevansi. Judul baru tanpa poster ditunda
    (gerbang thumbnail yang sama seperti di server).
- Episode baru dikirim ke API aplikasi (`POST /api/anime`) yang
  menyimpannya per judul — pola tulis yang sudah hemat kuota D1.

## Kenapa katalog tidak di-upsert langsung ke Supabase?

Situs my-anime membaca katalog dari store aplikasinya sendiri
(D1/KV lewat `/api/anime`); **tidak ada tabel anime/episodes di
Supabase**, jadi menulis katalog ke Supabase tidak akan tampil di web.
Supabase dipakai untuk yang memang perannya di spesifikasi ini:

- `sync_logs` — satu baris per run (mulai, selesai, jumlah judul
  diperiksa/diperbarui, episode ditambah, status, error).
- `sync_hashes` — cermin hash inkremental per judul.
- Satu query ringan di akhir run menjaga proyek Supabase gratis
  tidak ter-*pause* karena menganggur.

## Sinkron inkremental (hemat kuota)

- Keadaan katalog dibaca dari **satu** `GET /api/anime` (ter-cache
  tepi 120 detik). Ringkasannya memuat `episodeNumbers`, `status`,
  dan `totalEpisodes` per judul — tidak ada lagi ratusan GET detail
  per run yang dulu membakar kuota baca D1.
- Hash sha256 per judul disimpan di `.sync-cache.json`
  (dipertahankan antar-run oleh `actions/cache`) dan dicerminkan ke
  `sync_hashes`. Judul yang hash-nya tidak berubah dilewati tanpa
  mengambil satu halaman pun.
- Judul `FINISHED` yang episodenya sudah lengkap dilewati.
- GET detail hanya diambil untuk judul yang benar-benar akan
  ditambah episodenya.
- POST dibatasi `SYNC_MAX` episode per run (bawaan 20 agar muat
  di `timeout-minutes: 10`), dengan timeout + retry backoff dan
  jeda sopan antar request.

## Setup

1. **SQL** — jalankan `supabase/sync_logs.sql` satu kali di
   Supabase SQL Editor (proyek yang sama dengan aplikasi).
   RLS aktif tanpa policy publik; hanya service role yang menulis.
2. **GitHub Secrets** — di repo ini: *Settings → Secrets and
   variables → Actions → New repository secret*:
   - `SUPABASE_URL` — URL proyek Supabase.
   - `SUPABASE_SERVICE_ROLE_KEY` — kunci **service_role**
     (Supabase → Project Settings → API). Jangan pernah menaruh
     kunci ini di kode, di sisi klien, atau di chat.
   Tanpa kedua secret ini sinkron katalog tetap jalan; hanya
   pencatatan ke `sync_logs` yang dilewati.
3. **Jalankan manual** — tab **Actions → Sinkron my-anime →
   Run workflow** (bisa pilih batas episode / mode dry-run).
   Setelah itu workflow berjalan sendiri tiap 30 menit.

## Menjalankan di lokal

```bash
npm ci
node scripts/sync.mjs --dry-run --source 9anime --limit-series 3
node scripts/sync.mjs --max 5
```

Keluar dengan kode non-nol bila run gagal fatal (katalog tidak
terbaca atau semua sumber gagal) sehingga GitHub mengirim
notifikasi kegagalan.

## Status migrasi

- Cron VM lama `my-anime-auto-sync` **dinonaktifkan** setelah repo
  ini ter-push dan skrip teruji — Actions adalah satu-satunya
  mesin sinkron sekarang.
- Endpoint `POST /api/anime` di Worker **tetap ada dan memang
  harus ada**: itu jalur tulis katalognya (dipakai skrip ini).
  Yang dihapus dari Worker adalah *penjadwal* sinkronnya —
  Worker my-anime sejak awal tidak punya cron sinkron bawaan;
  sinkron selalu berjalan di luar (dulu VM, sekarang Actions).
- Halaman publik tidak berubah sama sekali (tidak ada perubahan
  UI), dan katalog dibaca dari cache tepi seperti sebelumnya.
- Catatan batas: kuota tulis harian D1 tetap berlaku untuk
  POST katalog (reset 07:00 WIB) — itu batas penyimpanan situs,
  bukan batas mesin sinkron. Dan situs sumber yang memblokir IP
  pusat data bisa menolak sebagian ekstraksi dari IP GitHub
  (sama seperti dari VM); kegagalan per episode dicatat di log
  run Actions, bukan disembunyikan.

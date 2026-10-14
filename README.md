# Sinkron2.0

Sinkron otomatis katalog [my-anime](https://my-anime-app.adiyth-arpansa.workers.dev)
yang berjalan di **GitHub Actions + Supabase (paket gratis)** —
ter-shard, paralel, dan tahan lama. Scraping TIDAK memakai kuota
request Cloudflare Worker (Worker hanya menerima POST episode
per judul seperti biasa). Tampilan/UI situs tidak berubah.

## Arsitektur

```
GitHub Actions                          Supabase (gratis)
├─ sync-fast.yml  tiap 10 mnt           ├─ sync_logs   (log per shard)
│   matrix 4 shard × mode fast          ├─ sync_hashes (cermin hash)
│   (hanya RELEASING + judul baru)      └─ watchdog pg_cron+pg_net:
├─ sync-full.yml  harian 02:23 WIB          dispatch Actions bila
│   matrix 4 shard × mode full              >60 mnt tanpa sukses,
├─ keepalive.yml  bulanan (commit kecil)    notif Telegram bila
└─ scripts/sync.mjs (Node 20)               >2 jam / 3x gagal beruntun
     │  POST episode baru per judul
     ▼
my-anime-app (Cloudflare Workers)
  POST /api/anime -> simpan per judul (D1/KV)
  POST /api/revalidate (rahasia bersama) -> memo katalog dibuang,
     halaman judul terkait langsung segar
```

- **Shard**: daftar kandidat (union katalog + peta situs 3 sumber)
  dibangun deterministik & diurutkan; shard `i` hanya memproses
  kandidat dengan `index % SHARD_TOTAL === i`. Partisi selalu
  saling lepas & lengkap (ada `--self-test-shard`).
- **Paralel**: di dalam shard, judul diproses bersamaan memakai
  `p-limit` sebesar `CONCURRENCY`. Retry + backoff eksponensial
  + jitter; HTTP 429 dihormati via `Retry-After`; pelacak
  kesehatan per host menaikkan jeda otomatis saat 403/429
  beruntun (sinyal ramp-down, lihat bawah).
- **Hemat kerja**: hash sha256 per judul (fingerprint
  katalog+situs) DAN per episode (stream+unduh+subtitle) di
  `.sync-cache.json` (dipertahankan antar-run oleh
  `actions/cache`, kunci terpisah per mode+shard) — yang tidak
  berubah tidak diambil & tidak dikirim ulang. Metadata AniList
  di-cache TTL 7 hari (Kitsu cadangan). Judul FINISHED lengkap
  dilewati. Keadaan katalog dari SATU `GET /api/anime`.
- **Sumber** (satu-satunya yang terverifikasi hidup):
  hianimes.se (Zoko: base64+XOR -> m3u8), gogoanime.or.at
  (MegaPlay AES-256-CBC -> m3u8), 9anime.or.at (AJAX -> MP4).
  Judul baru tanpa poster ditunda (gerbang thumbnail).

## Kenapa katalog tidak di-upsert langsung ke Supabase?

Situs membaca katalog dari store aplikasinya sendiri (D1/KV
lewat `/api/anime`); **tidak ada tabel anime/episodes di
Supabase** — menulis katalog ke Supabase tidak akan tampil di
web. Supabase dipakai sesuai perannya: `sync_logs`,
`sync_hashes`, watchdog, dan query ringan anti-pause per run.

## Panduan setup (urutan persis)

1. **SQL log** — Supabase SQL Editor: jalankan
   `supabase/sync_logs.sql` (membuat `sync_logs` + `sync_hashes`,
   RLS tanpa policy publik).
2. **GitHub Secrets** — repo ini → *Settings → Secrets and
   variables → Actions → New repository secret*:
   - `SUPABASE_URL` — URL proyek Supabase.
   - `SUPABASE_SERVICE_ROLE_KEY` — kunci **service_role**
     (Supabase → Project Settings → API). Hanya di secret; tidak
     pernah di kode/klien/chat. Tanpa dua secret ini sinkron
     katalog tetap jalan, hanya log yang dilewati.
   - `REVALIDATE_SECRET` — token acak panjang yang SAMA dengan
     yang dipasang di Worker (`wrangler secret put
     REVALIDATE_SECRET` di proyek my-anime-app). Tanpa ini,
     revalidate dilewati (halaman tetap segar <=120 detik).
3. **Watchdog (cadangan + notifikasi)** — Supabase SQL Editor:
   jalankan `supabase/fallback_dispatch.sql`. Sebelumnya:
   - Aktifkan ekstensi `pg_cron` & `pg_net` (SQL file ini sudah
     mencoba; atau Dashboard → Database → Extensions).
   - Buat **fine-grained PAT** GitHub: *Settings → Developer
     settings → Fine-grained tokens* — repository access HANYA
     repo ini, permission **Actions: Read and write** saja.
     Simpan di Supabase Vault (Dashboard → Project Settings →
     Vault): `select vault.create_secret('<PAT>', 'github_dispatch_token');`
   - Notifikasi Telegram (opsional): bot via @BotFather, lalu
     Vault: `telegram_bot_token` + `telegram_chat_id`.
   - Uji: `select public.sync_watchdog();`
4. **Uji jalan** — tab **Actions**:
   - *Sinkron Cepat (RELEASING)* → Run workflow (bawaan:
     20 episode/shard, konkurensi 5). Empat job shard harus
     hijau.
   - *Sinkron Penuh (harian)* → Run workflow sekali untuk
     sapuan awal.
   - Periksa `sync_logs`: satu baris per shard per run, kolom
     `shard` 0–3, `mode` fast/full, status success.
   - Setelah episode baru masuk, buka halaman judulnya: harus
     langsung tampil (revalidate), bukan menunggu 2 menit.
5. **Jadwal otomatis** berjalan sendiri: fast tiap 10 menit,
   full harian 02:23 WIB, keepalive tanggal 1 tiap bulan.

## Ramp-up & ramp-down (PENTING)

Tahap awal (sudah terpasang): **SHARD_TOTAL 4, CONCURRENCY 5**.
Naikkan bertahap bila log bersih selama beberapa hari:

| Tahap | SHARD_TOTAL | CONCURRENCY | Ubah di |
|---|---|---|---|
| 1 (awal) | 4 | 5 | — |
| 2 | 8 | 5 | matrix `shard: [0..7]` + env `SHARD_TOTAL: "8"` di kedua workflow sync |
| 3 | 8 | 8 | input bawaan `concurrency` kedua workflow |

**Ramp-down**: bila log run mulai menunjukkan 403/429 beruntun
dari satu sumber (skrip mencetak peringatan otomatis), turunkan
`CONCURRENCY` dulu (8→5→3), lalu `SHARD_TOTAL` bila perlu —
IP pusat data GitHub dipakai bersama, jangan sampai diblokir
sumber. Kunci cache per shard akan mulai dari nol untuk nomor
shard baru; itu wajar dan hanya berarti satu sapuan lebih penuh.

## Self-hosted runner cadangan (Linux)

Bila runner GitHub bermasalah, job bisa dipindah ke mesin
sendiri tanpa mengubah kode:

1. Di mesin Linux (VPS/PC rumah, x64/ARM64): buat user khusus,
   unduh runner dari repo ini → *Settings → Actions → Runners →
   New self-hosted runner* (perintah resmi per mesin ada di
   sana), jalankan `./config.sh --url <url repo> --token <token>`
   lalu `./run.sh` (atau pasang sebagai service: `sudo ./svc.sh
   install && sudo ./svc.sh start`).
2. Beri label runner, mis. `sinkron` (saat config tambahkan
   `--labels sinkron`).
3. Pindah job: set **Variables** repo → *Settings → Secrets and
   variables → Actions → Variables*: `SYNC_RUNNER` = `sinkron`.
   Kedua workflow sync memakai
   `runs-on: ${{ vars.SYNC_RUNNER || 'ubuntu-latest' }}` — tanpa
   variabel itu mereka tetap di runner GitHub. Kosongkan lagi
   variabelnya untuk kembali.
4. Catatan: IP rumah/VPS justru kadang LEBIH dipercaya sumber
   daripada IP pusat data — runner cadangan juga jalan keluar
   bila sumber memblokir IP GitHub.

## Menjalankan di lokal

```bash
npm ci
node scripts/sync.mjs --self-test-shard
node scripts/sync.mjs --mode fast --shard 0 --shards 4 --concurrency 5 --max 3 --dry-run
node scripts/sync.mjs --mode full --max 5
```

Skrip keluar non-nol bila gagal fatal (katalog tak terbaca /
semua peta sumber gagal) agar GitHub mengirim notifikasi.
Kegagalan per episode dicatat di log run, tidak disembunyikan.

## Batas jujur

- Kuota tulis harian D1 (reset 07:00 WIB) tetap berlaku untuk
  POST katalog — itu batas penyimpanan situs, bukan mesin
  sinkron; mode fast 10-menitan dibatasi SYNC_MAX per shard
  agar tidak menggerusnya.
- Jadwal `schedule` GitHub bisa terlambat beberapa menit saat
  beban tinggi; watchdog Supabase adalah jaring pengamannya.
- Halaman publik tidak berubah sama sekali.

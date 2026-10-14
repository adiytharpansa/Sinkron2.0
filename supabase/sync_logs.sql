-- Sinkron2.0 — tabel log & keadaan sinkron di Supabase.
-- Jalankan SEKALI di Supabase SQL Editor (aman dijalankan
-- ulang: semua pernyataan idempoten).
--
-- Keamanan: RLS aktif TANPA policy publik — anon/authenticated
-- tidak bisa membaca/menulis sama sekali. Hanya service role
-- (dipakai GitHub Actions lewat secret) yang bisa, karena
-- service role melewati RLS. Service role key TIDAK PERNAH
-- masuk kode klien / aplikasi web.

create table if not exists public.sync_logs (
  id             bigint generated always as identity primary key,
  started_at     timestamptz not null default now(),
  finished_at    timestamptz,
  shard          integer not null default 0,
  mode           text not null default 'full',
  titles_checked integer not null default 0,
  titles_updated integer not null default 0,
  episodes_added integer not null default 0,
  status         text not null default 'running'
                 check (status in ('running', 'success', 'failed')),
  error          text
);

-- Untuk pemasangan lama (tabel dibuat sebelum ada shard/mode):
alter table public.sync_logs add column if not exists shard integer not null default 0;
alter table public.sync_logs add column if not exists mode  text    not null default 'full';

create index if not exists sync_logs_started_idx on public.sync_logs (started_at desc);
create index if not exists sync_logs_status_idx  on public.sync_logs (status, finished_at desc);

alter table public.sync_logs enable row level security;
-- Sengaja TIDAK ada policy: akses publik tertutup total.

-- Hash per judul (cermin dari .sync-cache.json di Actions):
-- judul yang hash-nya sama dilewati tanpa pengambilan apa pun.
create table if not exists public.sync_hashes (
  anime_id   text primary key,
  hash       text not null,
  updated_at timestamptz not null default now()
);
alter table public.sync_hashes enable row level security;
-- Sengaja TIDAK ada policy juga di sini.

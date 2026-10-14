-- Sinkron2.0 — tabel log & keadaan sinkron my-anime.
-- Jalankan file ini SATU KALI di Supabase SQL Editor
-- (proyek Supabase yang sama dengan aplikasi my-anime).
--
-- RLS aktif TANPA policy publik: anon/authenticated tidak
-- bisa membaca atau menulis tabel ini sama sekali. Hanya
-- service role (dipakai scripts/sync.mjs dari GitHub
-- Secrets) yang bisa — service role memang melewati RLS.

create table if not exists public.sync_logs (
  id bigint generated always as identity primary key,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  titles_checked integer not null default 0,
  titles_updated integer not null default 0,
  episodes_added integer not null default 0,
  status text not null default 'running'
    check (status in ('running', 'success', 'failed')),
  error text
);

create index if not exists sync_logs_started_at_idx
  on public.sync_logs (started_at desc);

alter table public.sync_logs enable row level security;
-- Sengaja TANPA policy: akses publik tertutup total.

-- Keadaan hash inkremental per judul (cermin dari cache
-- lokal skrip; skrip tetap jalan walau tabel ini kosong).
create table if not exists public.sync_hashes (
  anime_id text primary key,
  hash text not null,
  updated_at timestamptz not null default now()
);

alter table public.sync_hashes enable row level security;
-- Sengaja TANPA policy: hanya service role.

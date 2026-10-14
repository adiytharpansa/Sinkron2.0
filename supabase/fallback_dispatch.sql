-- Sinkron2.0 — PENJAGA CADANGAN (watchdog) di Supabase.
-- Jalankan SEKALI di Supabase SQL Editor SETELAH sync_logs.sql.
--
-- Cara kerja: pg_cron menjalankan public.sync_watchdog()
-- tiap 15 menit.
--   1. Bila TIDAK ada sinkron sukses > 60 menit di sync_logs,
--      watchdog memanggil API GitHub workflow_dispatch untuk
--      workflow "Sinkron Cepat (RELEASING)" (sync-fast.yml) —
--      jadi walau jadwal Actions macet, sinkron dibangkitkan
--      lagi dari sisi Supabase.
--   2. Bila tidak ada sukses > 2 jam, atau 3 run terakhir
--      berturut-turut gagal, watchdog mengirim notifikasi
--      Telegram (maksimal 1x per 3 jam agar tidak spam).
--   3. Setiap jalan juga menyentuh sync_logs (query ringan)
--      agar proyek paket gratis tidak ter-pause.
--
-- SYARAT SEBELUM MENJALANKAN (lihat README "Panduan setup"):
--   a. Aktifkan ekstensi pg_cron & pg_net (bisa dari SQL ini,
--      atau Dashboard > Database > Extensions).
--   b. Buat fine-grained Personal Access Token di GitHub:
--      Settings > Developer settings > Fine-grained tokens —
--      repository access: HANYA Sinkron2.0; permissions:
--      Actions = Read and write (tidak ada izin lain).
--   c. Simpan token itu di Supabase Vault (Dashboard >
--      Project Settings > Vault, atau SQL):
--        select vault.create_secret('<TOKEN>', 'github_dispatch_token');
--      Token TIDAK ditulis di file ini dan TIDAK di kode mana pun.
--   d. (Opsional, notifikasi Telegram) buat bot via @BotFather,
--      lalu simpan dua secret Vault:
--        select vault.create_secret('<TOKEN_BOT>', 'telegram_bot_token');
--        select vault.create_secret('<CHAT_ID>',   'telegram_chat_id');
--      Tanpa keduanya, langkah notifikasi dilewati senyap.

create extension if not exists pg_net;
create extension if not exists pg_cron;

-- Keadaan watchdog (menahan spam notifikasi).
create table if not exists public.sync_watchdog_state (
  id              integer primary key check (id = 1),
  last_notified_at timestamptz,
  last_dispatched_at timestamptz
);
insert into public.sync_watchdog_state (id) values (1)
  on conflict (id) do nothing;
alter table public.sync_watchdog_state enable row level security;
-- Tanpa policy: hanya postgres/service role.

create or replace function public.sync_watchdog()
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  v_last_success timestamptz;
  v_token        text;
  v_tg_token     text;
  v_tg_chat      text;
  v_fail_streak  integer;
  v_state        public.sync_watchdog_state%rowtype;
  v_msg          text := null;
begin
  select * into v_state from public.sync_watchdog_state where id = 1;

  select max(finished_at) into v_last_success
    from public.sync_logs
   where status = 'success' and finished_at is not null;

  -- (1) Pemicu cadangan: >60 menit tanpa sukses -> dispatch.
  if v_last_success is null or v_last_success < now() - interval '60 minutes' then
    select decrypted_secret into v_token
      from vault.decrypted_secrets where name = 'github_dispatch_token' limit 1;
    if v_token is not null
       and (v_state.last_dispatched_at is null
            or v_state.last_dispatched_at < now() - interval '30 minutes') then
      perform net.http_post(
        url := 'https://api.github.com/repos/adiytharpansa/Sinkron2.0/actions/workflows/sync-fast.yml/dispatches',
        headers := jsonb_build_object(
          'Authorization', 'Bearer ' || v_token,
          'Accept', 'application/vnd.github+json',
          'X-GitHub-Api-Version', '2022-11-28',
          'Content-Type', 'application/json',
          'User-Agent', 'sinkron-watchdog'
        ),
        body := jsonb_build_object('ref', 'main')
      );
      update public.sync_watchdog_state
         set last_dispatched_at = now() where id = 1;
    end if;
  end if;

  -- (2) Notifikasi: >2 jam tanpa sukses, atau >=3 gagal beruntun.
  select count(*) into v_fail_streak
    from (
      select status from public.sync_logs
       where finished_at is not null
       order by finished_at desc
       limit 3
    ) t
   where t.status = 'failed';

  if exists (select 1 from public.sync_logs)
     and (v_last_success is null or v_last_success < now() - interval '2 hours') then
    v_msg := 'Sinkron my-anime: belum ada sinkron sukses selama 2 jam terakhir'
             || case when v_last_success is null
                     then ' (belum pernah ada yang sukses tercatat).'
                     else ' (terakhir sukses ' || to_char(v_last_success, 'YYYY-MM-DD HH24:MI') || ' UTC).'
                end;
  end if;
  if v_fail_streak >= 3 then
    v_msg := 'Sinkron my-anime: 3 run terakhir berturut-turut GAGAL. Cek tab Actions repo Sinkron2.0.';
  end if;

  if v_msg is not null
     and (v_state.last_notified_at is null
          or v_state.last_notified_at < now() - interval '3 hours') then
    select decrypted_secret into v_tg_token
      from vault.decrypted_secrets where name = 'telegram_bot_token' limit 1;
    select decrypted_secret into v_tg_chat
      from vault.decrypted_secrets where name = 'telegram_chat_id' limit 1;
    if v_tg_token is not null and v_tg_chat is not null then
      perform net.http_post(
        url := 'https://api.telegram.org/bot' || v_tg_token || '/sendMessage',
        headers := jsonb_build_object('Content-Type', 'application/json'),
        body := jsonb_build_object('chat_id', v_tg_chat, 'text', v_msg)
      );
      update public.sync_watchdog_state
         set last_notified_at = now() where id = 1;
    end if;
  end if;
end;
$$;

-- Jadwalkan tiap 15 menit (idempoten: cabut jadwal lama dulu).
do $$
begin
  if exists (select 1 from cron.job where jobname = 'sinkron-watchdog') then
    perform cron.unschedule('sinkron-watchdog');
  end if;
end $$;
select cron.schedule('sinkron-watchdog', '*/15 * * * *',
                     $$select public.sync_watchdog();$$);

-- Uji manual kapan pun dengan:
--   select public.sync_watchdog();
-- dan periksa hasil panggilan HTTP di net.http_request_queue /
-- net._http_response (skema net) bila dispatch terasa tidak jalan.

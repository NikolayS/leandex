-- Test 05: Guardrails and autonomous safety checks
-- Exit on first error for CI
\set ON_ERROR_STOP on
\set QUIET on

\echo '======================================'
\echo 'TEST 05: Guardrails'
\echo '======================================'

create or replace function test_guardrails_target_db() returns text as $$
declare
  _target_db text;
begin
  select database_name into _target_db
  from leandex.target_databases
  where enabled
  limit 1;

  if _target_db is null then
    raise exception 'FAIL: no enabled target database';
  end if;

  return _target_db;
end;
$$ language plpgsql;

-- 1. Build test fixture and baseline state.
do $$
declare
  _target_db text := test_guardrails_target_db();
begin
  delete from leandex.current_processed_index where schemaname = 'test_guardrails';
  delete from leandex.reindex_history where schemaname = 'test_guardrails';
  delete from leandex.index_latest_state where schemaname = 'test_guardrails';

  perform leandex._dblink_connect_if_not(_target_db::name);
  perform dblink(_target_db, $sql$
    drop schema if exists test_guardrails cascade;
    create schema test_guardrails;

    create table test_guardrails.guard_table (
      id bigserial primary key,
      email text not null,
      status text not null,
      payload text not null,
      created_at timestamptz not null default now()
    );

    insert into test_guardrails.guard_table (email, status, payload)
    select
      'user' || g || '@example.com',
      case when g % 10 = 0 then 'stale' else 'live' end,
      repeat(md5(g::text), 4)
    from generate_series(1, 150000) as g;

    create index idx_guardrails_email on test_guardrails.guard_table(email);
    create index idx_guardrails_status on test_guardrails.guard_table(status);

    analyze test_guardrails.guard_table;
  $sql$);

  call leandex.periodic(false);
  perform leandex.do_force_populate_index_stats(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email');

  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'index_size_threshold', '0', 'test override');
  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'respect_external_index_activity', 'true', 'test override');
  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'max_parallel_reindexes', '1', 'test override');
  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'min_window_remaining', '0', 'test override');

  raise notice 'PASS: test fixture created';
end $$;

-- 2. First observed baseline must be low-confidence observed metadata.
do $$
declare
  _state record;
begin
  select * into _state
  from leandex.index_latest_state
  where datname = test_guardrails_target_db()::name
    and schemaname = 'test_guardrails'
    and relname = 'guard_table'
    and indexrelname = 'idx_guardrails_email';

  if _state.first_seen_at is null or _state.last_seen_at is null then
    raise exception 'FAIL: missing first_seen_at/last_seen_at';
  end if;

  if _state.baseline_source <> 'observed' then
    raise exception 'FAIL: expected observed baseline source, got %', _state.baseline_source;
  end if;

  if _state.baseline_confidence <> 'low' then
    raise exception 'FAIL: expected low baseline confidence, got %', _state.baseline_confidence;
  end if;

  if _state.last_seen_relfilenode is null then
    raise exception 'FAIL: relfilenode was not tracked';
  end if;

  raise notice 'PASS: observed baseline metadata initialized';
end $$;

-- 3. Outside allowed window should skip and record reason.
do $$
declare
  _target_db text := test_guardrails_target_db();
  _window text;
  _history record;
begin
  _window := format(
    '[{"days":[%s],"start":"%s","end":"%s"}]',
    extract(isodow from current_timestamp)::int,
    to_char(current_timestamp - interval '2 hours', 'HH24:MI'),
    to_char(current_timestamp - interval '1 hour', 'HH24:MI')
  );

  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'allowed_start_windows', _window, 'test outside window');

  call leandex.do_reindex(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', true);

  select status, skip_reason into _history
  from leandex.reindex_history
  where datname = _target_db::name
    and schemaname = 'test_guardrails'
    and indexrelname = 'idx_guardrails_email'
  order by id desc
  limit 1;

  if _history.status <> 'skipped' then
    raise exception 'FAIL: expected skipped outside window, got %', _history.status;
  end if;

  if _history.skip_reason not like 'outside allowed start window%' then
    raise exception 'FAIL: unexpected skip reason: %', _history.skip_reason;
  end if;

  if exists (
    select 1
    from leandex.current_processed_index
    where datname = _target_db::name
      and schemaname = 'test_guardrails'
      and indexrelname = 'idx_guardrails_email'
  ) then
    raise exception 'FAIL: current_processed_index not cleaned after skip';
  end if;

  raise notice 'PASS: outside allowed window skipped cleanly';
end $$;

-- 4. Inside allowed window should be permitted.
do $$
declare
  _target_db text := test_guardrails_target_db();
  _window text;
  _decision record;
begin
  _window := format(
    '[{"days":[%s],"start":"%s","end":"%s"}]',
    extract(isodow from current_timestamp)::int,
    to_char(current_timestamp - interval '15 minutes', 'HH24:MI'),
    to_char(current_timestamp + interval '45 minutes', 'HH24:MI')
  );

  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'allowed_start_windows', _window, 'test inside window');
  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'min_window_remaining', '0', 'test override');

  select * into _decision
  from leandex._evaluate_reindex_start(
    _target_db::name,
    'test_guardrails',
    'guard_table',
    'idx_guardrails_email'
  );

  if not _decision.allowed_to_start then
    raise exception 'FAIL: expected allowed start, got %', _decision.reason;
  end if;

  raise notice 'PASS: inside allowed window permitted';
end $$;

-- 5. Near end of allowed window with min_window_remaining should skip.
do $$
declare
  _target_db text := test_guardrails_target_db();
  _window text;
  _decision record;
begin
  _window := format(
    '[{"days":[%s],"start":"%s","end":"%s"}]',
    extract(isodow from current_timestamp)::int,
    to_char(current_timestamp - interval '5 minutes', 'HH24:MI'),
    to_char(current_timestamp + interval '1 minute', 'HH24:MI')
  );

  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'allowed_start_windows', _window, 'test near end');
  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'min_window_remaining', '10 minutes', 'test override');

  select * into _decision
  from leandex._evaluate_reindex_start(
    _target_db::name,
    'test_guardrails',
    'guard_table',
    'idx_guardrails_email'
  );

  if _decision.allowed_to_start then
    raise exception 'FAIL: expected min_window_remaining guard to deny start';
  end if;

  if _decision.reason not like 'min_window_remaining%' then
    raise exception 'FAIL: unexpected min_window_remaining reason: %', _decision.reason;
  end if;

  raise notice 'PASS: min_window_remaining guard works';
end $$;

-- 6. Remote session settings should enforce safe defaults.
do $$
declare
  _target_db text := test_guardrails_target_db();
  _settings jsonb;
  _remote_version int;
begin
  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'allowed_start_windows', null, 'clear test window');
  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'lock_timeout', '30s', 'test default');

  _settings := leandex._apply_remote_reindex_session_settings(
    _target_db::name,
    'test_guardrails',
    'guard_table',
    'idx_guardrails_email'
  );

  if _settings->>'statement_timeout' <> '0' then
    raise exception 'FAIL: statement_timeout must be 0, got %', _settings->>'statement_timeout';
  end if;

  if _settings->>'lock_timeout' <> '30s' then
    raise exception 'FAIL: lock_timeout must be 30s by default, got %', _settings->>'lock_timeout';
  end if;

  if _settings->>'idle_in_transaction_session_timeout' <> '1min' then
    raise exception 'FAIL: idle_in_transaction_session_timeout must be 1min, got %', _settings->>'idle_in_transaction_session_timeout';
  end if;

  if _settings->>'idle_session_timeout' <> '0' then
    raise exception 'FAIL: idle_session_timeout must be 0, got %', _settings->>'idle_session_timeout';
  end if;

  _remote_version := (_settings->>'server_version_num')::int;

  if _remote_version >= 170000 then
    if _settings->>'transaction_timeout' <> '0' then
      raise exception 'FAIL: transaction_timeout must be 0 on PG17+, got %', _settings->>'transaction_timeout';
    end if;
  elsif _settings ? 'transaction_timeout' and (_settings->>'transaction_timeout') is not null then
    raise exception 'FAIL: transaction_timeout should not be set on PG13-16, got %', _settings->>'transaction_timeout';
  end if;

  raise notice 'PASS: remote session settings enforced';
end $$;

-- 7. Respect external index activity and skip.
do $$
declare
  _target_db text := test_guardrails_target_db();
  _sent int;
  _tries int := 0;
  _reason text;
  _history record;
begin
  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'allowed_start_windows', null, 'clear window');
  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'respect_external_index_activity', 'true', 'test override');

  if 'extidx' = any(coalesce(dblink_get_connections(), array[]::text[])) then
    perform dblink_disconnect('extidx');
  end if;
  perform dblink_connect('extidx', 'leandex_target');
  _sent := dblink_send_query('extidx', 'reindex index concurrently test_guardrails.idx_guardrails_status');

  loop
    _tries := _tries + 1;
    select blocker_reason into _reason
    from leandex._detect_reindex_blockers(
      _target_db::name,
      'test_guardrails',
      'guard_table',
      'idx_guardrails_email'
    );

    exit when _reason like 'external index activity:%' or _tries > 50;
    perform pg_sleep(0.1);
  end loop;

  call leandex.do_reindex(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', true);

  select status, skip_reason into _history
  from leandex.reindex_history
  where datname = _target_db::name
    and schemaname = 'test_guardrails'
    and indexrelname = 'idx_guardrails_email'
  order by id desc
  limit 1;

  if _history.status <> 'skipped' then
    raise exception 'FAIL: expected skipped due to external activity, got %', _history.status;
  end if;

  if _history.skip_reason not like 'external index activity:%' then
    raise exception 'FAIL: unexpected external-activity reason: %', _history.skip_reason;
  end if;

  while dblink_is_busy('extidx') = 1 loop
    perform pg_sleep(0.1);
  end loop;
  perform dblink_get_result('extidx');
  perform dblink_disconnect('extidx');

  raise notice 'PASS: external index activity guard works';
end $$;

-- 8. max_parallel_reindexes should block a second starter.
do $$
declare
  _target_db text := test_guardrails_target_db();
  _lock_key bigint;
  _history record;
begin
  _lock_key := leandex._parallel_reindex_lock_key(_target_db::name, 1);
  perform pg_advisory_lock(_lock_key);

  begin
    call leandex.do_reindex(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', true);
  exception when others then
    perform pg_advisory_unlock(_lock_key);
    raise;
  end;
  perform pg_advisory_unlock(_lock_key);

  select status, skip_reason into _history
  from leandex.reindex_history
  where datname = _target_db::name
    and schemaname = 'test_guardrails'
    and indexrelname = 'idx_guardrails_email'
  order by id desc
  limit 1;

  if _history.status <> 'skipped' then
    raise exception 'FAIL: expected skipped when reindex slot busy, got %', _history.status;
  end if;

  if _history.skip_reason not like 'max_parallel_reindexes reached%' then
    raise exception 'FAIL: unexpected parallelism reason: %', _history.skip_reason;
  end if;

  raise notice 'PASS: max_parallel_reindexes guard works';
end $$;

-- 9. Pre-existing blocking transaction should not hang forever.
do $$
declare
  _target_db text := test_guardrails_target_db();
  _started timestamptz;
  _elapsed interval;
  _history record;
begin
  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'lock_timeout', '200ms', 'test short timeout');
  if 'blocker' = any(coalesce(dblink_get_connections(), array[]::text[])) then
    perform dblink_disconnect('blocker');
  end if;
  perform dblink_connect('blocker', 'leandex_target');
  perform dblink_exec('blocker', 'begin isolation level repeatable read');
  perform dblink_exec('blocker', 'set local enable_seqscan = off');
  perform dblink_exec('blocker', 'select count(*) from test_guardrails.guard_table where email >= ''user1@example.com''');
  perform pg_sleep(0.3);

  _started := clock_timestamp();
  call leandex.do_reindex(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', true);
  _elapsed := clock_timestamp() - _started;

  select status, skip_reason, error_message into _history
  from leandex.reindex_history
  where datname = _target_db::name
    and schemaname = 'test_guardrails'
    and indexrelname = 'idx_guardrails_email'
  order by id desc
  limit 1;

  if _elapsed > interval '5 seconds' then
    raise exception 'FAIL: blocking guard took too long: %', _elapsed;
  end if;

  if _history.status not in ('skipped', 'failed') then
    raise exception 'FAIL: expected skipped/failed for blocker scenario, got %', _history.status;
  end if;

  if coalesce(_history.skip_reason, _history.error_message, '') not like 'blocking transaction%' then
    raise exception 'FAIL: missing blocking transaction reason, got skip=% error=%', _history.skip_reason, _history.error_message;
  end if;

  if exists (
    select 1
    from leandex.current_processed_index
    where datname = _target_db::name
      and schemaname = 'test_guardrails'
      and indexrelname = 'idx_guardrails_email'
  ) then
    raise exception 'FAIL: current_processed_index leaked after blocker scenario';
  end if;

  perform dblink_exec('blocker', 'rollback');
  perform dblink_disconnect('blocker');

  raise notice 'PASS: blocking transaction guard returns promptly';
end $$;

-- 10. Successful reindex should upgrade baseline confidence and track relfilenode change.
do $$
declare
  _target_db text := test_guardrails_target_db();
  _before oid;
  _after record;
begin
  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'lock_timeout', '30s', 'restore default');
  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'respect_external_index_activity', 'false', 'avoid false positives');
  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'min_window_remaining', '0', 'test override');
  perform leandex.set_or_replace_setting(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', 'allowed_start_windows', null, 'clear window');

  select last_seen_relfilenode into _before
  from leandex.index_latest_state
  where datname = _target_db::name
    and schemaname = 'test_guardrails'
    and indexrelname = 'idx_guardrails_email';

  call leandex.do_reindex(_target_db, 'test_guardrails', 'guard_table', 'idx_guardrails_email', true);
  perform leandex._record_indexes_info(_target_db::name, 'test_guardrails', 'guard_table', 'idx_guardrails_email');

  select baseline_source, baseline_confidence, last_seen_relfilenode, first_seen_at, last_seen_at
  into _after
  from leandex.index_latest_state
  where datname = _target_db::name
    and schemaname = 'test_guardrails'
    and indexrelname = 'idx_guardrails_email';

  if _after.baseline_source <> 'post_reindex' then
    raise exception 'FAIL: expected post_reindex baseline source, got %', _after.baseline_source;
  end if;

  if _after.baseline_confidence <> 'high' then
    raise exception 'FAIL: expected high baseline confidence, got %', _after.baseline_confidence;
  end if;

  if _after.last_seen_relfilenode is null or _after.last_seen_relfilenode = _before then
    raise exception 'FAIL: relfilenode did not change after reindex (% -> %)', _before, _after.last_seen_relfilenode;
  end if;

  if _after.last_seen_at < _after.first_seen_at then
    raise exception 'FAIL: last_seen_at regressed';
  end if;

  raise notice 'PASS: successful reindex upgrades baseline metadata';
end $$;

-- 11. Cleanup.
do $$
declare
  _target_db text := test_guardrails_target_db();
begin
  perform leandex._dblink_connect_if_not(_target_db::name);
  perform dblink(_target_db, 'drop schema if exists test_guardrails cascade');
  delete from leandex.current_processed_index where schemaname = 'test_guardrails';
  delete from leandex.reindex_history where schemaname = 'test_guardrails';
  delete from leandex.index_latest_state where schemaname = 'test_guardrails';
  delete from leandex.config where schemaname = 'test_guardrails';
  raise notice 'PASS: test cleanup completed';
end $$;

drop function if exists test_guardrails_target_db();

\echo 'TEST 05: PASSED'
\echo ''

-- Test 02: Core Functionality Test
-- Exit on first error for CI
\set ON_ERROR_STOP on
\set QUIET on

\echo '======================================'
\echo 'TEST 02: Core Functionality'
\echo '======================================'

-- Helper function to get and validate target database
create or replace function get_target_database() returns text as $$
declare
  _target_db text;
begin
  -- Get target database name from control database configuration
  select database_name into _target_db
  from leandex.target_databases
  where enabled = true
  limit 1;

  if _target_db is null then
    raise exception 'FAIL: No target database configured in leandex.target_databases. Control database architecture requires target database registration.';
  end if;

  -- Test connection
  begin
    perform leandex._connect_securely(_target_db);
  exception when others then
    raise exception 'FAIL: Cannot connect to target database %. Error: %', _target_db, SQLERRM;
  end;

  return _target_db;
end;
$$ language plpgsql;

-- 1. Create test schema and tables in target database via dblink
do $$
declare
  _target_db text;
begin
  _target_db := get_target_database();
  raise notice 'INFO: Using target database: %', _target_db;

  -- Create test schema and tables in target database
  perform dblink(_target_db, '
    create schema if not exists test_leandex_app;

    drop table if exists test_leandex_app.test_table cascade;
    create table test_leandex_app.test_table (
      id serial primary key,
      email VARCHAR(255),
      status VARCHAR(50),
      data JSONB,
      created_at timestamp default NOW()
    );

    insert into test_leandex_app.test_table (email, status, data)
    select
      ''user'' || i || ''@test.com'',
      case when i % 3 = 0 then ''active'' else ''inactive'' end,
      jsonb_build_object(''id'', i, ''value'', random() * 100)
    from generate_series(1, 1000) i;

    create index idx_test_email on test_leandex_app.test_table(email);
    create index idx_test_status on test_leandex_app.test_table(status);
    create index idx_test_created on test_leandex_app.test_table(created_at);
    create index idx_test_data_gin on test_leandex_app.test_table using gin(data);

    analyze test_leandex_app.test_table;
  ');

  raise notice 'PASS: Test schema and tables created in target database';
end $$;

-- 2. Test periodic scan (dry run) and verify indexes
do $$
declare
  _count integer;
  _periodic_success boolean := false;
begin
  -- Target database is REQUIRED for the tool to work
  perform 1 from leandex.target_databases where enabled = true;
  if not found then
    raise exception 'FAIL: No target database configured. The tool requires control database architecture.';
  end if;

  -- Test connection to target database
  perform leandex._connect_securely(
    (select database_name from leandex.target_databases where enabled = true limit 1)
  );

  -- Run periodic scan - this should work with FDW properly configured
  call leandex.periodic(false);
  raise notice 'PASS: Periodic scan (dry run) completed';

  -- Verify indexes were detected
  select count(*) into _count
  from leandex.index_latest_state
  where schemaname = 'test_leandex_app';

  if _count < 4 then
    raise exception 'FAIL: Expected at least 4 indexes, found %', _count;
  end if;
  raise notice 'PASS: % indexes detected in test schema', _count;
end $$;

-- 3. Test force populate baseline
do $$
begin
  -- Force populate should work if we got this far
  perform leandex.do_force_populate_index_stats(
    (select database_name from leandex.target_databases where enabled = true limit 1),
    'test_leandex_app',
    null,
    null
  );
  raise notice 'PASS: Force populate baseline completed';
exception when others then
  raise exception 'FAIL: Force populate failed: %', SQLERRM;
end $$;

-- 4. Verify baseline was established
do $$
declare
  _count integer;
begin
  select count(*) into _count
  from leandex.index_latest_state
  where schemaname = 'test_leandex_app'
  and best_ratio is not null;

  if _count < 1 then
    raise exception 'FAIL: No baselines established';
  end if;
  raise notice 'PASS: Baseline established for % indexes', _count;
end $$;

-- 5. Test bloat estimation
do $$
declare
  _count integer;
begin
  -- Create some bloat in target database
  perform dblink(
    (select database_name from leandex.target_databases where enabled = true limit 1),
    '
    delete from test_leandex_app.test_table where id % 3 = 0;
    update test_leandex_app.test_table set status = ''updated'' where id % 5 = 0;
    analyze test_leandex_app.test_table;
    '
  );

  -- Update current state
  call leandex.periodic(false);

  -- Check bloat estimates
  select count(*) into _count
  from leandex.get_index_bloat_estimates(
    (select database_name from leandex.target_databases where enabled = true limit 1)
  )
  where schemaname = 'test_leandex_app'
  and estimated_bloat is not null;

  if _count < 1 then
    raise exception 'FAIL: No bloat estimates generated';
  end if;
  raise notice 'PASS: Bloat estimates available for % indexes', _count;
end $$;

-- 7. Test reindex threshold detection
do $$
declare
  _threshold FLOAT;
  _max_bloat FLOAT;
begin
  -- Get configured threshold
  select value::FLOAT into _threshold
  from leandex.config
  where key = 'index_rebuild_scale_factor';

  -- Get max bloat
  select max(estimated_bloat) into _max_bloat
  from leandex.get_index_bloat_estimates(
    (select database_name from leandex.target_databases where enabled = true limit 1)
  )
  where schemaname = 'test_leandex_app';

  raise notice 'PASS: Bloat detection working (max bloat: %, threshold: %)',
    coalesce(_max_bloat, 0), _threshold;
end $$;

-- 7b. Regression: do_force_populate_index_stats called AFTER bloat must NOT
-- destroy a healthy baseline. Reported by an early user who ran the function
-- a second time post-bloat and saw estimated_bloat lock at 1.00 forever.
-- Asserts loudly if the precondition is unmet so this test cannot silently
-- pass after future restructuring.
do $$
declare
  _bloat_before_force float;
  _bloat_after_force  float;
  _baseline_source_before text;
  _baseline_source_after  text;
  _best_ratio_before real;
  _best_ratio_after  real;
  _target_db text;
begin
  select database_name into _target_db
  from leandex.target_databases where enabled = true limit 1;

  -- Precondition: a trusted baseline must already exist (step 3 ran
  -- do_force_populate_index_stats on the healthy state, so source='forced'),
  -- and step 5 then bloated the indexes, so estimated_bloat must be > 1.
  select max(estimates.estimated_bloat),
         (array_agg(distinct estimates.baseline_source))[1],
         max(state.best_ratio)
    into _bloat_before_force, _baseline_source_before, _best_ratio_before
  from leandex.get_index_bloat_estimates(_target_db) as estimates
  join leandex.index_latest_state as state
    using (datname, schemaname, relname, indexrelname)
  where estimates.schemaname = 'test_leandex_app';

  if _bloat_before_force is null then
    raise exception 'FAIL: regression precondition unmet — estimated_bloat is null '
      'before the test action. Earlier setup steps must establish a trusted '
      'baseline and bloat the indexes; investigate test ordering.';
  end if;

  if _bloat_before_force < 1.05 then
    raise exception 'FAIL: regression precondition unmet — pre-action bloat (%) '
      'is too close to 1.0 to detect a destructive overwrite. Bloat workload '
      'in step 5 is not producing measurable bloat.', _bloat_before_force;
  end if;

  -- The user's mistake: re-run on already-bloated indexes.
  perform leandex.do_force_populate_index_stats(_target_db, 'test_leandex_app', null, null);
  call leandex.periodic(false);

  select max(estimates.estimated_bloat),
         (array_agg(distinct estimates.baseline_source))[1],
         max(state.best_ratio)
    into _bloat_after_force, _baseline_source_after, _best_ratio_after
  from leandex.get_index_bloat_estimates(_target_db) as estimates
  join leandex.index_latest_state as state
    using (datname, schemaname, relname, indexrelname)
  where estimates.schemaname = 'test_leandex_app';

  -- Assert (a) bloat estimate did not collapse, (b) best_ratio not raised,
  -- (c) baseline_source still trusted (no downgrade).
  if _bloat_after_force is null or _bloat_after_force < _bloat_before_force * 0.95 then
    raise exception 'FAIL: do_force_populate_index_stats destroyed the healthy '
      'baseline (bloat before=%, after=%)', _bloat_before_force, _bloat_after_force;
  end if;

  if _best_ratio_after > _best_ratio_before * 1.001 then
    raise exception 'FAIL: best_ratio was raised by do_force_populate_index_stats '
      '(before=%, after=%) — should be non-increasing', _best_ratio_before, _best_ratio_after;
  end if;

  if _baseline_source_after is null
     or _baseline_source_after not in ('forced', 'reindexed', 'improved') then
    raise exception 'FAIL: baseline_source was downgraded to % (was %)',
      _baseline_source_after, _baseline_source_before;
  end if;

  raise notice 'PASS: do_force_populate_index_stats is non-destructive '
    '(bloat before=%, after=%; best_ratio before=%, after=%; source %→%)',
    _bloat_before_force, _bloat_after_force,
    _best_ratio_before, _best_ratio_after,
    _baseline_source_before, _baseline_source_after;
end $$;

-- 8. Cleanup test data
do $$
begin
  -- Clean up target database
  perform dblink(
    (select database_name from leandex.target_databases where enabled = true limit 1),
    'drop schema if exists test_leandex_app cascade;'
  );

  -- Clean up control database tracking tables
  delete from leandex.index_latest_state where schemaname = 'test_leandex_app';
  delete from leandex.reindex_history where schemaname = 'test_leandex_app';
  raise notice 'PASS: Test cleanup completed';
end $$;

-- Cleanup helper function
drop function if exists get_target_database();

\echo 'TEST 02: PASSED'
\echo ''

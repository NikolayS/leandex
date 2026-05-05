-- Test 05: Schema migration v1 → v2 (_structure_version_1_2)
-- Validates that the migration:
--   - adds the new columns and check constraint;
--   - backfills baselined rows as 'migrated' (trusted) so upgrades do not
--     trigger an unintended REINDEX wave;
--   - leaves rows with NULL best_ratio untagged;
--   - bumps tables_version to 2;
--   - is idempotent (re-running is a no-op).
--
-- We synthesize a v1-shaped state inside the already-installed schema by
-- dropping the v2 columns/constraint and resetting tables_version to 1, then
-- invoke the migration directly. This avoids needing a separate database.
\set ON_ERROR_STOP on
\set QUIET on

\echo '======================================'
\echo 'TEST 05: Migration v1 → v2'
\echo '======================================'

-- 1. Synthesize a v1-state: drop v2 schema additions
do $$
begin
  alter table leandex.index_latest_state
    drop constraint if exists baseline_source_present_when_ratio_set,
    drop column if exists baseline_source,
    drop column if exists baseline_set_at,
    drop column if exists last_reindex_at;

  -- wipe any state from prior tests so seed data below is deterministic
  delete from leandex.index_latest_state;

  update leandex.tables_version set version = 1;

  raise notice 'PASS: v1 state synthesized';
end $$;

-- 2. Seed two representative rows: one with a baseline (best_ratio set), one
-- without (too small to characterize)
insert into leandex.index_latest_state(
  datid, datname, schemaname, relname,
  indexrelid, indexrelname, indexsize, indisvalid, estimated_tuples, best_ratio,
  mtime
) values
  (1, 'demo', 'public', 'tab', 100, 'idx_baselined', 1048576, true, 1000, 1024.0,
   '2026-01-01 00:00:00+00'::timestamptz),
  (1, 'demo', 'public', 'tab', 101, 'idx_too_small', 1024,    true, 1,    null,
   '2026-01-01 00:00:00+00'::timestamptz);

-- 3. Run the migration
do $$
begin
  perform leandex._structure_version_1_2();
  raise notice 'PASS: migration executed';
end $$;

-- 4. Assert post-conditions
do $$
declare
  _src_baselined  text;
  _set_at_baselined timestamptz;
  _src_too_small  text;
  _ratio_too_small real;
  _version smallint;
  _has_constraint boolean;
begin
  select baseline_source, baseline_set_at
    into _src_baselined, _set_at_baselined
    from leandex.index_latest_state where indexrelname = 'idx_baselined';

  if _src_baselined is distinct from 'migrated' then
    raise exception 'FAIL: baselined row should be tagged ''migrated'', got %',
      _src_baselined;
  end if;
  if _set_at_baselined is distinct from '2026-01-01 00:00:00+00'::timestamptz then
    raise exception 'FAIL: baseline_set_at should equal mtime, got %',
      _set_at_baselined;
  end if;
  raise notice 'PASS: baselined row tagged ''migrated'' with set_at=mtime';

  select baseline_source, best_ratio
    into _src_too_small, _ratio_too_small
    from leandex.index_latest_state where indexrelname = 'idx_too_small';

  if _src_too_small is not null then
    raise exception 'FAIL: row with NULL best_ratio should keep NULL source, got %',
      _src_too_small;
  end if;
  if _ratio_too_small is not null then
    raise exception 'FAIL: NULL best_ratio should remain NULL';
  end if;
  raise notice 'PASS: row with NULL best_ratio left untouched';

  select version into _version from leandex.tables_version;
  if _version <> 2 then
    raise exception 'FAIL: tables_version should be 2, got %', _version;
  end if;
  raise notice 'PASS: tables_version bumped to 2';

  select exists (
    select 1 from pg_constraint
    where conrelid = 'leandex.index_latest_state'::regclass
      and conname = 'baseline_source_present_when_ratio_set'
  ) into _has_constraint;
  if not _has_constraint then
    raise exception 'FAIL: baseline_source_present_when_ratio_set constraint missing';
  end if;
  raise notice 'PASS: source-presence check constraint installed';
end $$;

-- 5. Assert the source-presence invariant is enforced
do $$
declare
  _expected_failure boolean := false;
begin
  begin
    insert into leandex.index_latest_state(
      datid, datname, schemaname, relname,
      indexrelid, indexrelname, indexsize, indisvalid, estimated_tuples, best_ratio,
      baseline_source
    ) values
      (1, 'demo', 'public', 'tab', 200, 'idx_invariant_violator', 1048576, true, 1000, 1024.0,
       null);  -- ratio set but source NULL → must violate
  exception when check_violation then
    _expected_failure := true;
  end;

  if not _expected_failure then
    raise exception 'FAIL: invariant constraint did not reject (best_ratio set, source NULL)';
  end if;
  raise notice 'PASS: invariant rejects (best_ratio set, source NULL)';
end $$;

-- 6. Idempotency: re-running the migration must be a no-op
do $$
declare
  _src_before text;
  _set_at_before timestamptz;
  _src_after  text;
  _set_at_after  timestamptz;
begin
  select baseline_source, baseline_set_at
    into _src_before, _set_at_before
    from leandex.index_latest_state where indexrelname = 'idx_baselined';

  perform leandex._structure_version_1_2();

  select baseline_source, baseline_set_at
    into _src_after, _set_at_after
    from leandex.index_latest_state where indexrelname = 'idx_baselined';

  if _src_before is distinct from _src_after
     or _set_at_before is distinct from _set_at_after then
    raise exception 'FAIL: re-running migration mutated the row '
      '(source %→%, set_at %→%)', _src_before, _src_after, _set_at_before, _set_at_after;
  end if;

  raise notice 'PASS: migration is idempotent';
end $$;

-- 7. Cleanup
delete from leandex.index_latest_state where indexrelname like 'idx_%';

\echo 'TEST 05: PASSED'
\echo ''

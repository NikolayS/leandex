-- leandex single-file installer
--
-- Installs the leandex schema, tables, functions, and FDW/dblink helpers.
-- Run this in the leandex control database, normally named leandex_control.
--
-- Example:
--   psql -d leandex_control
--   \i leandex.sql

begin;

do $$
begin
  assert
    current_setting('server_version_num', false)::int >= 130000,
    'Postgres 13 or higher is required.';

  raise notice 'Installing leandex into control database "%".', current_database();
end; $$;

create schema leandex;

/*
 * Settings table
 */
create table leandex.config (
  id bigserial primary key,
  datname name,
  schemaname name,
  relname name,
  indexrelname name,
  "key" text not null,
  "value" text,
  "comment" text
);

alter table leandex.config add constraint inherit_check1 check (
  indexrelname is null
  or
    indexrelname is not null
    and relname is not null
);
alter table leandex.config add constraint inherit_check2 check (
  relname is null
  or
    relname is not null
    and schemaname is not null
);
alter table leandex.config add constraint inherit_check3 check (
  schemaname is null
  or
    schemaname is not null
    and datname is not null
);

create unique index config_u1 on leandex.config(key) where datname is null;
create unique index config_u2 on leandex.config(key, datname) where schemaname is null;
create unique index config_u3 on leandex.config(key, datname, schemaname) where relname is null;
create unique index config_u4 on leandex.config(key, datname, schemaname, relname) where indexrelname is null;
create unique index config_u5 on leandex.config(key, datname, schemaname, relname, indexrelname);

-- Default "global" settings
insert into leandex.config (
  key,
  value,
  "comment"
) values (
  'index_size_threshold',
  '10MB',
  'ignore indexes smaller than 10MB, unless there are forced entries in the history'
), (
  'index_rebuild_scale_factor',
  '2',
  'rebuild indexes if the estimated bloat is more than 2x the original size'
), (
  'minimum_reliable_index_size',
  '128kB',
  'indexes smaller than this are not reliable for bloat estimation'
), (
  'reindex_history_retention_period',
  '10 years',
  'default retention period for reindex history'
), (
  'lock_timeout',
  '30s',
  'remote lock_timeout applied before reindex'
), (
  'idle_in_transaction_session_timeout',
  '1min',
  'remote idle_in_transaction_session_timeout applied before reindex'
), (
  'idle_session_timeout',
  '0',
  'remote idle_session_timeout applied before reindex'
), (
  'max_parallel_reindexes',
  '1',
  'maximum concurrent reindexes per target database'
), (
  'respect_external_index_activity',
  'true',
  'skip reindex start if external create index or reindex activity is active'
), (
  'min_window_remaining',
  '0',
  'minimum remaining time in an allowed start window before a reindex may start'
);

-- Default database-level setting
insert into leandex.config (
  datname,
  schemaname,
  relname,
  indexrelname,
  "key",
  "value",
  "comment"
) values (
  '*',
  'repack',
  null,
  null,
  'skip',
  'true',
  'skip repack internal schema'
), (
  '*',
  'pgq',
  'event_*',
  null,
  'skip',
  'true',
  'skip pgq transient tables'
);


/*
 * Databases to manage with leandex
 */
create table leandex.target_databases (
  id bigserial primary key,
  database_name name not null unique,
  host text not null default 'localhost',
  port integer not null default 5432,
  fdw_server_name name not null unique,
  enabled boolean default true,
  added_at timestamptz default now(),
  last_checked_at timestamptz,
  notes text
);


/*
 * History of REINDEX operations
 */
create table leandex.reindex_history (
  id bigserial primary key,
  entry_timestamp timestamptz not null default now(),
  datid oid,
  datname name not null,
  schemaname name not null,
  relname name not null,
  indexrelid oid,
  indexrelname name not null,
  server_version_num integer not null default current_setting('server_version_num')::integer,
  indexsize_before bigint not null,
  indexsize_after bigint,  -- null while REINDEX is in progress or failed
  estimated_tuples bigint not null,
  reindex_duration interval,  -- null while REINDEX is in progress or failed
  analyze_duration interval,  -- null while REINDEX is in progress or failed
  status text not null default 'completed' check (status in ('in_progress', 'completed', 'failed', 'skipped')),
  skip_reason text,
  error_message text
);

create index reindex_history_oid_index on leandex.reindex_history(datid, indexrelid);
create index reindex_history_index on leandex.reindex_history(datname, schemaname, relname, indexrelname);
create index reindex_history_datname_index on leandex.reindex_history(datname);
create index reindex_history_timestamp_index on leandex.reindex_history(entry_timestamp);
create index reindex_history_status_index on leandex.reindex_history(status);


/*
 * Latest state of indexes for bloat estimation
 */
create table leandex.index_latest_state (
  id bigserial primary key,
  mtime timestamptz not null default now(),
  datid oid not null,
  datname name not null,
  schemaname name not null,
  relname name not null,
  indexrelid oid not null,
  indexrelname name not null,
  indexsize bigint not null,
  indisvalid boolean not null default true,
  estimated_tuples bigint not null,
  best_ratio real,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  last_seen_relfilenode oid,
  baseline_source text not null default 'observed' check (baseline_source in ('observed', 'post_reindex', 'manual')),
  baseline_confidence text not null default 'low' check (baseline_confidence in ('low', 'high'))
);

create unique index index_latest_state_oid_index on leandex.index_latest_state(datid, indexrelid);
create index index_latest_state_index on leandex.index_latest_state(datname, schemaname, relname, indexrelname);
create index index_latest_state_datname_index on leandex.index_latest_state(datname);
create index index_latest_state_datid_index on leandex.index_latest_state(datid);
create index index_latest_state_indisvalid_index on leandex.index_latest_state(indisvalid);


/*
 * History view – formatted view of reindex history, better for human consumption
 */
create view leandex.history as
  select date_trunc('second', entry_timestamp)::timestamp as ts,
    datname as db, -- Use datname for database identification
    schemaname as schema,
    relname as table,
    indexrelname as index,
    pg_size_pretty(indexsize_before) as size_before,
    pg_size_pretty(indexsize_after) as size_after,
    (indexsize_before::float / nullif(indexsize_after, 0))::numeric(12, 2) as ratio,
    estimated_tuples as tuples,
    date_trunc('seconds', reindex_duration) as duration,
    status,
    left(coalesce(skip_reason, error_message), 100) as error
  from leandex.reindex_history order by id desc;


/*
 * Current version of table structure
 */
create table leandex.tables_version (
	version smallint not null
);

create unique index tables_version_single_row on leandex.tables_version((version is not null));

insert into leandex.tables_version values(2);


/*
 * Current processed index; can be invalid
 */
create table leandex.current_processed_index (
  id bigserial primary key,
  mtime timestamptz not null default now(),
  datname name not null,
  schemaname name not null,
  relname name not null,
  indexrelname name not null
);

commit;


-- leandex functions

begin;

-- Turn off useless (in this particular case) NOTICE noise
set client_min_messages to warning;

/*
 * Get current version of leandex
 * Returns version string for compatibility checks and diagnostics
 */
create function leandex.version() returns text as $body$
  select '0.1.beta1';
$body$ language sql immutable;


/*
 * Check if PostgreSQL version has critical REINDEX CONCURRENTLY bugs fixed
 * Returns true for PG versions safe for concurrent reindexing (12.10+, 13.6+, 14.4+)
 */
create function leandex._check_pg_version_bugfixed() returns boolean as
$body$
  /* Fixes not covered here:
       - 17.6, 16.10: fix in BRIN (rebuild required) -- we don't support BRIN yet
       - 16.2 (and backpatches): rare GIN corruption fixed in f76b975d5 (TODO: evaluate if it's worth including)
       - 15.8 (and backpatches): a fix related to SP-GiST (TODO: evaluate if it's worth including)
       - pre-PG12 fixes – we now have PG13+ as a requirement
  */
  select (
    (
      /* PG12.10 fix: Enforce standard locking protocol for TOAST table updates,
         to prevent problems with REINDEX CONCURRENTLY
         https://gitlab.com/postgres/postgres/-/commit/5ed74d874
        ("Fix corruption of toast indexes with REINDEX CONCURRENTLY") */
      current_setting('server_version_num')::integer >= 120010
      and current_setting('server_version_num')::integer < 130000
    ) or (
      /* PG13.6 fix: Enforce standard locking protocol for TOAST table updates,
         to prevent problems with REINDEX CONCURRENTLY (Michael Paquier)
         https://gitlab.com/postgres/postgres/-/commit/9acea52ea
         ("Fix corruption of toast indexes with REINDEX CONCURRENTLY") */
      current_setting('server_version_num')::integer >= 130006
      and current_setting('server_version_num')::integer < 140000
    ) or (
      /* PG14.4 fix: Prevent possible corruption of indexes created or rebuilt
         with the CONCURRENTLY option (Álvaro Herrera)
         https://gitlab.com/postgres/postgres/-/commit/042b584c7
         ("Revert changes to CONCURRENTLY that "sped up" Xmin advance") */
      current_setting('server_version_num')::integer >= 140004
    )
  );
$body$
language sql;


/*
 * Check if PostgreSQL 14 version has critical REINDEX CONCURRENTLY bug fixed
 * Returns false for dangerous PG 14.0-14.3 versions (bug #17485)
 */
create function leandex._check_pg14_version_bugfixed() returns boolean as
$body$
  select
    current_setting('server_version_num')::integer < 140000
    or current_setting('server_version_num')::integer >= 140004;
$body$
language sql;

/*
 * Validate PostgreSQL version safety and raise appropriate warnings/errors
 * Raises EXCEPTION for PG 14.0-14.3, WARNING for other affected versions
 */
create function leandex._validate_pg_version() returns void as
$body$
begin
  if not leandex._check_pg14_version_bugfixed() then
    raise exception using
      message = format(
        'The database version %s is affected by PostgreSQL BUG #17485 which makes using leandex unsafe, please update to latest minor release.',
        current_setting('server_version')
      ),
      detail = 'See https://www.postgresql.org/message-id/202205251144.6t4urostzc3s@alvherre.pgsql';
  end if;

  if not leandex._check_pg_version_bugfixed() then
    raise warning using
      message = format(
        'The database version %s is affected by PostgreSQL bugs which make using leandex potentially unsafe, please update to latest minor release.',
        current_setting('server_version')
      ),
      detail =
        'See https://www.postgresql.org/message-id/E1mumI4-0001Zp-PB@gemulon.postgresql.org '
        'and https://www.postgresql.org/message-id/E1n8C7O-00066j-Q5@gemulon.postgresql.org';
  end if;
end;
$body$
language plpgsql;


/*
 * Installation-time safety validation
 * Blocks unsafe deployments: enforces PG13+ requirement and detects known bugs
 */
do $$
begin
  if current_setting('server_version_num')::int < 130000 then
    raise exception 'leandex requires PostgreSQL 13 or higher; version in use: %.',
    current_setting('server_version');
  end if;

  -- Validate PostgreSQL version safety
  perform leandex._validate_pg_version();
end;
$$;

/*
 * Comprehensive environment validation for leandex setup
 * Complete preflight check: version, extensions, schema, permissions, FDW connectivity
 */
create function leandex.check_environment()
returns table(
  component text,
  is_ok boolean,
  details text
) as
$body$
declare
  _missing_permissions_count integer;
  _res record;
  _fdw_self_ok boolean := false;
begin
  -- PostgreSQL version
  return query select
    'PostgreSQL version (>=13)'::text,
    (current_setting('server_version_num')::int >= 130000),
    current_setting('server_version');

  -- Known bugfix statuses
  return query select
    'Known bugs fixed'::text,
    leandex._check_pg_version_bugfixed(),
    case when leandex._check_pg_version_bugfixed() then 'Minor version is safe' else 'Upgrade to latest minor recommended' end;

  return query select
    'PG14 bug #17485 fixed'::text,
    leandex._check_pg14_version_bugfixed(),
    case when leandex._check_pg14_version_bugfixed() then 'Not affected' else 'Update to 14.4 or newer' end;

  -- Extensions
  return query select
    'Extension: dblink'::text,
    exists (select 1 from pg_extension where extname = 'dblink'),
    'Run: create extension dblink;';

  return query select
    'Extension: postgres_fdw'::text,
    exists (select 1 from pg_extension where extname = 'postgres_fdw'),
    'Run: create extension postgres_fdw;';

  -- Schema presence
  return query select
    'Schema: leandex'::text,
    exists (select 1 from pg_namespace where nspname = 'leandex'),
    '';

  -- Required tables
  for _res in
    select unnest(array[
      'config',
      'index_latest_state',
      'reindex_history',
      'current_processed_index',
      'tables_version'
    ]) as tbl
  loop
    return query select
      format('Table: %I.%I', 'leandex', _res.tbl),
      exists (
        select
        from information_schema.tables
        where table_schema = 'leandex' and table_name = _res.tbl
      ),
      '';
  end loop;

  -- Core routines presence
  for _res in
    select unnest(array[
      'version',
      'periodic',
      'do_reindex',
      'get_index_bloat_estimates',
      'check_permissions'
    ]) as func
  loop
    return query select
      format('Function: %I.%I(..)', 'leandex', _res.func),
      exists (
        select
        from pg_proc as p
        join pg_namespace as n on p.pronamespace = n.oid
        where n.nspname = 'leandex' and p.proname = _res.func
      ),
      '';
  end loop;

  -- Permissions summary
  select count(*) into _missing_permissions_count
  from leandex.check_permissions() as p
  where p.status = false;

  return query select
    'Permissions summary'::text,
    (_missing_permissions_count = 0),
    format('Missing: %s', _missing_permissions_count);

  -- FDW security status (detailed lines)
  for _res in select * from leandex.check_fdw_security_status() loop
    return query
    select
      format('FDW: %s', _res.component)::text,
      (lower(_res.status) in ('ok','installed','granted','exists','secure','configured')),
      _res.details::text;
  end loop;

  -- Control DB architecture checks
  return query select
    'Control DB: table leandex.target_databases'::text,
    exists (
      select 1 from information_schema.tables where table_schema = 'leandex' and table_name = 'target_databases'
    ),
    'Required for multi-database control mode';

  if exists (
    select 1
    from information_schema.tables
    where
      table_schema = 'leandex'
      and table_name = 'target_databases'
  ) then
    return query
    select
      'Control DB: registered targets'::text,
      ((select count(*) from leandex.target_databases where enabled) > 0),
      (select string_agg(database_name, ', ') from leandex.target_databases);

    return query
    select
      'Safety: current DB not listed as target'::text,
      not exists (
        select 1
        from leandex.target_databases
        where database_name = current_database()
        ),
      'Do not register the control database as a target';
  end if;

  -- Best-effort FDW connectivity test (use any enabled target's fdw_server_name)
  begin
    perform dblink_connect(
      'env_test',
      coalesce(
        (select fdw_server_name from leandex.target_databases where enabled limit 1),
        null
      )
    );
    perform dblink_disconnect('env_test');
    _fdw_self_ok := true;
  exception when others then
    _fdw_self_ok := false;
  end;

  return query select
    'FDW self-connection test'::text,
    _fdw_self_ok,
    case when _fdw_self_ok then 'Connected via user mapping' else 'Ensure at least one enabled target with valid user mapping' end;

  return;
end;
$body$
language plpgsql;


-- Install dblink extension for remote database operations
-- Note: postgres_fdw is NOT installed here - it should already be configured
-- with proper servers and user mappings to register target databases
create extension if not exists dblink;

/*
 * Validate table structure version meets minimum requirements
 * Throws exception if schema is outdated and needs upgrade
 */
create function leandex._check_structure_version() returns void as
$body$
declare
  _tables_version integer;
  _required_version integer := 2;
begin
  select version into strict _tables_version from leandex.tables_version;

  if (_tables_version < _required_version) then
    raise exception using
      message = format(
        'Current tables version %s is less than minimally required %s for %s code version.',
        _tables_version,
        _required_version,
        leandex.version()
      ),
      hint = 'Update tables structure.';
  end if;
end;
$body$
language plpgsql;


/*
 * Automatically upgrade table structure to required version
 * Performs incremental schema migrations using version-specific upgrade functions
 */
create function leandex.check_update_structure_version() returns void as
$body$
declare
   _tables_version integer;
   _required_version integer := 2;
begin
  select version into strict _tables_version from leandex.tables_version;

  while (_tables_version < _required_version) loop
    execute format(
      'select leandex._structure_version_%s_%s()',
      _tables_version,
      _tables_version + 1
    );

    _tables_version := _tables_version + 1;
  end loop;

  return;
end;
$body$
language plpgsql;


create function leandex._structure_version_1_2() returns void as
$body$
begin
  alter table leandex.reindex_history
    drop constraint if exists reindex_history_status_check;

  alter table leandex.reindex_history
    add constraint reindex_history_status_check
    check (status in ('in_progress', 'completed', 'failed', 'skipped'));

  alter table leandex.reindex_history
    add column if not exists skip_reason text;

  alter table leandex.index_latest_state
    add column if not exists first_seen_at timestamptz not null default now(),
    add column if not exists last_seen_at timestamptz not null default now(),
    add column if not exists last_seen_relfilenode oid,
    add column if not exists baseline_source text not null default 'observed',
    add column if not exists baseline_confidence text not null default 'low';

  alter table leandex.index_latest_state
    drop constraint if exists index_latest_state_baseline_source_check;

  alter table leandex.index_latest_state
    add constraint index_latest_state_baseline_source_check
    check (baseline_source in ('observed', 'post_reindex', 'manual'));

  alter table leandex.index_latest_state
    drop constraint if exists index_latest_state_baseline_confidence_check;

  alter table leandex.index_latest_state
    add constraint index_latest_state_baseline_confidence_check
    check (baseline_confidence in ('low', 'high'));

  update leandex.index_latest_state
  set first_seen_at = coalesce(first_seen_at, now()),
    last_seen_at = coalesce(last_seen_at, coalesce(first_seen_at, now())),
    baseline_source = coalesce(baseline_source, 'observed'),
    baseline_confidence = coalesce(baseline_confidence, 'low');

  update leandex.config
  set value = '30s',
    comment = 'remote lock_timeout applied before reindex'
  where datname is null
    and key = 'lock_timeout'
    and value = '5s';

  insert into leandex.config (key, value, comment)
  values
    ('idle_in_transaction_session_timeout', '1min', 'remote idle_in_transaction_session_timeout applied before reindex'),
    ('idle_session_timeout', '0', 'remote idle_session_timeout applied before reindex'),
    ('max_parallel_reindexes', '1', 'maximum concurrent reindexes per target database'),
    ('respect_external_index_activity', 'true', 'skip reindex start if external create index or reindex activity is active'),
    ('min_window_remaining', '0', 'minimum remaining time in an allowed start window before a reindex may start')
  on conflict (key) where datname is null do nothing;

  update leandex.tables_version
  set version = 2;
end;
$body$
language plpgsql;


-- FDW and connection management functions have been moved to leandex_fdw.sql


/*
 * Get reindexable indexes from remote database
 * Filters for safe indexes: excludes system schemas, BRIN, exclusion constraints
 */
create function leandex._remote_get_indexes_indexrelid(_datname name)
returns table(
  datname name,
  schemaname name,
  relname name,
  indexrelname name,
  indexrelid oid
) as
$body$
declare
  _use_toast_tables text;
begin
  if leandex._check_pg_version_bugfixed() then
    _use_toast_tables := 'True';
  else
    _use_toast_tables := 'False';
  end if;

  -- Secure FDW connection for querying indexes
  perform leandex._connect_securely(_datname);

  return query select
    _datname,
    _res.schemaname,
    _res.relname,
    _res.indexrelname,
    _res.indexrelid
  from
    dblink(
      _datname,
      format(
        $sql$
          select
            n.nspname as schemaname,
            c.relname,
            i.relname as indexrelname,
            x.indexrelid
          from pg_index as x
          join pg_catalog.pg_class as c on c.oid = x.indrelid
          join pg_catalog.pg_class as i on i.oid = x.indexrelid
          join pg_catalog.pg_namespace as n on n.oid = c.relnamespace
          join pg_catalog.pg_am as a on a.oid = i.relam
          -- TOAST indexes info
          left join pg_catalog.pg_class as c1 on c1.reltoastrelid = c.oid and n.nspname = 'pg_toast'
          left join pg_catalog.pg_namespace as n1 on c1.relnamespace = n1.oid
          where
            true
            -- limit reindex for indexes on tables/mviews/TOAST
            -- and c.relkind = any (array['r'::"char", 't'::"char", 'm'::"char"])
            -- limit reindex for indexes on tables/mviews (skip TOAST until bugfix of BUG #17268)
            and ((c.relkind = any (array['r'::"char", 'm'::"char"])) or ((c.relkind = 't'::"char") and %s))
            -- ignore exclusion constraints
            and not exists (select from pg_constraint where pg_constraint.conindid = i.oid and pg_constraint.contype = 'x')
            -- ignore indexes for system tables
            and n.nspname not in ('pg_catalog', 'information_schema')
            -- ignore indexes on TOAST tables of system tables
            and (n1.nspname is null or n1.nspname not in ('pg_catalog', 'information_schema', 'leandex'))
            -- skip BRIN indexes... please see BUG #17205 https://www.postgresql.org/message-id/flat/17205-42b1d8f131f0cf97%%40postgresql.org
            and a.amname not in ('brin') and x.indislive
            -- skip indexes on temp relations
            and c.relpersistence <> 't' -- t = temporary table/sequence
            -- debug only
            -- order by 1, 2, 3
        $sql$,
        _use_toast_tables
      )
    )
    as _res(
      schemaname name,
      relname name,
      indexrelname name,
      indexrelid oid
    );
end;
$body$
language plpgsql;


/*
 * Convert shell-style wildcard patterns to PostgreSQL regex format
 * Transforms * to .* and ? to . with anchors for exact matching
 */
create function leandex._pattern_convert(
  _var text
) returns text as
$body$
  select '^(' || replace(replace(_var, '*', '.*'), '?', '.') || ')$';
$body$
language sql strict immutable;


/*
 * Get configuration setting value using hierarchical priority lookup
 * Searches: index → table → schema → database → global priority order
 */
create function leandex.get_setting(
  _datname text,
  _schemaname text,
  _relname text,
  _indexrelname text,
  _key text
) returns text as
$body$
declare
  _value text;
begin
  perform leandex._check_structure_version();

  -- raise notice 'debug: |%|%|%|%|', _datname, _schemaname, _relname, _indexrelname;

  select _t.value into _value from (
    -- per index setting
    select
      1 as priority,
      value from leandex.config
    where
      _key = config.key
	    and (_datname operator(pg_catalog.~) leandex._pattern_convert(config.datname))
	    and (_schemaname operator(pg_catalog.~) leandex._pattern_convert(config.schemaname))
	    and (_relname operator(pg_catalog.~) leandex._pattern_convert(config.relname))
	    and (_indexrelname operator(pg_catalog.~) leandex._pattern_convert(config.indexrelname))
	    and config.indexrelname is not null
	    and true
    union all
    -- per table setting
    select
      2 as priority,
      value from leandex.config
    where
      _key = config.key
      and (_datname operator(pg_catalog.~) leandex._pattern_convert(config.datname))
      and (_schemaname operator(pg_catalog.~) leandex._pattern_convert(config.schemaname))
      and (_relname operator(pg_catalog.~) leandex._pattern_convert(config.relname))
      and config.relname is not null
      and config.indexrelname is null
    union all
    -- per schema setting
    select
      3 as priority,
      value from leandex.config
    where
      _key = config.key
      and (_datname operator(pg_catalog.~) leandex._pattern_convert(config.datname))
      and (_schemaname operator(pg_catalog.~) leandex._pattern_convert(config.schemaname))
      and config.schemaname is not null
      and config.relname is null
    union all
    -- per database setting
    select
      4 as priority,
      value from leandex.config
    where
      _key = config.key
      and (_datname      operator(pg_catalog.~) leandex._pattern_convert(config.datname))
      and config.datname is not null
      and config.schemaname is null
    union all
    -- global setting
    select
      5 as priority,
      value from leandex.config
    where
      _key = config.key
      and config.datname is null
    ) as _t
    where value is not null
    order by priority
    limit 1;

  return _value;
end;
$body$
language plpgsql stable;


/*
 * Set or update configuration setting at appropriate hierarchy level
 * Auto-detects specificity level based on null parameters, handles conflicts
 */
create function leandex.set_or_replace_setting(
  _datname text,
  _schemaname text,
  _relname text,
  _indexrelname text,
  _key text,
  _value text,
  _comment text
) returns void as
$body$
begin
    perform leandex._check_structure_version();

    if _datname is null then
      insert into leandex.config (datname, schemaname, relname, indexrelname, key, value, comment)
      values (_datname, _schemaname, _relname, _indexrelname, _key, _value, _comment)
      on conflict (key)
      where datname is null
      do update set
        value = excluded.value,
        comment = excluded.comment;
    elsif _schemaname is null then
      insert into leandex.config (datname, schemaname, relname, indexrelname, key, value, comment)
      values (_datname, _schemaname, _relname, _indexrelname, _key, _value, _comment)
      on conflict (key, datname)
      where schemaname is null
      do update set
        value = excluded.value,
        comment = excluded.comment;
    elsif _relname is null    then
      insert into leandex.config (datname, schemaname, relname, indexrelname, key, value, comment)
      values (_datname, _schemaname, _relname, _indexrelname, _key, _value, _comment)
      on conflict (key, datname, schemaname)
      where relname is null
      do update set
        value = excluded.value,
        comment = excluded.comment;
    elsif _indexrelname is null then
      insert into leandex.config (datname, schemaname, relname, indexrelname, key, value, comment)
      values (_datname, _schemaname, _relname, _indexrelname, _key, _value, _comment)
      on conflict (key, datname, schemaname, relname)
      where indexrelname is null
      do update set
        value = excluded.value,
        comment = excluded.comment;
    else
      insert into leandex.config (datname, schemaname, relname, indexrelname, key, value, comment)
      values (_datname, _schemaname, _relname, _indexrelname, _key, _value, _comment)
      on conflict (key, datname, schemaname, relname, indexrelname)
      do update set
        value = excluded.value,
        comment = excluded.comment;
    end if;
    return;
end;
$body$
language plpgsql;


/*
 * Get detailed index information from remote database with filtering
 * Returns comprehensive metrics, clamps zero tuples, supports wildcard filtering
 */
create function leandex._remote_get_indexes_info(
  _datname name,
  _schemaname name,
  _relname name,
  _indexrelname name
) returns table(
  datid oid,
  indexrelid oid,
  datname name,
  schemaname name,
  relname name,
  indexrelname name,
  indisvalid boolean,
  indexsize bigint,
  estimated_tuples bigint,
  relfilenode oid
) as
$body$
declare
  _use_toast_tables text;
begin
  if leandex._check_pg_version_bugfixed() then
    _use_toast_tables := 'True';
  else
    _use_toast_tables := 'False';
  end if;

  -- Secure FDW connection for querying index info
  perform leandex._connect_securely(_datname);

  return query select
    d.oid as datid,
    _res.indexrelid,
    _datname,
    _res.schemaname,
    _res.relname,
    _res.indexrelname,
    _res.indisvalid,
    _res.indexsize,
    greatest(1, indexreltuples),
    _res.relfilenode
  from
    dblink(_datname,
      format(
        $sql$
          select
            x.indexrelid,
            n.nspname as schemaname,
            c.relname,
            i.relname as indexrelname,
            x.indisvalid,
            i.reltuples::bigint as indexreltuples,
            pg_catalog.pg_relation_size(i.oid)::bigint as indexsize,
            coalesce(nullif(i.relfilenode, 0), i.oid)::oid as relfilenode
          from pg_index as x
          join pg_catalog.pg_class as c           on c.oid = x.indrelid
          join pg_catalog.pg_class as i           on i.oid = x.indexrelid
          join pg_catalog.pg_namespace as n       on n.oid = c.relnamespace
          join pg_catalog.pg_am as a              on a.oid = i.relam
          left join pg_catalog.pg_class as c1     on c1.reltoastrelid = c.oid and n.nspname = 'pg_toast'
          left join pg_catalog.pg_namespace as n1 on c1.relnamespace = n1.oid
          where true
            and ((c.relkind = any (array['r'::"char", 'm'::"char"])) or ((c.relkind = 't'::"char") and %s))
            and not exists (select from pg_constraint where pg_constraint.conindid = i.oid and pg_constraint.contype = 'x')
            and n.nspname not in ('pg_catalog', 'information_schema')
            and (n1.nspname is null or n1.nspname not in ('pg_catalog', 'information_schema', 'leandex'))
            and a.amname not in ('brin') and x.indislive
            and c.relpersistence <> 't'
        $sql$,
        _use_toast_tables
      )
    )
    as _res(
      indexrelid oid,
      schemaname name,
      relname name,
      indexrelname name,
      indisvalid boolean,
      indexreltuples bigint,
      indexsize bigint,
      relfilenode oid
    ),
    pg_database as d
    where
      d.datname = _datname
      and (_schemaname is null or _res.schemaname = _schemaname)
      and (_relname is null or _res.relname = _relname)
      and (_indexrelname is null or _res.indexrelname = _indexrelname);
end;
$body$
language plpgsql;


/*
 * Record and maintain index information in the tracking table
 * Updates metadata, manages bloat ratios, cleans removed indexes, supports filtering
 */
create function leandex._record_indexes_info(
  _datname name,
  _schemaname name,
  _relname name,
  _indexrelname name,
  _force_populate boolean default false
) returns void as
$body$
declare
  index_info record;
  _connection_created boolean := false;
begin
  if dblink_get_connections() is null or not (_datname = any(dblink_get_connections())) then
    perform leandex._dblink_connect_if_not(_datname);
    _connection_created := true;
  end if;

  with _actual_indexes as (
    select datid, indexrelid, datname, schemaname, relname, indexrelname,
      indisvalid, indexsize, estimated_tuples, relfilenode
    from leandex._remote_get_indexes_info(_datname, _schemaname, _relname, _indexrelname)
  ),
  _previous_state as (
    select distinct on (datname, schemaname, relname, indexrelname)
      datname, schemaname, relname, indexrelname, indexrelid,
      first_seen_at, last_seen_at, last_seen_relfilenode,
      baseline_source, baseline_confidence, best_ratio
    from leandex.index_latest_state
    where datname = _datname
      and (_schemaname is null or schemaname = _schemaname)
      and (_relname is null or relname = _relname)
      and (_indexrelname is null or indexrelname = _indexrelname)
    order by datname, schemaname, relname, indexrelname, mtime desc
  ),
  _old_indexes as (
    delete from leandex.index_latest_state as i
    where not exists (
      select
      from _actual_indexes
      where i.datid = _actual_indexes.datid
        and i.indexrelid = _actual_indexes.indexrelid
    )
      and i.datname = _datname
      and (_schemaname is null or i.schemaname = _schemaname)
      and (_relname is null or i.relname = _relname)
      and (_indexrelname is null or i.indexrelname = _indexrelname)
  )
  insert into leandex.index_latest_state as i
  (
    datid,
    datname,
    schemaname,
    relname,
    indexrelid,
    indexrelname,
    indexsize,
    indisvalid,
    estimated_tuples,
    best_ratio,
    first_seen_at,
    last_seen_at,
    last_seen_relfilenode,
    baseline_source,
    baseline_confidence
  )
  select
    a.datid,
    a.datname,
    a.schemaname,
    a.relname,
    a.indexrelid,
    a.indexrelname,
    a.indexsize,
    a.indisvalid,
    a.estimated_tuples,
    case
      when exists (
        select 1
        from leandex.reindex_history as h
        where h.status = 'completed'
          and h.datname = a.datname
          and h.schemaname = a.schemaname
          and h.relname = a.relname
          and h.indexrelname = a.indexrelname
          and h.indexrelid = a.indexrelid
          and h.entry_timestamp >= coalesce(p.last_seen_at, '-infinity'::timestamptz)
      )
      and a.indexsize > pg_size_bytes(leandex.get_setting(a.datname, a.schemaname, a.relname, a.indexrelname, 'minimum_reliable_index_size')) then
        a.indexsize::real / a.estimated_tuples::real
      when a.indexsize > pg_size_bytes(leandex.get_setting(a.datname, a.schemaname, a.relname, a.indexrelname, 'minimum_reliable_index_size')) then
        a.indexsize::real / a.estimated_tuples::real
      else
        null
    end as best_ratio,
    case
      when p.indexrelid = a.indexrelid
        or exists (
          select 1
          from leandex.reindex_history as h
          where h.status = 'completed'
            and h.datname = a.datname
            and h.schemaname = a.schemaname
            and h.relname = a.relname
            and h.indexrelname = a.indexrelname
            and h.indexrelid = a.indexrelid
            and h.entry_timestamp >= coalesce(p.last_seen_at, '-infinity'::timestamptz)
        ) then coalesce(p.first_seen_at, now())
      else now()
    end,
    now(),
    a.relfilenode,
    case
      when exists (
        select 1
        from leandex.reindex_history as h
        where h.status = 'completed'
          and h.datname = a.datname
          and h.schemaname = a.schemaname
          and h.relname = a.relname
          and h.indexrelname = a.indexrelname
          and h.indexrelid = a.indexrelid
          and h.entry_timestamp >= coalesce(p.last_seen_at, '-infinity'::timestamptz)
      ) then 'post_reindex'
      else 'observed'
    end,
    case
      when exists (
        select 1
        from leandex.reindex_history as h
        where h.status = 'completed'
          and h.datname = a.datname
          and h.schemaname = a.schemaname
          and h.relname = a.relname
          and h.indexrelname = a.indexrelname
          and h.indexrelid = a.indexrelid
          and h.entry_timestamp >= coalesce(p.last_seen_at, '-infinity'::timestamptz)
      ) then 'high'
      else 'low'
    end
  from _actual_indexes as a
  left join _previous_state as p
    on p.datname = a.datname
   and p.schemaname = a.schemaname
   and p.relname = a.relname
   and p.indexrelname = a.indexrelname
  on conflict (datid, indexrelid)
  do update set
    mtime = now(),
    datname = excluded.datname,
    schemaname = excluded.schemaname,
    relname = excluded.relname,
    indexrelname = excluded.indexrelname,
    indisvalid = excluded.indisvalid,
    indexsize = excluded.indexsize,
    estimated_tuples = excluded.estimated_tuples,
    last_seen_at = now(),
    last_seen_relfilenode = excluded.last_seen_relfilenode,
    best_ratio = case
      when exists (
          select 1
          from leandex.reindex_history as h
          where h.status = 'completed'
            and h.datname = i.datname
            and h.schemaname = i.schemaname
            and h.relname = i.relname
            and h.indexrelname = i.indexrelname
            and h.indexrelid = i.indexrelid
            and h.entry_timestamp >= i.last_seen_at
        )
        and excluded.indexsize > pg_size_bytes(leandex.get_setting(excluded.datname, excluded.schemaname, excluded.relname, excluded.indexrelname, 'minimum_reliable_index_size'))
        then excluded.indexsize::real / excluded.estimated_tuples::real
      when _force_populate
        and excluded.indexsize > pg_size_bytes(leandex.get_setting(excluded.datname, excluded.schemaname, excluded.relname, excluded.indexrelname, 'minimum_reliable_index_size'))
        then excluded.indexsize::real / excluded.estimated_tuples::real
      when excluded.indexsize < pg_size_bytes(leandex.get_setting(excluded.datname, excluded.schemaname, excluded.relname, excluded.indexrelname, 'minimum_reliable_index_size'))
        then i.best_ratio
      when i.best_ratio is null
        then excluded.indexsize::real / excluded.estimated_tuples::real
      else
        least(i.best_ratio, excluded.indexsize::real / excluded.estimated_tuples::real)
    end,
    baseline_source = case
      when exists (
          select 1
          from leandex.reindex_history as h
          where h.status = 'completed'
            and h.datname = i.datname
            and h.schemaname = i.schemaname
            and h.relname = i.relname
            and h.indexrelname = i.indexrelname
            and h.indexrelid = i.indexrelid
            and h.entry_timestamp >= i.last_seen_at
        ) then 'post_reindex'
      when _force_populate then 'observed'
      else i.baseline_source
    end,
    baseline_confidence = case
      when exists (
          select 1
          from leandex.reindex_history as h
          where h.status = 'completed'
            and h.datname = i.datname
            and h.schemaname = i.schemaname
            and h.relname = i.relname
            and h.indexrelname = i.indexrelname
            and h.indexrelid = i.indexrelid
            and h.entry_timestamp >= i.last_seen_at
        ) then 'high'
      when _force_populate then 'low'
      else i.baseline_confidence
    end;

  for index_info in
    select indexrelname, relname, schemaname, datname
    from leandex.index_latest_state
    where not indisvalid
      and datname = _datname
      and (_schemaname is null or schemaname = _schemaname)
      and (_relname is null or relname = _relname)
      and (_indexrelname is null or indexrelname = _indexrelname)
  loop
    raise warning 'Not valid index % on %.% found in %.',
      index_info.indexrelname, index_info.schemaname, index_info.relname, index_info.datname;
  end loop;

exception when others then
  if _connection_created and dblink_get_connections() is not null
     and _datname = any(dblink_get_connections()) then
    perform dblink_disconnect(_datname);
  end if;
  raise;
end;
$body$
language plpgsql;

/*
 * Clean up old and stale records from tracking tables
 * Removes old history records and stale database state records based on retention settings
 */
create function leandex._cleanup_old_records() returns void as
$body$
begin
  -- TODO replace with fast distinct implementation
  with rels as materialized (
    select distinct datname, schemaname, relname, indexrelname
    from leandex.reindex_history
  ), age_limit as materialized (
    select *, now() - leandex.get_setting(datname, schemaname, relname, indexrelname, 'reindex_history_retention_period')::interval as max_age
    from rels
  )
  delete from leandex.reindex_history
  using age_limit
  where
    reindex_history.datname = age_limit.datname
    and reindex_history.schemaname = age_limit.schemaname
    and reindex_history.relname = age_limit.relname
    and reindex_history.indexrelname = age_limit.indexrelname
    and reindex_history.entry_timestamp < age_limit.max_age;

  -- clean index_latest_state for not existing databases
  delete from leandex.index_latest_state
  where datid not in (
    select oid from pg_database
    where
      not datistemplate
      and datallowconn
      and leandex.get_setting(datname, null, null, null, 'skip')::boolean is distinct from true
  );

  return;
end;
$body$
language plpgsql;


/*
 * Calculate and return bloat estimates for all indexes in a database
 * Compares current size ratios with historical best, ordered by bloat level
 */
create function leandex.get_index_bloat_estimates(
  _datname name
) returns table(
  datname name,
  schemaname name,
  relname name,
  indexrelname name,
  indexsize bigint,
  estimated_bloat real
) as
$body$
declare
  _datid oid;
begin
  perform leandex._check_structure_version();

  select oid from pg_database as d where d.datname = _datname into _datid;

  -- calculate estimated bloat by comparing the current size-to-tuple ratio with the best observed ratio
  return query select
    _datname,
    i.schemaname,
    i.relname,
    i.indexrelname,
    i.indexsize,
    (i.indexsize::real / (i.best_ratio * estimated_tuples::real)) as estimated_bloat
  from leandex.index_latest_state as i
  where
    i.datid = _datid
    -- and indisvalid is true
  -- use NULLS FIRST so that indexes with null estimated bloat
  -- (which will be reindexed in the next scheduled run) appear first;
  -- order by most bloated indexes first
  order by estimated_bloat desc nulls first;
end;
$body$
language plpgsql strict;



create function leandex._record_reindex_history_event(
  _datname name,
  _schemaname name,
  _relname name,
  _indexrelname name,
  _status text,
  _skip_reason text default null,
  _error_message text default null
) returns bigint as
$body$
declare
  _history_id bigint;
begin
  insert into leandex.reindex_history (
    datid,
    datname,
    schemaname,
    relname,
    indexrelid,
    indexrelname,
    indexsize_before,
    indexsize_after,
    estimated_tuples,
    reindex_duration,
    analyze_duration,
    entry_timestamp,
    status,
    skip_reason,
    error_message
  )
  select datid,
    datname,
    schemaname,
    relname,
    indexrelid,
    indexrelname,
    indexsize,
    null,
    estimated_tuples,
    null,
    null,
    now(),
    _status,
    _skip_reason,
    _error_message
  from (
    select distinct on (datid, indexrelid)
      datid, datname, schemaname, relname, indexrelid, indexrelname, indexsize, estimated_tuples
    from leandex.index_latest_state
    where datname = _datname
      and schemaname = _schemaname
      and relname = _relname
      and indexrelname = _indexrelname
    order by datid, indexrelid, mtime desc
  ) as latest
  returning id into _history_id;

  return _history_id;
end;
$body$
language plpgsql;


create function leandex._matching_window_end(
  _windows jsonb,
  _ts timestamptz default current_timestamp
) returns timestamptz as
$body$
declare
  _window jsonb;
  _days integer[];
  _dow integer := extract(isodow from _ts)::integer;
  _prev_dow integer := case when extract(isodow from _ts)::integer = 1 then 7 else extract(isodow from _ts)::integer - 1 end;
  _start_time time;
  _end_time time;
  _day_start timestamptz := date_trunc('day', _ts);
  _candidate_end timestamptz;
  _matched_end timestamptz;
begin
  if _windows is null then
    return null;
  end if;

  for _window in select value from jsonb_array_elements(_windows)
  loop
    select coalesce(array_agg(value::text::integer), array[1,2,3,4,5,6,7])
    into _days
    from jsonb_array_elements(coalesce(_window->'days', '[1,2,3,4,5,6,7]'::jsonb));

    _start_time := (_window->>'start')::time;
    _end_time := (_window->>'end')::time;

    if _end_time > _start_time then
      if _dow = any(_days)
         and _ts >= _day_start + _start_time
         and _ts < _day_start + _end_time then
        _candidate_end := _day_start + _end_time;
      else
        _candidate_end := null;
      end if;
    else
      if _dow = any(_days)
         and _ts >= _day_start + _start_time then
        _candidate_end := _day_start + interval '1 day' + _end_time;
      elsif _prev_dow = any(_days)
         and _ts < _day_start + _end_time then
        _candidate_end := _day_start + _end_time;
      else
        _candidate_end := null;
      end if;
    end if;

    if _candidate_end is not null and (_matched_end is null or _candidate_end > _matched_end) then
      _matched_end := _candidate_end;
    end if;
  end loop;

  return _matched_end;
end;
$body$
language plpgsql stable;


create function leandex._evaluate_reindex_start(
  _datname name,
  _schemaname name,
  _relname name,
  _indexrelname name
) returns table(
  allowed_to_start boolean,
  reason text,
  window_remaining interval
) as
$body$
declare
  _allowed_windows jsonb;
  _allowed_end timestamptz;
  _min_window_remaining interval := coalesce(leandex.get_setting(_datname, _schemaname, _relname, _indexrelname, 'min_window_remaining'), '0')::interval;
begin
  if nullif(leandex.get_setting(_datname, _schemaname, _relname, _indexrelname, 'allowed_start_windows'), '') is not null then
    _allowed_windows := leandex.get_setting(_datname, _schemaname, _relname, _indexrelname, 'allowed_start_windows')::jsonb;
  end if;


  if _allowed_windows is not null then
    _allowed_end := leandex._matching_window_end(_allowed_windows, current_timestamp);
    if _allowed_end is null then
      return query select false, 'outside allowed start window', null::interval;
      return;
    end if;

    if _allowed_end - current_timestamp < _min_window_remaining then
      return query select false,
        format('min_window_remaining not satisfied (%s remaining, need %s)', _allowed_end - current_timestamp, _min_window_remaining),
        _allowed_end - current_timestamp;
      return;
    end if;

    return query select true, null::text, _allowed_end - current_timestamp;
    return;
  end if;

  return query select true, null::text, null::interval;
end;
$body$
language plpgsql stable;


create function leandex._parallel_reindex_lock_key(
  _datname name,
  _slot integer
) returns bigint as
$body$
  select hashtextextended(format('leandex-reindex:%s:%s', _datname, _slot), 0);
$body$
language sql immutable strict;


create function leandex._try_acquire_reindex_slot(
  _datname name,
  _max_parallel integer
) returns integer as
$body$
declare
  _slot integer;
begin
  for _slot in 1..greatest(coalesce(_max_parallel, 1), 1)
  loop
    if pg_try_advisory_lock(leandex._parallel_reindex_lock_key(_datname, _slot)) then
      return _slot;
    end if;
  end loop;

  return null;
end;
$body$
language plpgsql;


create function leandex._release_reindex_slot(
  _datname name,
  _slot integer
) returns void as
$body$
begin
  if _slot is not null then
    perform pg_advisory_unlock(leandex._parallel_reindex_lock_key(_datname, _slot));
  end if;
end;
$body$
language plpgsql;


create procedure leandex._clear_current_processed_index(
  _datname name,
  _schemaname name,
  _relname name,
  _indexrelname name
) as
$body$
begin
  delete from leandex.current_processed_index
  where datname = _datname
    and schemaname = _schemaname
    and relname = _relname
    and indexrelname = _indexrelname;
end;
$body$
language plpgsql;


create function leandex._apply_remote_reindex_session_settings(
  _datname name,
  _schemaname name,
  _relname name,
  _indexrelname name
) returns jsonb as
$body$
declare
  _server_version_num integer;
  _lock_timeout text := coalesce(leandex.get_setting(_datname, _schemaname, _relname, _indexrelname, 'lock_timeout'), '30s');
  _idle_in_tx_timeout text := coalesce(leandex.get_setting(_datname, _schemaname, _relname, _indexrelname, 'idle_in_transaction_session_timeout'), '1min');
  _idle_session_timeout text := coalesce(leandex.get_setting(_datname, _schemaname, _relname, _indexrelname, 'idle_session_timeout'), '0');
  _settings record;
begin
  perform leandex._dblink_connect_if_not(_datname);

  perform dblink_exec(
    _datname,
    format('set application_name = %L', format('leandex:%s:%s.%s.%s', current_database(), _schemaname, _relname, _indexrelname))
  );

  select server_version_num into _server_version_num
  from dblink(
    _datname,
    'select current_setting(''server_version_num'')::int'
  ) as t(server_version_num integer);

  perform dblink_exec(_datname, format('set lock_timeout = %L', _lock_timeout));
  perform dblink_exec(_datname, 'set statement_timeout = 0');
  perform dblink_exec(_datname, format('set idle_in_transaction_session_timeout = %L', _idle_in_tx_timeout));

  if _server_version_num >= 140000 then
    perform dblink_exec(_datname, format('set idle_session_timeout = %L', _idle_session_timeout));
  end if;

  if _server_version_num >= 170000 then
    perform dblink_exec(_datname, 'set transaction_timeout = 0');
  end if;

  select * into _settings
  from dblink(
    _datname,
    $sql$
      select current_setting('server_version_num')::int,
        current_setting('lock_timeout'),
        current_setting('statement_timeout'),
        current_setting('idle_in_transaction_session_timeout'),
        current_setting('idle_session_timeout', true),
        current_setting('transaction_timeout', true),
        current_setting('application_name')
    $sql$
  ) as t(
    server_version_num integer,
    lock_timeout text,
    statement_timeout text,
    idle_in_transaction_session_timeout text,
    idle_session_timeout text,
    transaction_timeout text,
    application_name text
  );

  return jsonb_build_object(
    'server_version_num', _settings.server_version_num,
    'lock_timeout', _settings.lock_timeout,
    'statement_timeout', _settings.statement_timeout,
    'idle_in_transaction_session_timeout', _settings.idle_in_transaction_session_timeout,
    'idle_session_timeout', _settings.idle_session_timeout,
    'transaction_timeout', _settings.transaction_timeout,
    'application_name', _settings.application_name
  );
end;
$body$
language plpgsql;


create function leandex._detect_reindex_blockers(
  _datname name,
  _schemaname name,
  _relname name,
  _indexrelname name
) returns table(
  blocker_reason text
) as
$body$
declare
  _respect_external boolean := coalesce(leandex.get_setting(_datname, _schemaname, _relname, _indexrelname, 'respect_external_index_activity'), 'true')::boolean;
  _lock_timeout text := coalesce(leandex.get_setting(_datname, _schemaname, _relname, _indexrelname, 'lock_timeout'), '30s');
  _reason text;
begin
  perform leandex._dblink_connect_if_not(_datname);

  if _respect_external then
    begin
      select reason into _reason
      from dblink(
        _datname,
        format(
          $sql$
            select format(
              'external index activity: pid=%%s command=%%s app=%%s',
              a.pid,
              p.command,
              coalesce(a.application_name, '<unknown>')
            ) as reason
            from pg_stat_progress_create_index as p
            join pg_stat_activity as a using (pid)
            join pg_class as c on c.oid = p.relid
            join pg_namespace as n on n.oid = c.relnamespace
            where n.nspname = %1$L
              and c.relname = %2$L
              and coalesce(a.application_name, '') not like 'leandex:%%'
            limit 1
          $sql$,
          _schemaname,
          _relname
        )
      ) as t(reason text);
    exception when others then
      _reason := null;
    end;

    if _reason is null then
      select reason into _reason
      from dblink(
        _datname,
        format(
          $sql$
            select format(
              'external index activity: pid=%%s state=%%s app=%%s',
              pid,
              state,
              coalesce(application_name, '<unknown>')
            ) as reason
            from pg_stat_activity
            where pid <> pg_backend_pid()
              and coalesce(application_name, '') not like 'leandex:%%'
              and state <> 'idle'
              and (
                lower(query) like 'create index%%'
                or lower(query) like 'reindex%%'
              )
              and lower(query) like lower(%1$L)
              and lower(query) like lower(%2$L)
            limit 1
          $sql$,
          '%' || _schemaname || '%',
          '%' || _relname || '%'
        )
      ) as t(reason text);
    end if;

    if _reason is not null then
      return query select _reason;
      return;
    end if;
  end if;

  select reason into _reason
  from dblink(
    _datname,
    $sql$
      select format(
        'old snapshot: pid=%s age=%s backend_xmin=%s app=%s state=%s',
        a.pid,
        clock_timestamp() - a.xact_start,
        a.backend_xmin,
        coalesce(a.application_name, '<unknown>'),
        a.state
      ) as reason
      from pg_stat_activity as a
      where a.datname = current_database()
        and a.pid <> pg_backend_pid()
        and a.backend_xmin is not null
        and coalesce(a.application_name, '') not like 'leandex:%'
      order by a.xact_start nulls last, a.backend_start
      limit 1
    $sql$
  ) as t(reason text);

  if _reason is not null then
    return query select _reason;
    return;
  end if;

  select reason into _reason
  from dblink(
    _datname,
    format(
      $sql$
        select format(
          'blocking transaction: pid=%%s age=%%s app=%%s state=%%s',
          a.pid,
          clock_timestamp() - a.xact_start,
          coalesce(a.application_name, '<unknown>'),
          a.state
        ) as reason
        from pg_locks as l
        join pg_stat_activity as a on a.pid = l.pid
        join pg_class as c on c.oid = l.relation
        join pg_namespace as n on n.oid = c.relnamespace
        where l.granted
          and a.pid <> pg_backend_pid()
          and a.xact_start is not null
          and a.xact_start <= clock_timestamp() - %1$L::interval
          and coalesce(a.application_name, '') not like 'leandex:%%'
          and n.nspname = %2$L
          and c.relname in (%3$L, %4$L)
          and l.mode in (
            'ShareUpdateExclusiveLock',
            'ShareLock',
            'ShareRowExclusiveLock',
            'ExclusiveLock',
            'AccessExclusiveLock'
          )
        order by a.xact_start
        limit 1
      $sql$,
      _lock_timeout,
      _schemaname,
      _relname,
      _indexrelname
    )
  ) as t(reason text);

  if _reason is not null then
    return query select _reason;
  end if;
end;
$body$
language plpgsql;


create function leandex._run_remote_reindex(
  _datname name,
  _schemaname name,
  _relname name,
  _indexrelname name
) returns text as
$body$
begin
  perform leandex._apply_remote_reindex_session_settings(
    _datname,
    _schemaname,
    _relname,
    _indexrelname
  );

  perform dblink_exec(
    _datname,
    format('reindex index concurrently %I.%I', _schemaname, _indexrelname)
  );

  return null;
exception when others then
  return case
    when sqlerrm ilike '%lock timeout%' then 'blocking transaction: ' || sqlerrm
    else sqlerrm
  end;
end;
$body$
language plpgsql;

/*
 * Perform concurrent reindexing of a specific index
 * Executes REINDEX INDEX CONCURRENTLY via secure dblink with logging and error handling
 */
create function leandex._reindex_index(
  _datname name,
  _schemaname name,
  _relname name,
  _indexrelname name
) returns void as
$body$
declare
  _indexsize_before bigint;
  _indexsize_after  bigint;
  _timestamp        timestamp;
  _reindex_duration interval;
  _analyze_duration interval :='0s';
  _estimated_tuples bigint;
  _indexrelid oid;
  _datid oid;
  _indisvalid boolean;
begin
  -- Ensure a dblink connection exists for the target database using FDW for secure password handling.
  -- The connection name is set to the database name (note: not unique per index).
  if dblink_get_connections() is null or not (_datname = any(dblink_get_connections())) then
    -- Establish a secure connection to the target database, handling control database mode if needed
    perform leandex._connect_securely(_datname);

    raise notice 'Created dblink connection: %', _datname;
  end if;

  -- raise notice 'working with %.%.% %', _datname, _schemaname, _relname, _indexrelname;

  -- Retrieve the current index size and confirm the index exists in the target database
  select indexsize, estimated_tuples into _indexsize_before, _estimated_tuples
  from leandex._remote_get_indexes_info(_datname, _schemaname, _relname, _indexrelname)
  where indisvalid;

  -- If the index doesn't exist anymore, exit the function
  if not found then
    return;
  end if;

  -- Perform the reindex operation using synchronous dblink
  _timestamp := pg_catalog.clock_timestamp ();

  -- Execute REINDEX INDEX CONCURRENTLY in a synchronous manner (similar to the original pg_index_watch)
  -- This operation blocks until the reindexing process is fully completed
  begin
    perform dblink(
      _datname,
      format('reindex index concurrently %I.%I', _schemaname, _indexrelname)
    );

    raise notice 'reindex index concurrently %.% completed successfully', _schemaname, _indexrelname;
  exception when others then
    raise notice 'reindex failed for %.%: %', _schemaname, _indexrelname, SQLERRM;
    -- Continue anyway, the index might have issues
    -- This allows the function to complete successfully even if the reindex fails
  end;

  -- Don't disconnect - keep connection for reuse (like original pg_index_watch)

  _reindex_duration := pg_catalog.clock_timestamp() - _timestamp;

  -- Retrieve the index size after reindexing
  select indexsize into _indexsize_after
  from leandex._remote_get_indexes_info(_datname, _schemaname, _relname, _indexrelname)
  where indisvalid;

  -- If the index doesn't exist anymore or is invalid, use the original size
  if _indexsize_after is null then
    _indexsize_after := _indexsize_before;
  end if;

  -- Log the completed reindex operation to the reindex_history table
  insert into leandex.reindex_history (
    datname,
    schemaname,
    relname,
    indexrelname,
    indexsize_before,
    indexsize_after,
    estimated_tuples,
    reindex_duration,
    analyze_duration,
    entry_timestamp
  ) values (
    _datname,
    _schemaname,
    _relname,
    _indexrelname,
    _indexsize_before,
    _indexsize_after,
    _estimated_tuples,
    _reindex_duration,
    '0'::interval,
    now()
  );

  raise notice 'reindex COMPLETED: %.% - size before: %, size after: %, duration: %',
    _schemaname, _indexrelname,
    pg_size_pretty(_indexsize_before),
    pg_size_pretty(_indexsize_after),
    _reindex_duration;
end;
$body$
language plpgsql strict;


/*
 * Main reindexing orchestrator procedure
 * Identifies and reindexes bloated indexes based on thresholds and estimates
 */
create procedure leandex.do_reindex(
  _datname name,
  _schemaname name,
  _relname name,
  _indexrelname name,
  _force boolean default false
) as
$body$
declare
  _index record;
  _start_gate record;
  _blocker record;
  _slot integer;
  _max_parallel integer;
  _final_size bigint;
  _final_info record;
  _error_message text;
  _history_id bigint;
begin
  perform leandex._check_structure_version();

  if _datname = current_database() then
    raise exception using
      message = format(
        'Cannot REINDEX in current database %s - this causes deadlocks.',
        _datname
      ),
      hint = 'Use separate control database.';
  end if;

  if dblink_get_connections() is null or not (_datname = any(dblink_get_connections())) then
    perform leandex._dblink_connect_if_not(_datname);
    commit;
  end if;

  for _index in
    select datname, schemaname, relname, indexrelname, indexsize, estimated_bloat
    from leandex.get_index_bloat_estimates(_datname)
    where (_schemaname is null or schemaname = _schemaname)
      and (_relname is null or relname = _relname)
      and (_indexrelname is null or indexrelname = _indexrelname)
      and (_force or (
        indexsize >= pg_size_bytes(leandex.get_setting(datname, schemaname, relname, indexrelname, 'index_size_threshold'))
        and leandex.get_setting(datname, schemaname, relname, indexrelname, 'skip')::boolean is distinct from true
        and (
          estimated_bloat is null
          or estimated_bloat >= leandex.get_setting(datname, schemaname, relname, indexrelname, 'index_rebuild_scale_factor')::float
        )
      ))
  loop
    select * into _start_gate
    from leandex._evaluate_reindex_start(_index.datname, _index.schemaname, _index.relname, _index.indexrelname);

    if not _start_gate.allowed_to_start then
      perform leandex._record_reindex_history_event(
        _index.datname,
        _index.schemaname,
        _index.relname,
        _index.indexrelname,
        'skipped',
        _start_gate.reason
      );
      continue;
    end if;

    select * into _blocker
    from leandex._detect_reindex_blockers(_index.datname, _index.schemaname, _index.relname, _index.indexrelname)
    limit 1;

    if _blocker.blocker_reason is not null then
      perform leandex._record_reindex_history_event(
        _index.datname,
        _index.schemaname,
        _index.relname,
        _index.indexrelname,
        'skipped',
        _blocker.blocker_reason
      );
      continue;
    end if;

    _max_parallel := greatest(coalesce(leandex.get_setting(_index.datname, _index.schemaname, _index.relname, _index.indexrelname, 'max_parallel_reindexes'), '1')::integer, 1);

    _slot := leandex._try_acquire_reindex_slot(_index.datname, _max_parallel);

    if _slot is null then
      perform leandex._record_reindex_history_event(
        _index.datname,
        _index.schemaname,
        _index.relname,
        _index.indexrelname,
        'skipped',
        format('max_parallel_reindexes reached (limit=%s)', _max_parallel)
      );
      continue;
    end if;

    begin
      insert into leandex.current_processed_index(
        datname,
        schemaname,
        relname,
        indexrelname
      ) values (
        _index.datname,
        _index.schemaname,
        _index.relname,
        _index.indexrelname
      );

      _history_id := leandex._record_reindex_history_event(
        _index.datname,
        _index.schemaname,
        _index.relname,
        _index.indexrelname,
        'in_progress'
      );
    exception when others then
      call leandex._clear_current_processed_index(_index.datname, _index.schemaname, _index.relname, _index.indexrelname);
      perform leandex._release_reindex_slot(_index.datname, _slot);
      _slot := null;
      _history_id := null;
      raise;
    end;

    commit;

    begin
      _error_message := leandex._run_remote_reindex(
        _index.datname,
        _index.schemaname,
        _index.relname,
        _index.indexrelname
      );

      if _error_message is null then
        select indexrelid, indexsize into _final_info
        from leandex._remote_get_indexes_info(_index.datname, _index.schemaname, _index.relname, _index.indexrelname)
        where indisvalid;

        update leandex.reindex_history
        set reindex_duration = clock_timestamp() - entry_timestamp,
          status = 'completed',
          indexrelid = _final_info.indexrelid,
          indexsize_after = _final_info.indexsize,
          skip_reason = null,
          error_message = null
        where id = _history_id;
      else
        update leandex.reindex_history
        set status = 'failed',
          error_message = _error_message,
          reindex_duration = clock_timestamp() - entry_timestamp
        where id = _history_id;
      end if;
    exception when others then
      update leandex.reindex_history
      set status = 'failed',
        error_message = sqlerrm,
        reindex_duration = clock_timestamp() - entry_timestamp
      where id = _history_id
        and status = 'in_progress';
    end;

    call leandex._clear_current_processed_index(_index.datname, _index.schemaname, _index.relname, _index.indexrelname);
    perform leandex._release_reindex_slot(_index.datname, _slot);
    _slot := null;
    _history_id := null;
    commit;
  end loop;
end;
$body$
language plpgsql;

/*
 * Force-populate index statistics and bloat baselines without reindexing
 * Records current size-to-tuple ratios as optimal baselines, supports filtering
 */
create function leandex.do_force_populate_index_stats(
  _datname name,
  _schemaname name,
  _relname name,
  _indexrelname name
) returns void as
$body$
declare
  _connection_created boolean := false;
begin
  -- Ensure table structure is at required version
  perform leandex._check_structure_version();

  -- Ensure dblink connection is established before starting any transaction with cleanup guarantee
  if dblink_get_connections() is null or not (_datname = any(dblink_get_connections())) then
    perform leandex._dblink_connect_if_not(_datname);
    _connection_created := true;
  end if;

  -- Force-populate best_ratio from current state without reindexing
  perform leandex._record_indexes_info(_datname, _schemaname, _relname, _indexrelname, _force_populate=>true);
  return;

exception when others then
  -- Guaranteed connection cleanup on any exception
  if _connection_created and dblink_get_connections() is not null
     and _datname = any(dblink_get_connections()) then
    perform dblink_disconnect(_datname);
  end if;
  raise; -- Re-raise the original exception
end;
$body$
language plpgsql;


/*
 * Acquire advisory lock to prevent concurrent periodic executions
 * Prevents resource conflicts and duplicate processing, returns lock ID or raises exception
 */
create function leandex._check_lock() returns bigint as
$body$
declare
  _id bigint;
  _is_not_running boolean;
begin
  -- Get the lock id for the leandex namespace
  select oid from pg_namespace where nspname = 'leandex' into _id;

  -- Check if the lock is already held by another instance
  select pg_try_advisory_lock(_id) into _is_not_running;

  -- If the lock is already held by another instance, raise an error
  if not _is_not_running then
    raise 'Previous launch of leandex.periodic is still running.';
  end if;

  return _id;
end;
$body$
language plpgsql;


/*
 * Clean up orphaned invalid indexes from failed REINDEX INDEX CONCURRENTLY operations
 * Drops leftover "_ccnew" indexes and cleans tracking records to prevent storage waste
 */
create procedure leandex._cleanup_our_not_valid_indexes() as
$body$
declare
  _index record;
  _invalid record;
  _base_name text;
begin
  for _index in
    select distinct datname, schemaname, relname, indexrelname
    from leandex.reindex_history
    where status = 'failed'
      and entry_timestamp >= now() - interval '7 days'
  loop
    begin
      if dblink_get_connections() is null or not (_index.datname = any(dblink_get_connections())) then
        perform leandex._connect_securely(_index.datname);
      end if;

      _base_name := _index.indexrelname || '_ccnew';

      for _invalid in
        select invalid_index_name
        from dblink(_index.datname,
          format(
            $sql$
              select i.relname as invalid_index_name
              from pg_index x
              join pg_catalog.pg_class as c on c.oid = x.indrelid
              join pg_catalog.pg_class as i on i.oid = x.indexrelid
              join pg_catalog.pg_namespace as n on n.oid = c.relnamespace
              where n.nspname = %1$L
                and c.relname = %2$L
                and not x.indisvalid
                and left(i.relname, length(%3$L)) = %3$L
                and substring(i.relname from length(%3$L) + 1) ~ '^[0-9]*$'
            $sql$,
            _index.schemaname,
            _index.relname,
            _base_name
          )
        ) as _res(invalid_index_name name)
      loop
        if not exists (
          select from dblink(
            _index.datname,
            format(
              $sql$
                select x.indexrelid
                from pg_index x
                join pg_catalog.pg_class as c on c.oid = x.indrelid
                join pg_catalog.pg_class as i on i.oid = x.indexrelid
                join pg_catalog.pg_namespace as n on n.oid = c.relnamespace
                where n.nspname = %1$L
                  and c.relname = %2$L
                  and i.relname = %3$L
              $sql$,
              _index.schemaname,
              _index.relname,
              _index.indexrelname
            )
          ) as _res(indexrelid oid))
        then
          raise warning 'The invalid index %.% exists, but no original index %.% was found in database %',
            _index.schemaname, _invalid.invalid_index_name, _index.schemaname, _index.indexrelname, _index.datname;
        end if;

        perform dblink_exec(_index.datname, format('drop index concurrently %I.%I',
          _index.schemaname, _invalid.invalid_index_name));

        raise warning 'The invalid index %.% was dropped in database %',
          _index.schemaname, _invalid.invalid_index_name, _index.datname;
      end loop;
    exception when others then
      raise warning 'Failed to clean invalid indexes for %.%.% in database %: %',
        _index.schemaname, _index.relname, _index.indexrelname, _index.datname, sqlerrm;
    end;

    if not exists (
      select 1
      from leandex.reindex_history as h
      where h.datname = _index.datname
        and h.schemaname = _index.schemaname
        and h.relname = _index.relname
        and h.indexrelname = _index.indexrelname
        and h.status = 'in_progress'
    ) then
      call leandex._clear_current_processed_index(_index.datname, _index.schemaname, _index.relname, _index.indexrelname);
    end if;
  end loop;
end;
$body$
language plpgsql;


/*
 * Main periodic execution procedure for automated index maintenance
 * Primary entry point for scheduled operations: validates, migrates, processes databases
 */
create or replace procedure leandex.periodic(
  real_run boolean default false,
  force boolean default false
) as
$body$
declare
  _datname name;
  _schemaname name;
  _relname name;
  _indexrelname name;
  _id bigint;
begin
  -- Validate PostgreSQL version safety
  perform leandex._validate_pg_version();

  -- Acquire advisory lock to prevent concurrent executions
  select leandex._check_lock() into _id;

  -- Check if the table structure is up to date
  perform leandex.check_update_structure_version();

  -- Check if we're in control database mode
  if exists (select from pg_tables where schemaname = 'leandex' and tablename = 'target_databases') then
    -- Control database mode: process all enabled target databases
    for _datname in
      select database_name
      from leandex.target_databases
      where enabled
    loop
      -- Clean old history for this database
      delete from leandex.reindex_history
      where datname = _datname
        and entry_timestamp < now() - coalesce(
          leandex.get_setting(datname, schemaname, relname, indexrelname, 'reindex_history_retention_period')::interval,
          '10 years'::interval
        );

      -- Record indexes for this database
      perform leandex._record_indexes_info(_datname, null, null, null);

      if real_run then
        call leandex.do_reindex(_datname, null, null, null, force);
        -- refresh snapshot right after reindex to clamp baseline with current ratio
        perform leandex._record_indexes_info(_datname, null, null, null);
      end if;
    end loop;

    -- Note: No need to update completed reindexes - all tracking is synchronous now

    -- Clean up any invalid _ccnew indexes from failed reindexes
    call leandex._cleanup_our_not_valid_indexes();
  else
    -- Standalone mode (shouldn't happen with our fixes, but keep for safety)
    raise exception 'Control database architecture required. Cannot run periodic in standalone mode.';
  end if;

  -- Note: best_ratio updates are now handled during snapshot insertion
  -- Historical snapshots preserve the best_ratio calculation at the time of observation
  -- No need to update old snapshots - new snapshots will have updated best_ratio values

  perform pg_advisory_unlock(_id);
end;
$body$
language plpgsql;


/*
 * Comprehensive permission and setup validation for leandex
 * Validates required permissions, extensions, and FDW configuration for managed services
 */
create function leandex.check_permissions() returns table(
  permission text,
  status boolean
) as
$body$
begin
  return query select
    'Can create indexes'::text,
    has_database_privilege(current_database(), 'create');

  return query select
    'Can read pg_stat_user_indexes'::text,
    has_table_privilege('pg_stat_user_indexes', 'select');

  return query select
    'Has dblink extension'::text,
    exists (select from pg_extension where extname = 'dblink');

  return query select
    'Has postgres_fdw extension'::text,
    exists (select from pg_extension where extname = 'postgres_fdw');

  return query select
    'Has target servers registered'::text,
    exists (select 1 from leandex.target_databases);

  return query select
    'Has user mapping for dblink'::text,
    exists (
      select 1 from pg_user_mappings as um
      where um.usename = current_user
        and um.srvname in (select fdw_server_name from leandex.target_databases where enabled)
    );

  -- Verify reindex capability by checking ownership of at least one index
  return query select
    'Can reindex (owns indexes)'::text,
    exists (
      select from pg_index as i
      join pg_class as c on i.indexrelid = c.oid
      join pg_namespace as n on c.relnamespace = n.oid
      where
        n.nspname not in ('pg_catalog', 'information_schema')
        and pg_has_role(c.relowner, 'usage')
      limit 1
    );
end;
$body$
language plpgsql;


/*
 * Installation-time permission validation and user guidance
 * Shows setup status and provides clear feedback on missing requirements
 */
do $$
declare
  _perm record;
  _all_ok boolean := true;
begin
  raise notice 'leandex - monitoring current database only';
  raise notice 'Database: %', current_database();
  raise notice '';
  raise notice 'Checking permissions...';

  for _perm in select * from leandex.check_permissions() loop
    raise notice '  %: %',
      rpad(_perm.permission, 30),
      case when _perm.status then 'OK' else 'MISSING' end;
      if not _perm.status then
        _all_ok := false;
      end if;
  end loop;

  raise notice '';

  if _all_ok then
    raise notice 'All permissions OK. You can use leandex.';
  else
    raise warning 'Some permissions are missing. leandex may not work correctly.';
  end if;

  raise notice '';
  raise notice 'Usage: call leandex.periodic(true);  -- true = perform actual reindexing';
end $$;

commit;


-- leandex FDW and dblink helpers

begin;

-- Turn off useless (in this particular case) NOTICE noise
set client_min_messages to warning;

-- FDW and connection management functions for leandex
-- This file contains all functions related to Foreign Data Wrapper (FDW) setup,
-- secure database connections, and connection management.

/*
 * Establish secure dblink connection to target database via postgres_fdw
 * Uses FDW user mapping for secure credentials, prevents deadlocks, auto-reconnects
 */
create function leandex._connect_securely(
  _datname name
) returns void as
$body$
begin
  -- CRITICAL: Prevent deadlocks - never allow reindex in the same database
  -- Control database architecture is REQUIRED
  if _datname = current_database() then
    raise exception using
      message = format(
        'Cannot connect to current database %s - this causes deadlocks.',
        _datname
      ),
      hint = 'leandex must be run from separate control database.';
  end if;

  -- Disconnect existing connection if any
  if _datname = any(dblink_get_connections()) then
    perform dblink_disconnect(_datname);
  end if;

  -- Use ONLY postgres_fdw with user mapping (secure approach)
  -- Password is stored in a postgres_fdw user mapping, not embedded in a dblink connection string
  declare
    _fdw_server_name text;
  begin
    -- Control database architecture is REQUIRED - get the FDW server for the target database
    select fdw_server_name
    into _fdw_server_name
    from leandex.target_databases
    where database_name = _datname
    and enabled = true;

    if _fdw_server_name is null then
      raise exception using
        message = format(
          'Target database %s not registered or not enabled in leandex.target_databases.',
          _datname
        ),
        hint = 'Control database setup required.';
    end if;

    -- Use user mapping via postgres_fdw: dblink_connect with server name (no plaintext passwords)
    perform dblink_connect(_datname, _fdw_server_name);
    perform dblink_exec(
      _datname,
      format('set application_name = %L', format('leandex:%s', current_database()))
    );

  exception when others then
    raise exception using
      message = format(
        'FDW connection failed for database %s using server %s: %s',
        _datname,
        coalesce(_fdw_server_name, '<unknown>'),
        sqlerrm
      );
  end;
end;
$body$
language plpgsql;


/*
 * Establish secure dblink connection if not already connected
 * Creates secure FDW connection only if needed, handles null connections case
 */
create function leandex._dblink_connect_if_not(
  _datname name
) returns void as
$body$
begin
  -- Use secure FDW connection if not already connected
  -- Handle null case when no connections exist
  if dblink_get_connections() is null or not (_datname = any(dblink_get_connections())) then
    perform leandex._connect_securely(_datname);
  end if;

  return;
end;
$body$
language plpgsql;


/*
 * Comprehensive postgres_fdw security setup validation
 * Validates FDW configuration components with detailed status and guidance
 */
create function leandex.check_fdw_security_status() returns table(
  component text,
  status text,
  details text
) as
$body$
begin
  -- Check postgres_fdw extension
  return query select
    'postgres_fdw extension'::text,
    case when exists (select from pg_extension where extname = 'postgres_fdw')
      then 'INSTALLED' else 'MISSING' end::text,
    case when exists (select from pg_extension where extname = 'postgres_fdw')
      then 'Extension is available for use'
      else 'Run: create extension postgres_fdw;' end::text;

  -- Check FDW usage privilege
  return query select
    'FDW usage privilege'::text,
    case when has_foreign_data_wrapper_privilege(current_user, 'postgres_fdw', 'usage')
      then 'GRANTED' else 'DENIED' end::text,
    case when has_foreign_data_wrapper_privilege(current_user, 'postgres_fdw', 'usage')
      then format('User %s can use postgres_fdw', current_user)
      else format('Run: grant usage on foreign data wrapper postgres_fdw to %s;', current_user) end::text;

  -- Check target servers registered
  return query select
    'Target servers registered'::text,
    case
      when exists (select from leandex.target_databases) then 'YES'
      else 'NO'
    end::text,
    'Register targets with SQL: create server + user mapping + insert into leandex.target_databases'::text;

  -- Check user mapping for current user on at least one target server
  return query select
    'User mapping for current user'::text,
    case when exists (
      select 1
      from pg_user_mappings um
      where
        um.usename = current_user
        and um.srvname in (
          select fdw_server_name
          from leandex.target_databases
          where enabled
        )
    ) then
      'exists'
    else
      'MISSING'
    end::text,
    'Create mapping: create user mapping for current_user server <server> options (user ''<remote_user>'', password ''<password>'');'::text;

  -- Overall security status
  return query select
    'Overall security status'::text,
    case when (
      exists (select from pg_extension where extname = 'postgres_fdw') and
      has_foreign_data_wrapper_privilege(current_user, 'postgres_fdw', 'usage') and
      exists (select 1 from leandex.target_databases) and
      exists (
        select 1 from pg_user_mappings um
        where
          um.usename = current_user
          and um.srvname in (select fdw_server_name from leandex.target_databases where enabled)
      )
    ) then 'SECURE' else 'SETUP_REQUIRED' end::text,
    case when (
      exists (select from pg_extension where extname = 'postgres_fdw') and
      has_foreign_data_wrapper_privilege(current_user, 'postgres_fdw', 'usage') and
      exists (select 1 from leandex.target_databases) and
      exists (
        select 1 from pg_user_mappings um
        where
          um.usename = current_user
          and um.srvname in (select fdw_server_name from leandex.target_databases where enabled)
      )
    ) then 'All FDW components are properly configured'
      else 'Complete the missing setup steps above' end::text;
end;
$body$
language plpgsql;

commit;

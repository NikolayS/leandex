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
  '5s',
  'remote lock_timeout applied before reindex'
), (
  'statement_timeout',
  '0',
  'remote statement_timeout applied before reindex; 0 disables it'
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
  status text not null default 'completed' check (status in ('in_progress', 'completed', 'failed')),
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
  -- Provenance of best_ratio. NULL while best_ratio is NULL (index too small).
  --   'first_seen' — initial observation; baseline may itself be bloated, so
  --                  estimated_bloat is reported as NULL until promoted
  --   'reindexed'  — stamped after a successful leandex-driven REINDEX
  --   'forced'     — operator-attested clean state via do_force_populate_index_stats
  --   'improved'   — least() reduced best_ratio after first observation, which
  --                  proves the original baseline was not the minimum
  baseline_source text check (baseline_source in ('first_seen', 'reindexed', 'forced', 'improved')),
  baseline_set_at timestamptz,
  last_reindex_at timestamptz
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
    left(error_message, 100) as error
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

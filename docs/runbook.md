## Runbook

### Scope
- **Audience**: DB operators/SREs running `leandex` in production
- **Targets**: PostgreSQL 13+ with a dedicated control database

### Register target databases
```sql
-- Register a target DB to manage (repeat per database)
insert into leandex.target_databases(database_name, host, port, fdw_server_name)
values ('<target_db_name>', '<target_host>', 5432, 'target_<target_db_name>');

-- List registered targets
select id, database_name, host, port, enabled, added_at from leandex.target_databases order by id;
```

### Initial dry-run and first real run
```sql
-- Dry run: detect bloat, do not reindex
call leandex.periodic(false);

-- Real run: perform reindex as needed
call leandex.periodic(true);
```

### Scheduling with pg_cron
```sql
-- Identify pg_cron database
show cron.database_name;        -- connect to that DB before scheduling

-- Daily at 02:00 in control DB
select cron.schedule_in_database(
  'leandex_daily',
  '0 2 * * *',
  'call leandex.periodic(real_run := true);',
  '<leandex_control_db>'
);

-- Verify/maintain schedules
select jobname, schedule, command, database, active from cron.job where jobname like 'leandex%';
select cron.unschedule('leandex_daily');  -- disable
```

### Scheduling with external schedulers

Prefer running a single `psql` command from your scheduler rather than wrapping leandex in a repository shell helper:

```bash
psql -h <control_host> -U <admin_or_operator_user> -d <leandex_control_db> \
  -v ON_ERROR_STOP=1 \
  -c "call leandex.periodic(real_run := true);"
```

If your environment has replicas, make the scheduler target the primary/control writer only.

## Managing State

### Inventory of objects (control DB)
- **Schema**: `leandex`
- **Tables**: `target_databases`, `reindex_history`, `index_latest_state`, `config`, `tables_version`, `current_processed_index`
- **View**: `history`
- **Functions/Procedures**: `periodic`, `do_reindex`, `get_index_bloat_estimates`, `do_force_populate_index_stats`, `get_setting`, `set_or_replace_setting`, `check_permissions`, `version`, `check_update_structure_version`, FDW helpers

### Manage targets
```sql
-- Enable/disable a target
update leandex.target_databases set enabled = false where database_name = '<target_db_name>';
update leandex.target_databases set enabled = true  where database_name = '<target_db_name>';

-- Remove a target
delete from leandex.target_databases where database_name = '<target_db_name>';
```

### Configuration management
```sql
-- View effective setting resolution order (global → db → schema → table → index)
-- Read effective value for a key
select leandex.get_setting('<db>', '<schema>', '<table>', '<index>', 'index_rebuild_scale_factor');

-- Set or replace settings
-- Global: rebuild when bloat ≥ 2x (default is 2)
select leandex.set_or_replace_setting(null, null, null, null, 'index_rebuild_scale_factor', '2', 'default policy');

-- Per-database: ignore small indexes under 20MB
select leandex.set_or_replace_setting('<db>', null, null, null, 'index_size_threshold', '20MB', 'raise threshold for this DB');

-- Per-index: force skip
select leandex.set_or_replace_setting('<db>', '<schema>', '<table>', '<index>', 'skip', 'true', 'temporarily skip');

-- List all configured overrides
select datname, schemaname, relname, indexrelname, key, value, comment
from leandex.config
order by key, datname nulls first, schemaname nulls first;
```

### Baseline and structure
```sql
-- Ensure table structure is at required version
select leandex.check_update_structure_version();

-- Initialize baseline ratios from current sizes (no reindex)
select leandex.do_force_populate_index_stats('<db>', null, null, null);
```

### Manual operations
```sql
-- Reindex a specific index in a target database
call leandex.do_reindex('<db>', '<schema>', '<table>', '<index>', false);

-- Force reindex regardless of estimated bloat
call leandex.do_reindex('<db>', '<schema>', '<table>', '<index>', true);
```

## Observability

### Health and status
```sql
-- Permissions check (sanity)
select * from leandex.check_permissions();

-- FDW security status
select * from leandex.check_fdw_security_status();

-- Current version
select leandex.version();
```

### Bloat and history
```sql
-- Bloat estimates for a target DB
select
  indexrelname,
  pg_size_pretty(indexsize::bigint) as current_size,
  round(estimated_bloat::numeric, 2) as bloat_x
from leandex.get_index_bloat_estimates('<db>')
order by estimated_bloat desc nulls last
limit 50;

-- Recent operations
select * from leandex.history limit 50;

-- Only failures (investigate)
select * from leandex.history where status = 'failed' order by ts desc;
```

### In-progress tracking
```sql
-- What is being processed now
select * from leandex.current_processed_index order by mtime desc;

-- Raw reindex history (with durations)
select
  datname, schemaname, relname, indexrelname,
  pg_size_pretty(indexsize_before) as before,
  pg_size_pretty(indexsize_after)  as after,
  reindex_duration, status, error_message, entry_timestamp
from leandex.reindex_history
order by entry_timestamp desc
limit 50;
```

## Incident handling

### Reindex takes too long or appears stuck
```sql
-- See active item
select * from leandex.current_processed_index;

-- From control DB, check target DB backend activity via dblink
-- Replace placeholders to query pg_stat_activity on the target
select * from dblink('<db>', $$
  select pid, state, query_start, wait_event_type, wait_event, query
  from pg_stat_activity
  where query ilike 'reindex index concurrently%' $$)
as t(pid int, state text, query_start timestamptz, wait_event_type text, wait_event text, query text);
```

```sql
-- Cancel/terminate the REINDEX backend on the target DB (use with caution)
-- Find pid first (previous query), then:
select * from dblink('<db>', 'select pg_cancel_backend(<pid>)') as t(ok boolean);
-- If cancel does not work after waiting, terminate:
select * from dblink('<db>', 'select pg_terminate_backend(<pid>)') as t(ok boolean);
```

### Failed reindex with invalid _ccnew indexes
```sql
-- Auto-clean invalid _ccnew indexes left from failures
call leandex._cleanup_our_not_valid_indexes();

-- List remaining invalid _ccnew indexes (manual review)
select n.nspname, i.relname
from pg_index idx
join pg_class i on i.oid = idx.indexrelid
join pg_namespace n on n.oid = i.relnamespace
where i.relname ~ '_ccnew[0-9]*$' and not idx.indisvalid;

-- Drop manually if safe
-- Example:
-- select * from dblink('<db>', 'drop index concurrently if exists <schema>.<index>_ccnew') as t(result text);
```

### FDW connection or authentication failures

Recreate the FDW server, user mapping, and inventory row from SQL:

```sql
drop server if exists target_<target_db> cascade;

create server target_<target_db> foreign data wrapper postgres_fdw
  options (host '<target_host>', port '5432', dbname '<target_db>');

create user mapping for current_user server target_<target_db>
  options (user '<target_user>', password '<target_password>');

insert into leandex.target_databases(database_name, host, port, fdw_server_name, enabled)
values ('<target_db>', '<target_host>', 5432, 'target_<target_db>', true)
on conflict (database_name) do update
  set host = excluded.host, port = excluded.port, fdw_server_name = excluded.fdw_server_name, enabled = true;
```

Then re-check status:

```sql
select * from leandex.check_fdw_security_status();
```

### Excessive lock contention
```sql
-- Temporarily pause a target DB
update leandex.target_databases set enabled = false where database_name = '<db>';

-- Lower work by increasing size threshold for the DB
select leandex.set_or_replace_setting('<db>', null, null, null, 'index_size_threshold', '100MB', 'temporary throttle');
```

### Low disk space risk
```sql
-- Pause all reindex: global skip
select leandex.set_or_replace_setting(null, null, null, null, 'skip', 'true', 'global emergency pause');

-- Verify effective skip
select leandex.get_setting('<db>', null, null, null, 'skip');
```

## Emergency actions

### Immediate pause
```sql
-- Global pause (skip everywhere)
select leandex.set_or_replace_setting(null, null, null, null, 'skip', 'true', 'global pause');

-- Disable all scheduled jobs via pg_cron
select cron.unschedule('leandex_daily');
select cron.unschedule('leandex_weekly');
select cron.unschedule('leandex_monitor');
```

### Disable a specific target
```sql
update leandex.target_databases set enabled = false where database_name = '<db>';
```

### Uninstall
```sql
-- WARNING: removes schema and history in the current control database
\i uninstall.sql
```

Drop FDW servers/user mappings separately only after confirming they are not shared infrastructure.

## Upgrades

### Pre-checks and pause
```sql
-- Ensure no in-progress work
select * from leandex.current_processed_index;  -- should be empty

-- Pause schedules (pg_cron)
select cron.unschedule('leandex_daily');
select cron.unschedule('leandex_weekly');
```

### Backup state (optional but recommended)
```bash
# Dump leandex schema from control DB
pg_dump -h <control_host> -U <admin_user> -d <leandex_control_db> -n leandex -Fc -f /tmp/leandex_control.dump
```

### Apply update
```bash
cd leandex
git pull
psql -h <control_host> -U <admin_user> -d <leandex_control_db>
```

Inside `psql`, reload the single-file installer/update artifact:

```sql
\i leandex.sql
```

### Post-checks and resume
```sql
-- Migrate table structure if needed
select leandex.check_update_structure_version();

-- Verify version
select leandex.version();

-- Re-enable schedules
select cron.schedule_in_database(
  'leandex_daily', '0 2 * * *', 'call leandex.periodic(real_run := true);', 'leandex_control'
);
```

## Escalation

### Collect diagnostics
```sql
-- Versions and environment
select leandex.version() as leandex_version;
select current_setting('server_version') as server_version;
select * from leandex.check_fdw_security_status();
select * from leandex.check_permissions();

-- Recent failures
select * from leandex.history where status = 'failed' order by ts desc limit 100;

-- Config snapshot
select * from leandex.config order by key, datname nulls first, schemaname nulls first, relname nulls first, indexrelname nulls first;
```

```bash
# Export schema objects and recent history for support
pg_dump -h <control_host> -U <admin_user> -d <leandex_control_db> -n leandex -Fc -f ./leandex_schema.dump
psql    -h <control_host> -U <admin_user> -d <leandex_control_db> -c "copy (select * from leandex.reindex_history order by id desc limit 1000) to stdout with csv header" > ./reindex_history_sample.csv
```

### Notify and escalate
- **Provide**: the above dumps, `history` failures, FDW status, and recent configuration changes
- **State**: when the issue started, target DB(s) affected, change windows


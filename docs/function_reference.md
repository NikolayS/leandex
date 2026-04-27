## Function reference

### Table of contents

- [Core Functions](#core-functions)
- [Bloat Analysis](#bloat-analysis)
- [Non-Superuser Mode Functions](#non-superuser-mode-functions)
- [Configuration](#configuration)
- [FDW and connection setup](#fdw-and-connection-setup)
- [Maintenance helpers and meta](#maintenance-helpers-and-meta)

### Core Functions

#### `leandex.do_reindex()`
Manually triggers reindexing for specific objects.
```sql
procedure leandex.do_reindex(
    _datname name,
    _schemaname name,
    _relname name,
    _indexrelname name,
    _force boolean default false  -- Force reindex regardless of bloat
)
```

#### `leandex.periodic()`
Main procedure for automated bloat detection and reindexing.
```sql
procedure leandex.periodic(
    real_run boolean default false,  -- Execute actual reindexing
    force boolean default false      -- Force all eligible indexes
)
```

### Bloat Analysis

#### `leandex.get_index_bloat_estimates()`
Returns current bloat estimates for all indexes in a database.
```sql
function leandex.get_index_bloat_estimates(_datname name)
returns table(
    datname name,
    schemaname name,
    relname name,
    indexrelname name,
    indexsize bigint,
    estimated_bloat real
)
```

Notes:
- `estimated_bloat` is computed as `indexsize / (best_ratio * estimated_tuples)` using cached state in `leandex.index_latest_state`.
- Immediately after baseline initialization (see `do_force_populate_index_stats`) `estimated_bloat` will be ~1.0 by definition; it grows as indexes bloat further.

### Non-Superuser Mode Functions

#### `leandex.check_permissions()`
Verifies permissions for non-superuser mode operation.
```sql
function leandex.check_permissions()
returns table(
    permission text,
    status boolean
)
```

### Configuration

#### `leandex.get_setting()`
Reads effective setting with precedence (index → table → schema → db → global).
```sql
function leandex.get_setting(
  _datname text,
  _schemaname text,
  _relname text,
  _indexrelname text,
  _key text
) returns text
```

#### `leandex.set_or_replace_setting()`
Sets/overrides a setting at a specific scope.
```sql
function leandex.set_or_replace_setting(
  _datname text,
  _schemaname text,
  _relname text,
  _indexrelname text,
  _key text,
  _value text,
  _comment text
) returns void
```

### FDW and connection setup

#### `leandex._connect_securely()`
Internal helper that opens a dblink connection to a registered target database using the target's `postgres_fdw` server and current-user mapping.

```sql
function leandex._connect_securely(_datname name) returns void
```

Normal users should prefer `./leandex register-target`, which creates the foreign server, user mapping, and `leandex.target_databases` entry.

#### `leandex.check_fdw_security_status()`
Checks FDW-related setup status.
```sql
function leandex.check_fdw_security_status()
returns table(component text, status text, details text)
```

### Maintenance helpers and meta

#### `leandex.do_force_populate_index_stats()`
Initializes baseline using current sizes/tuples without reindex.
```sql
function leandex.do_force_populate_index_stats(
  _datname name,
  _schemaname name,
  _relname name,
  _indexrelname name
) returns void
```
Examples:
```sql
-- Initialize baseline for a target DB
select leandex.do_force_populate_index_stats('your_database', null, null, null);

-- Initialize baseline for one schema
select leandex.do_force_populate_index_stats('your_database', 'bot', null, null);
```

When to use:
- After initial registration, to establish best_ratio without reindexing.
- After major data reshaping, to reset baseline for specific schemas/tables.

#### `leandex.check_environment()`
Aggregated environment and installation self-check (PostgreSQL version, extensions, schema/tables, core routines presence).
```sql
function leandex.check_environment()
returns table(
  component text,
  is_ok boolean,
  details text
)
```

#### `leandex.check_update_structure_version()`
Migrates internal tables to the required version if needed.
```sql
function leandex.check_update_structure_version() returns void
```

#### `leandex.version()`
Returns current code version.
```sql
function leandex.version() returns text
```


# Architecture: design decisions and how it works

## Requirements

1. **Self-contained** - Everything inside Postgres
2. **Low-lock rebuilds** - uses `REINDEX INDEX CONCURRENTLY` only
3. **Managed-service oriented** - works where `CREATE DATABASE`, `dblink`, and `postgres_fdw` are available
4. **Safer connection handling** - uses `postgres_fdw` user mappings instead of plaintext dblink connection strings
5. **Not only btree** - designed for common index methods; BRIN is currently excluded due to [Postgres bug #17205](https://www.postgresql.org/message-id/flat/17205-42b1d8f131f0cf97%40postgresql.org)

## Design decisions

### `pg_cron` for scheduling
Enables self-contained operation without external schedulers. Available on most managed services. Optional - can trigger externally if unavailable.

We support two deployment scenarios:
- If pg_cron is not yet installed: install `pg_cron` in the control database (e.g., `index_pilot_control`). This keeps scheduling self-contained in the control DB.
- If pg_cron is already installed in another database: keep it as is and schedule jobs from that database using `cron.schedule_in_database(...)` to run commands in `index_pilot_control`. Note that pg_cron may only be installed in one database per cluster; `cron.schedule_in_database` is the supported way to run jobs targeting other databases.

### `dblink` for separate connections
`REINDEX INDEX CONCURRENTLY` cannot run in transaction blocks. `dblink` creates separate connection to execute reindex operations without blocking the control session.

### `postgres_fdw` for authentication
Uses `postgres_fdw` user mappings so dblink connects by server name instead of embedding plaintext passwords in dblink connection strings. Access to foreign servers and user mappings should be restricted to admin roles.

### Separate control database
Prevents deadlocks during reindex operations. Control database (`index_pilot_control`) manages all tracking while target databases remain clean without any `leandex` installation.

## Architecture diagram

```mermaid
graph TB
    subgraph "Control Database (index_pilot_control)"
        PGC[pg_cron scheduler]
        IPF[index_pilot functions]
        FDW[postgres_fdw servers]
        HIST[reindex_history]
        STATE[index_latest_state]
    end
    
    subgraph "Target Database 1"
        DB1[Tables & Indexes]
    end
    
    subgraph "Target Database 2"
        DB2[Tables & Indexes]
    end
    
    PGC -->|triggers| IPF
    IPF -->|secure connection| FDW
    FDW -->|dblink| DB1
    FDW -->|dblink| DB2
    IPF -->|REINDEX INDEX CONCURRENTLY| DB1
    IPF -->|REINDEX INDEX CONCURRENTLY| DB2
    IPF -->|logs results| HIST
    IPF -->|tracks bloat| STATE
```

## How it works

Operator-controlled maintenance loop:

1. Control database contains `index_pilot` schema and leandex functions
2. `postgres_fdw` user mappings hold target credentials, avoiding plaintext dblink connection strings
3. `dblink` executes `REINDEX INDEX CONCURRENTLY`
4. Bloat detection using Maxim Boguk's formula
5. `pg_cron` triggers periodic scans

## Compatibility

**Known target environments:**
- Self-managed Postgres 13+
- AWS RDS/Aurora
- Supabase

**Needs dedicated validation before claiming support:**
- Google Cloud SQL
- Azure Database for PostgreSQL
- Crunchy Bridge
- AlloyDB and other Postgres-compatible services

**Known limitation:**
- Services without `CREATE DATABASE`, `dblink`, or `postgres_fdw` support are not suitable for the current control-database design.

**Extensions:**
- `dblink` (required)
- `postgres_fdw` (required)
- `pg_cron` (optional)

## Bloat detection formula

Uses Maxim Boguk's formula (originally implemented in `pg_index_watch`) instead of traditional inaccurate bloat estimates (btree-only) or heavy `pgstattuple`-based methods (unusable in large databases):

```
bloat_indicator = index_size / pg_class.reltuples
```

**Advantages:**
- Designed for common index methods such as btree, GIN, GiST, hash, and HNSW; BRIN is excluded due to [Postgres bug #17205](https://www.postgresql.org/message-id/flat/17205-42b1d8f131f0cf97%40postgresql.org)
- Lightweight - no expensive table scans
- Better precision for fixed-width columns
- No superuser required

**How it works:**
1. Measure baseline after `REINDEX INDEX CONCURRENTLY`
2. Monitor ratio changes over time
3. Trigger reindex when ratio exceeds threshold

**Limitations:**
- Requires initial reindex to establish baseline
- Variable-length data (`text`, `jsonb`) may cause false positives if average size changes significantly

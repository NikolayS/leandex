# Contributing to leandex

This file is for contributor workflow. The README is for users deciding whether to use leandex and how to get started.

## Ground rules

- Keep changes focused and small.
- Prefer SQL-first workflows; do not add wrapper CLIs unless there is a strong reason.
- Add or update tests for behavior changes.
- Keep docs aligned with the actual install path: `psql` plus `\i leandex.sql`.
- Do not commit secrets, passwords, dumps, or production identifiers.

## Development setup

Prerequisites:

- Postgres 13 or newer
- `psql`
- Bash
- Docker, optional but useful for local Postgres

Quick local Postgres:

```bash
docker run --rm -d \
  --name leandex-dev-pg \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres:18-alpine
```

## Install from a checkout

Create a control database and load the single-file SQL artifact:

```bash
createdb -h <host> -U <user> leandex_control
psql -h <host> -U <user> -d leandex_control
```

Inside `psql`:

```sql
create extension if not exists postgres_fdw;
create extension if not exists dblink;
\i leandex.sql
```

Register a target database:

```sql
create server target_<db>
  foreign data wrapper postgres_fdw
  options (host '<target_host>', port '5432', dbname '<db>');

create user mapping for current_user
  server target_<db>
  options (user '<target_user>', password '<target_password>');

insert into leandex.target_databases(database_name, host, port, fdw_server_name, enabled)
values ('<db>', '<target_host>', 5432, 'target_<db>', true)
on conflict (database_name) do update
  set
    host = excluded.host,
    port = excluded.port,
    fdw_server_name = excluded.fdw_server_name,
    enabled = true;
```

Verify:

```sql
select * from leandex.check_fdw_security_status();
select * from leandex.check_environment();
```

## Testing

Run the SQL suite against an existing Postgres:

```bash
PGPASSWORD=postgres ./test/run_tests.sh \
  -h 127.0.0.1 -p 5432 -u postgres -w postgres -d test_leandex
```

Run the PG18 end-to-end bloat reduction scenario over a Docker network:

```bash
net="leandex-e2e-local"
pg="leandex-e2e-pg"
docker network create "$net"
docker run -d --name "$pg" --network "$net" --network-alias postgres \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres \
  postgres:18-alpine

docker run --rm --network "$net" -v "$PWD:/work" -w /work \
  -e DB_HOST=postgres -e DB_PORT=5432 \
  -e DB_USER=postgres -e DB_PASS=postgres \
  -e DB_NAME=test_leandex_e2e -e FDW_HOST=postgres \
  postgres:18-alpine \
  sh -lc 'apk add --no-cache bash >/dev/null && bash ci/e2e_bloat_reduction.sh'

docker rm -f "$pg"
docker network rm "$net"
```

Before opening a PR, run the smallest meaningful gate for the change. For SQL behavior changes, that usually means the SQL suite plus the PG18 e2e scenario.

## CI coverage

GitHub Actions runs:

- shell formatting and shellcheck;
- SQL security grep checks;
- SQL test suite on Postgres 13, 14, 15, 16, 17, and 18;
- SQL install verification on Postgres 13 through 18 over a Docker network;
- PG18 e2e bloat reduction.

## Style

### SQL

- Use lowercase SQL keywords.
- Use `snake_case` identifiers.
- Prefer explicit joins and explicit aliases with `as`.
- Prefer CTEs over deeply nested subqueries.
- Use one argument per line for long multi-argument calls.
- Comment non-trivial operational logic.

### Shell

- Use Bash for repository scripts.
- Use two-space indentation.
- Quote variables consistently.
- Prefer `$(...)` command substitution.
- Run ShellCheck and shfmt for changed scripts.

## Commits and PR titles

Use simplified Conventional Commits:

- `feat:` new functionality
- `fix:` bug fix
- `perf:` performance improvement
- `docs:` documentation only
- `chore:` infrastructure, CI, dependencies
- `test:` tests only
- `refactor:` refactor without behavior change

Breaking changes use `!`, for example `feat!: drop support for Postgres 13`.

Keep PRs focused. Include rationale, test evidence, migration notes when relevant, and issue references when available.

## Review checklist

- Tests added or updated for behavior changes.
- Local gate passed and evidence is in the PR.
- Docs updated when behavior, setup, or UX changes.
- Security checked: no secrets, no unsafe FDW/user-mapping shortcuts.
- Backward compatibility considered; breaking changes are explicit.

## Reporting issues

Please include:

- Postgres version: `select current_setting('server_version');`
- leandex version: `select leandex.version();`
- `select * from leandex.check_fdw_security_status();`
- `select * from leandex.check_environment();`
- minimal reproduction steps;
- expected and actual behavior.

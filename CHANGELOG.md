# Changelog

## Unreleased

- Extract leandex from the PostgresAI monorepo component formerly known as `pg_index_pilot`.
- Keep the internal SQL schema as `index_pilot` for compatibility.
- Add GitHub Actions CI for shell linting, SQL security checks, PostgreSQL 13-18 tests, installer verification, and e2e bloat reduction.
- Rename the installer entry point to `leandex`; keep `index_pilot.sh` as a compatibility wrapper.
- Add `leandex.sql` as a single-file SQL installer and make `./leandex install-control` prefer it.

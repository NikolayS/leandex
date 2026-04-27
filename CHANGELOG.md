# Changelog

## Unreleased

- Extract leandex from the PostgresAI monorepo component formerly known as `pg_leandex`.
- Keep the internal SQL schema as `leandex` for compatibility.
- Add GitHub Actions CI for shell linting, SQL security checks, PostgreSQL 13-18 tests, installer verification, and e2e bloat reduction.
- Rename the installer entry point to `leandex`.
- Add `leandex.sql` as a single-file SQL installer and make `./leandex install-control` prefer it.

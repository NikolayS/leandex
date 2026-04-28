# Changelog

## Unreleased

- Extract leandex from the PostgresAI monorepo component formerly known as `pg_leandex`.
- Keep the internal SQL schema as `leandex` for compatibility.
- Add GitHub Actions CI for shell linting, SQL security checks, PostgreSQL 13-18 tests, SQL install verification, and e2e bloat reduction.
- Add `leandex.sql` as the single-file SQL installer.

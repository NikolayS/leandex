# leandex

Keep your Postgres indexes lean — rebuild, drop, and suggest.

**Status:** Early development.

## What it does

- **Rebuild** — mitigate index bloat via `REINDEX CONCURRENTLY`, zero downtime
- **Drop** — remove unused, redundant, and invalid indexes safely
- **Suggest** — recommend missing indexes based on query workload

Invalid indexes are handled intelligently: rebuild when fixable, drop when redundant.

## License

Apache 2.0

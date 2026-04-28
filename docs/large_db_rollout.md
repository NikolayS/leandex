# Large DB rollout

## Goal
Roll out `leandex` on large production databases without turning maintenance into a self-inflicted outage.

## Principles
- Start with observation, not action.
- Keep one active reindex starter per target DB until proven safe.
- Gate starts with real maintenance windows.
- Let a started reindex finish; do not try to fake a duration cap with `statement_timeout`.
- Watch replicas, WAL, and disk like a hawk.

## Phase 0 — preflight

Before enabling real runs:

- verify FDW connectivity and permissions from the control DB;
- run `select leandex.check_update_structure_version();`;
- initialize baseline with `select leandex.do_force_populate_index_stats('<db>', null, null, null);`;
- review top candidates with `select * from leandex.get_index_bloat_estimates('<db>') order by estimated_bloat desc nulls last limit 50;`;
- confirm free disk headroom for worst-case concurrent rebuilds.

## Phase 1 — dry runs only

For at least a few cycles:

```sql
call leandex.periodic(false);
select * from leandex.history order by ts desc limit 50;
```

You want to learn:
- which indexes keep surfacing;
- whether any guardrails are firing;
- whether target registration and user mappings are boringly reliable.

## Phase 2 — narrow real runs

Start with one target DB and a hard window:

```sql
select leandex.set_or_replace_setting(
  '<db>', null, null, null,
  'allowed_start_windows',
  '[{"days":[1,2,3,4,5,6,7],"start":"01:00","end":"04:00"}]',
  'initial rollout window'
);

select leandex.set_or_replace_setting(
  '<db>', null, null, null,
  'min_window_remaining',
  '30 minutes',
  'avoid starting near window end'
);

select leandex.set_or_replace_setting(
  '<db>', null, null, null,
  'max_parallel_reindexes',
  '1',
  'default rollout limit'
);
```

Then run:

```sql
call leandex.periodic(true);
```

## Phase 3 — inspect the blast radius

After each real run, check:

```sql
select * from leandex.history order by ts desc limit 50;
select * from leandex.current_processed_index;
select * from leandex.get_index_bloat_estimates('<db>') order by estimated_bloat desc nulls last limit 20;
```

Outside `leandex`, also check:
- WAL generation and replica lag;
- write latency and lock waits;
- storage growth during concurrent rebuilds;
- autovacuum pressure on the same tables.

## Phase 4 — widen carefully

Only widen one variable at a time:

1. more databases,
2. broader windows,
3. lower size thresholds,
4. more parallelism.

Do not jump straight to `max_parallel_reindexes > 1` unless you've measured IO, WAL, and replica impact. Most fleets do not need that hero move.

## Recommended defaults

Keep these unless production evidence says otherwise:

- `lock_timeout = '30s'`
- `idle_in_transaction_session_timeout = '1min'`
- `idle_session_timeout = '0'` on PG14+
- `statement_timeout = 0`
- `respect_external_index_activity = true`
- `max_parallel_reindexes = 1`

## What to do when guardrails fire

- `outside allowed start window` — your schedule is wrong or the window is too narrow.
- `min_window_remaining not satisfied` — good; it saved you from starting too late.
- `external index activity: ...` — some other job is already doing index work. Back off.
- `max_parallel_reindexes reached` — intentional throttle, not a bug.
- `old snapshot: ...` — a backend has a snapshot that can stall `reindex index concurrently`; let it finish before trying again.
- `blocking transaction: ...` — investigate the long transaction before trying again.

## Emergency rollback

```sql
update leandex.target_databases
set enabled = false
where database_name = '<db>';
```

Or globally pause starts:

```sql
select leandex.set_or_replace_setting(null, null, null, null, 'skip', 'true', 'emergency pause');
```

That's the boring rollout. Boring is good.

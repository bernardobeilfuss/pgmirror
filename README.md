# pgmirror

Copy tables from several Postgres databases into one, incrementally, driven by a
YAML file. Adding a table is a config entry, not code.

```yaml
tables:
  - name: sale_item
    mode: incremental
    watermark_column: issued_at
    overlap_days: 7
```

It was extracted from a system that has mirrored the ERP databases of a group of
retail stores since May 2026 — every two hours during the day and once at night,
53 tables, three source databases, and one program that never needed a change to
take on a new table. This repository is
that code made generic: the company-specific names, hosts and rules are gone,
and the tests run against a real Postgres.

## Why it exists, in one incident

Five days after the first version went live, a stock report built on the copy
came out **3.5 times** higher than the real stock. No row was wrong. Each source
database also stored a copy of the other companies' stock; read one database at
a time, nobody noticed; summed in one place, every store was counted several times.

There were two easy fixes and both were wrong. Filtering inside the report means
every other report has to rediscover the trap. Cleaning the data on the way in
means the copy can no longer be checked against the source: any difference
might come from the source or from a rule in the middle.

So the design has one rule: **the copy stays faithful; deciding which copy is
true happens in one declared place, downstream of it** — in the original system,
a view that knows which database owns each store, and that every report reads
instead of the mirror. pgmirror is the half before that view: it copies, and it
does not decide.

There is one exception, and it is declared rather than hidden: `row_filter`
(below) lets a source copy only the rows that belong to it. It chooses *what to
copy*; it never rewrites *what came*. It lives in the config, next to the reason,
where anyone can review it.

Longer write-up (Portuguese): [*A cópia estava perfeita. A soma, não.*](https://www.linkedin.com/pulse/c%C3%B3pia-estava-perfeita-soma-n%C3%A3o-bernardo-beilfuss-v0vsf/)

## What it does — and does not

| Does | Does not |
|---|---|
| Postgres → Postgres, many sources into one destination | Other databases |
| Incremental copy by a date/timestamp column, with a look-back window | Change data capture, logical replication |
| Full replacement for small or volatile tables | Transformations — the mirror is the source's shape |
| Tolerates columns added on either side | Create or migrate the mirror tables for you (it prints a starting `CREATE TABLE`) |
| One transaction per (source, table): all of it lands, or none | Retries, alerting — it gives you exit codes and a run log |

## Quick start

```bash
pip install -e .
cp examples/pgmirror.example.yaml pgmirror.yaml   # edit hosts, tables
export WAREHOUSE_PASSWORD=... NORTH_PASSWORD=... SOUTH_PASSWORD=...

pgmirror check                                   # can I reach every database?
pgmirror init                                    # mirror + state schemas, state tables
pgmirror ddl --source north --table sale_item    # review, then run it on the destination
pgmirror run --dry-run                           # read and count, write nothing
pgmirror run
```

A typical cron: the small tables every two hours with a short look-back,
everything once at night with the configured one. Cron runs from `$HOME`, so use
absolute paths.

```cron
0 7-21/2 * * *  /opt/pgmirror/.venv/bin/pgmirror --config /etc/pgmirror.yaml run --schedule hourly --overlap-hours 4
0 22 * * *      /opt/pgmirror/.venv/bin/pgmirror --config /etc/pgmirror.yaml run
```

Two runs never write at the same time: each run takes an advisory lock **in the
destination database**, so the guarantee holds across cron entries, working
directories and machines. A second run exits with code 3.

## How the incremental copy works

For each (source, table) pair:

1. Read the last watermark from `etl.etl_state` (or `initial_watermark` on the first run).
2. `cutoff = watermark − overlap_days`.
3. In one destination transaction: delete this source's rows with
   `watermark_column >= cutoff`, insert what the source has from `cutoff` on,
   store the new watermark.

```
        already copied                   re-copied every run
  ──────────────────────────┬──────────────────────────────────▶ time
                         cutoff        last watermark       now
                            └──── overlap_days ────┘
```

**The overlap is a business number, not a technical one.** It is how long the
source takes to correct itself: a sale cancelled two days after it happened
changes a row the previous run already copied. In the system this came from it
is 7 days for sales, 30 for payables and 90 for slow-moving registers. A change
older than the window is never seen — choose the window from how the operation
behaves, not from how fast you want the run to be.

**The watermark never moves past now.** One row dated 2099 — a typo, a
scheduled delivery — would otherwise push the cutoff into the future and every
later run would copy nothing, silently. The row is still copied; it just does
not move the watermark. (This happened in production; the test that reproduces
it is `test_watermark_never_runs_into_the_future`.)

Both sides run in UTC sessions, so a `timestamptz` watermark means the same
instant on the source (SELECT) and on the destination (DELETE), whatever their
server time zones are; it is stored in UTC. Plain `date` and `timestamp` columns
are compared as they are.

## Row filters: declaring which copy is the real one

When a source database holds rows that belong to another source, declare it
once, on the source:

```yaml
sources:
  north:
    ...
    row_filter: { column: company_id, values: [1, 2] }
```

Every table whose source has a `company_id` column is filtered; tables without it
(shared lookups) are copied whole, and the log says the filter was not applied
there — so a typo in the column name shows up on the first run. If the source
has the column but the mirror table does not, the pair **fails**: a filter
nobody can check afterwards is one `ALTER TABLE ... DROP COLUMN` away from
silently protecting nothing. A table can opt out or override:

```yaml
  - name: customer
    mode: full
    row_filter_by_source:
      north: null        # north is the registry for everyone: no filter there
```

In incremental mode the delete window **ignores** the filter on purpose: rows
that should no longer be copied disappear as the window passes over them,
instead of living in the mirror forever.

## Columns are discovered, not declared

At run time pgmirror reads `information_schema` on both sides:

- column in the destination but not in the source → written as `NULL`, and logged;
- column in the source but not in the destination → ignored;
- `_source` (required) and `_synced_at` (optional) are provenance, filled by the
  tool; every other column, including ones that start with `_`, is data.

Adding a column to the mirror is an `ALTER TABLE` on the destination.

## When something fails

| Exit | Meaning |
|---|---|
| 0 | every pair succeeded |
| 1 | at least one pair failed; the others still ran |
| 2 | config or environment error; nothing ran |
| 3 | another run holds the lock; nothing ran |
| 4 | the destination became unreachable; the run stopped where it was |

A failing pair is rolled back and leaves the mirror and its watermark exactly as
they were — including a failure halfway through the insert, after the window
was already deleted (`test_failure_mid_insert_rolls_back_the_delete`). An
unreachable source fails its own pairs, never the whole run. Every attempt is in
the run log:

```sql
SELECT source, table_name, status, rows_written, error, finished_at - started_at AS took
FROM etl.etl_run_log
WHERE started_at > now() - interval '1 day' AND status <> 'ok';
```

The config is validated before any connection is opened, and it fails closed:
an unknown key, a misspelled mode, a table name that is not a plain identifier,
a missing password variable, a `--tables` subset naming a table that does not
exist — each one stops the run with exit 2. A typo in a file that drives
`DELETE` statements is never "probably fine". All identifiers are also quoted
with `psycopg2.sql.Identifier`; nothing from the config is pasted into SQL.

## Limitations, honestly

- **Deletes older than the window are not seen** in incremental mode. If the
  source hard-deletes old rows, give that table `mode: full`.
- The watermark column must be a `date` or `timestamp` that moves forward when a
  row changes (or at least is fixed at creation and the window covers the edits).
  Integer ids are not supported.
- **Two cases remove a row from the mirror** in incremental mode, because the
  window is deleted and the row is not read back: its watermark is edited to a
  date before the cutoff, or set to `NULL`. Rows whose watermark is `NULL` are
  never copied at all. If either can happen in your source, use `mode: full` for
  that table.
- Rows are streamed in batches of 20,000 from a server-side cursor, but the
  destination transaction for a pair holds the whole window. A very large first
  load is best done with a recent `initial_watermark` and widened step by step.
- One password per connection, from an environment variable. No secrets manager
  integration.

## Tests

```bash
pip install -e ".[dev]"
pytest                      # starts an embedded Postgres via `pgserver`
PGMIRROR_TEST_DSN=postgresql://postgres:pw@localhost:5432/postgres pytest   # or use yours
```

The integration tests are situations, not functions: a sale cancelled after it
was copied, a typo dated 2099, a column added on one side only, one source down
while the other is fine, the 3.5× stock — with and without the filter.

`tests/test_review_findings.py` holds one test per defect found in code review
before the first release, each reproducing the situation the defect caused — a
destination in another time zone duplicating rows, a filter switched off by a
missing column, a failure halfway through an insert. They failed before the fix.

## License

MIT

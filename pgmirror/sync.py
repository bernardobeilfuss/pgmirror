"""The two copy strategies.

Both follow the same contract: the destination table is a faithful mirror of
the source table, plus provenance columns whose names start with ``_``:

* ``_source``    (required) — which source the row came from.
* ``_synced_at`` (optional) — when this run wrote it.

Columns are discovered at run time from ``information_schema``. A column that
exists in the destination but not in the source is written as NULL (and
logged); a column that exists only in the source is ignored. Adding a column
to the copy is ``ALTER TABLE`` on the destination, not a code change.

Each (source, table) pair is written in a single destination transaction:
DELETE of the window, INSERT of what the source returned, watermark update.
Either all of it lands or none of it does.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import psycopg2.extras
from psycopg2 import sql

from pgmirror import state
from pgmirror.config import Destination, RowFilter, Source, Table

BATCH_SIZE = 20_000
PROVENANCE = ("_source", "_synced_at")

Log = Callable[[str], None]


@dataclass(frozen=True)
class Result:
    pulled: int
    written: int
    watermark: datetime | None


# ---------------------------------------------------------------------------
# Column discovery
# ---------------------------------------------------------------------------

def table_columns(conn, schema: str, table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (schema, table),
        )
        return [r[0] for r in cur.fetchall()]


WATERMARK_TYPES = ("date", "timestamp without time zone", "timestamp with time zone")


def column_type(conn, schema: str, table: str, column: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
            (schema, table, column),
        )
        row = cur.fetchone()
    return row[0] if row else None


@dataclass(frozen=True)
class ColumnPlan:
    copy: list[str]          # read from the source, in destination order
    provenance: list[str]    # filled by the tool
    missing: list[str]       # in the destination, absent in the source → NULL
    source: set[str]         # every column the source table has


def plan_columns(dest_cols: list[str], src_cols: list[str], where: str) -> ColumnPlan:
    if not dest_cols:
        raise RuntimeError(f"{where}: destination table does not exist "
                           "(`pgmirror ddl` prints a CREATE TABLE for it)")
    if not src_cols:
        raise RuntimeError(f"{where}: source table does not exist or is not readable")
    if "_source" not in dest_cols:
        raise RuntimeError(f"{where}: destination table has no _source column")
    src = set(src_cols)
    # Only these two names are reserved. A source column that happens to start
    # with "_" is data like any other and is copied.
    data = [c for c in dest_cols if c not in PROVENANCE]
    return ColumnPlan(
        copy=[c for c in data if c in src],
        provenance=[c for c in dest_cols if c in PROVENANCE],
        missing=[c for c in data if c not in src],
        source=src,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _filter_sql(row_filter: RowFilter | None, plan: ColumnPlan, where: str,
                log: Log) -> tuple[sql.Composable | None, tuple]:
    """`column = ANY(values)` when the source table has that column.

    Tables whose source has no such column (lookups shared by every company, say)
    are copied unfiltered — the same filter is declared once per source, not per
    table — and the log says so, so a typo in the column name is visible.

    A source that HAS the column while the mirror does not is refused: the filter
    would apply to the read but nobody could check it afterwards, and the first
    time someone drops that column "because the mirror doesn't need it" the
    filter would quietly stop protecting anything.
    """
    if row_filter is None:
        return None, ()
    if row_filter.column not in plan.source:
        log(f"  [{where}] row_filter on {row_filter.column!r} not applied: "
            "the source table has no such column")
        return None, ()
    if row_filter.column not in plan.copy:
        raise RuntimeError(f"{where}: row_filter column {row_filter.column!r} exists in the source "
                           "but not in the destination table; add it to the mirror")
    return (sql.SQL("{} = ANY(%s)").format(sql.Identifier(row_filter.column)),
            (list(row_filter.values),))


def _as_naive(value: Any, where: str) -> datetime:
    """Watermarks are stored as naive timestamps; timestamptz values in UTC."""
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    raise RuntimeError(f"{where}: watermark column holds {type(value).__name__}, "
                       "expected date or timestamp")


def _stream(src_conn, query: sql.Composable, params: tuple, name: str) -> Iterator[list[tuple]]:
    # Named (server-side) cursor: the source never ships the whole table at once.
    with src_conn.cursor(name=name) as cur:
        cur.itersize = BATCH_SIZE
        cur.execute(query, params)
        while True:
            batch = cur.fetchmany(BATCH_SIZE)
            if not batch:
                return
            yield batch


def _insert(dest_conn, dest: Destination, table: str, plan: ColumnPlan,
            rows: list[tuple], provenance_values: tuple) -> None:
    cols = plan.copy + plan.provenance
    stmt = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
        sql.Identifier(dest.schema), sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, cols)),
    )
    with dest_conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur, stmt.as_string(dest_conn),
            [tuple(r) + provenance_values for r in rows], page_size=2000,
        )


def _provenance_values(plan: ColumnPlan, source: str) -> tuple:
    known = {"_source": source, "_synced_at": datetime.now(timezone.utc)}
    return tuple(known.get(c) for c in plan.provenance)


def _select(source: Source, table: str, plan: ColumnPlan,
            where: list[sql.Composable]) -> sql.Composable:
    q = sql.SQL("SELECT {} FROM {}.{}").format(
        sql.SQL(", ").join(map(sql.Identifier, plan.copy)),
        sql.Identifier(source.schema), sql.Identifier(table),
    )
    if where:
        q = q + sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where)
    return q


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

def sync_incremental(dest_conn, src_conn, dest: Destination, source: Source, table: Table,
                     *, dry_run: bool = False, overlap_hours: int | None = None,
                     log: Log = print) -> Result:
    """Re-copy everything from (last watermark − overlap) onwards.

    The overlap is what makes this safe against edits in the past: a sale
    cancelled two days after it happened changes a row the previous run already
    copied. Rows are only ever re-read inside the window, so the overlap must be
    at least as long as the source takes to correct itself.
    """
    where = f"{source.name}/{table.name}"
    wm_col = table.watermark_column
    assert wm_col is not None  # guaranteed by config validation

    plan = plan_columns(table_columns(dest_conn, dest.schema, table.name),
                        table_columns(src_conn, source.schema, table.name), where)
    if wm_col not in plan.copy:
        raise RuntimeError(f"{where}: watermark column {wm_col!r} is missing in source or destination")
    wm_type = column_type(src_conn, source.schema, table.name, wm_col)
    if wm_type not in WATERMARK_TYPES:
        raise RuntimeError(f"{where}: watermark column {wm_col!r} is {wm_type}, "
                           "expected date or timestamp")
    if plan.missing:
        log(f"  [{where}] in destination but not in source (written as NULL): {plan.missing}")

    last = state.get_watermark(dest_conn, dest.state_schema, source.name, table.name)
    if last is None:
        last = table.initial_watermark
    if last is None:
        raise RuntimeError(f"{where}: no stored watermark and no initial_watermark")

    overlap = (timedelta(hours=overlap_hours) if overlap_hours is not None
               else timedelta(days=table.overlap_days))
    cutoff = last - overlap

    conditions: list[sql.Composable] = [sql.SQL("{} >= %s").format(sql.Identifier(wm_col))]
    params: tuple = (cutoff,)
    flt, flt_params = _filter_sql(table.filter_for(source), plan, where, log)
    if flt is not None:
        conditions.append(flt)
        params += flt_params
    log(f"  [{where}] incremental from {cutoff:%Y-%m-%d %H:%M}"
        + (f", {table.filter_for(source).column} filtered" if flt is not None else ""))

    wm_idx = plan.copy.index(wm_col)
    new_wm = last
    pulled = 0
    provenance = _provenance_values(plan, source.name)
    query = _select(source, table.name, plan, conditions)

    if not dry_run:
        # The DELETE window mirrors the SELECT window but ignores the row filter on
        # purpose: rows that should no longer be copied disappear as the window
        # passes over them, instead of living in the mirror forever.
        with dest_conn.cursor() as cur:
            cur.execute(
                sql.SQL("DELETE FROM {}.{} WHERE _source = %s AND {} >= %s").format(
                    sql.Identifier(dest.schema), sql.Identifier(table.name), sql.Identifier(wm_col)),
                (source.name, cutoff),
            )
            deleted = cur.rowcount

    for batch in _stream(src_conn, query, params, f"pgmirror_{table.name}"):
        pulled += len(batch)
        for r in batch:
            if r[wm_idx] is not None:
                v = _as_naive(r[wm_idx], where)
                if v > new_wm:
                    new_wm = v
        if not dry_run:
            _insert(dest_conn, dest, table.name, plan, batch, provenance)

    # Never move the watermark past "now". One row dated 2099 (a typo, a scheduled
    # delivery) would otherwise push the cutoff into the future and every later run
    # would copy nothing — silently. The row itself is still copied. The ceiling is
    # lenient (the later of local time and UTC) because it only exists to stop that
    # freeze, not to second-guess the clock of the source.
    ceiling = max(datetime.now(), datetime.now(timezone.utc).replace(tzinfo=None))
    if new_wm > ceiling:
        log(f"  [{where}] future watermark {new_wm:%Y-%m-%d} clamped to now")
        new_wm = ceiling

    src_conn.rollback()  # end the read-only transaction before committing anything
    if dry_run:
        dest_conn.rollback()
        log(f"  [{where}] dry run: {pulled:,} rows would be written")
        return Result(pulled, 0, last)

    state.set_watermark(dest_conn, dest.state_schema, source.name, table.name, new_wm, pulled)
    dest_conn.commit()
    log(f"  [{where}] deleted={deleted:,} inserted={pulled:,} watermark={new_wm:%Y-%m-%d %H:%M}")
    return Result(pulled, pulled, new_wm)


def sync_full(dest_conn, src_conn, dest: Destination, source: Source, table: Table,
              *, dry_run: bool = False, log: Log = print) -> Result:
    """Replace every row of this source with what the source has now."""
    where = f"{source.name}/{table.name}"
    plan = plan_columns(table_columns(dest_conn, dest.schema, table.name),
                        table_columns(src_conn, source.schema, table.name), where)
    if plan.missing:
        log(f"  [{where}] in destination but not in source (written as NULL): {plan.missing}")

    flt, flt_params = _filter_sql(table.filter_for(source), plan, where, log)
    query = _select(source, table.name, plan, [flt] if flt is not None else [])
    log(f"  [{where}] full copy" + (" (filtered)" if flt is not None else ""))

    if not dry_run:
        with dest_conn.cursor() as cur:
            cur.execute(
                sql.SQL("DELETE FROM {}.{} WHERE _source = %s").format(
                    sql.Identifier(dest.schema), sql.Identifier(table.name)),
                (source.name,),
            )
            deleted = cur.rowcount

    pulled = 0
    provenance = _provenance_values(plan, source.name)
    for batch in _stream(src_conn, query, flt_params, f"pgmirror_{table.name}"):
        pulled += len(batch)
        if not dry_run:
            _insert(dest_conn, dest, table.name, plan, batch, provenance)

    src_conn.rollback()
    if dry_run:
        dest_conn.rollback()
        log(f"  [{where}] dry run: {pulled:,} rows would be written")
        return Result(pulled, 0, None)

    state.set_watermark(dest_conn, dest.state_schema, source.name, table.name, None, pulled)
    dest_conn.commit()
    log(f"  [{where}] deleted={deleted:,} inserted={pulled:,}")
    return Result(pulled, pulled, None)


# ---------------------------------------------------------------------------
# DDL helper
# ---------------------------------------------------------------------------

def create_table_ddl(src_conn, source: Source, dest: Destination, table: str) -> str:
    """A CREATE TABLE for the mirror, with the source's column types + provenance.

    A starting point to review, not something the tool runs on its own: indexes
    and primary keys on the mirror are a decision about how it will be queried.
    """
    with src_conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.attname, format_type(a.atttypid, a.atttypmod)
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            (source.schema, table),
        )
        cols = cur.fetchall()
    src_conn.rollback()
    if not cols:
        raise RuntimeError(f"{source.name}/{table}: source table not found")
    lines = [sql.SQL("    {} {}").format(sql.Identifier(n), sql.SQL(t)) for n, t in cols]
    lines.append(sql.SQL("    _source text NOT NULL"))
    lines.append(sql.SQL("    _synced_at timestamptz"))
    stmt = sql.SQL(
        "CREATE SCHEMA IF NOT EXISTS {schema};\n"
        "CREATE TABLE IF NOT EXISTS {schema}.{table} (\n{cols}\n);\n"
        "-- Incremental tables: the window DELETE filters on (_source, <watermark_column>).\n"
        "-- CREATE INDEX ON {schema}.{table} (_source, <watermark_column>);"
    ).format(schema=sql.Identifier(dest.schema), table=sql.Identifier(table),
             cols=sql.SQL(",\n").join(lines))
    return stmt.as_string(src_conn)

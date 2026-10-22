"""Watermarks and run log, stored in the destination database.

Two tables in `destination.state_schema`:

* ``etl_state`` — one row per (source, table): the last watermark copied.
* ``etl_run_log`` — one row per (source, table) attempt, append-only. This is
  what you query when something looks off: did the run happen, how many rows
  came, what failed.
"""

from __future__ import annotations

from datetime import datetime

from psycopg2 import sql

BOOTSTRAP = """
CREATE SCHEMA IF NOT EXISTS {mirror};
CREATE SCHEMA IF NOT EXISTS {schema};

CREATE TABLE IF NOT EXISTS {schema}.etl_state (
    source          text        NOT NULL,
    table_name      text        NOT NULL,
    last_watermark  timestamp,
    last_run_at     timestamptz NOT NULL DEFAULT now(),
    rows_loaded     bigint,
    PRIMARY KEY (source, table_name)
);

CREATE TABLE IF NOT EXISTS {schema}.etl_run_log (
    id            bigserial   PRIMARY KEY,
    source        text        NOT NULL,
    table_name    text        NOT NULL,
    mode          text        NOT NULL,
    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz,
    rows_pulled   bigint,
    rows_written  bigint,
    status        text        NOT NULL,
    error         text
);
"""


def bootstrap(conn, schema: str, mirror_schema: str) -> None:
    """Create the mirror schema, the state schema and the state tables. Idempotent."""
    with conn.cursor() as cur:
        cur.execute(sql.SQL(BOOTSTRAP).format(schema=sql.Identifier(schema),
                                              mirror=sql.Identifier(mirror_schema)))
    conn.commit()


def get_watermark(conn, schema: str, source: str, table: str) -> datetime | None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT last_watermark FROM {}.etl_state "
                    "WHERE source = %s AND table_name = %s").format(sql.Identifier(schema)),
            (source, table),
        )
        row = cur.fetchone()
    return row[0] if row else None


def set_watermark(conn, schema: str, source: str, table: str,
                  watermark: datetime | None, rows: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("""
                INSERT INTO {}.etl_state (source, table_name, last_watermark, last_run_at, rows_loaded)
                VALUES (%s, %s, %s, now(), %s)
                ON CONFLICT (source, table_name) DO UPDATE
                SET last_watermark = EXCLUDED.last_watermark,
                    last_run_at    = EXCLUDED.last_run_at,
                    rows_loaded    = EXCLUDED.rows_loaded
            """).format(sql.Identifier(schema)),
            (source, table, watermark, rows),
        )


def start_run(conn, schema: str, source: str, table: str, mode: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("INSERT INTO {}.etl_run_log (source, table_name, mode, status) "
                    "VALUES (%s, %s, %s, 'running') RETURNING id").format(sql.Identifier(schema)),
            (source, table, mode),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def finish_run(conn, schema: str, run_id: int, pulled: int, written: int,
               status: str, error: str | None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("UPDATE {}.etl_run_log SET finished_at = now(), rows_pulled = %s, "
                    "rows_written = %s, status = %s, error = %s WHERE id = %s"
                    ).format(sql.Identifier(schema)),
            (pulled, written, status, error, run_id),
        )
    conn.commit()

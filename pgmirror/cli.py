"""Command line: ``pgmirror run | init | ddl | check``.

Exit codes are the interface for cron and for whatever alerts you:

    0  every (source, table) pair succeeded
    1  at least one pair failed — the others still ran; see etl_run_log
    2  config or environment error — nothing ran
    3  another run holds the lock — nothing ran
    4  the destination became unreachable — the run stopped where it was
"""

from __future__ import annotations

import argparse
import contextlib
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2

from pgmirror import config as cfgmod
from pgmirror import state, sync

EXIT_OK, EXIT_FAILED, EXIT_CONFIG, EXIT_LOCKED, EXIT_DESTINATION = 0, 1, 2, 3, 4

# UTC sessions on both sides. A naive watermark compared against a timestamptz
# column is read in the session time zone; if source and destination disagreed,
# the SELECT window and the DELETE window would start at different instants and
# rows would be duplicated or lost on every run. "UTC0" is the POSIX spelling:
# it also works on builds without the tz database, where "UTC" is rejected.
_UTC = "-c TimeZone=UTC0"


class DestinationUnavailable(RuntimeError):
    """The destination cannot be reached; nothing else can be recorded."""


class LockBusy(RuntimeError):
    """Another run holds the lock."""


def log(msg: str) -> None:
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def connect_destination(dest: cfgmod.Destination):
    try:
        return psycopg2.connect(
            connect_timeout=30,
            options=f"{_UTC} -c statement_timeout={dest.statement_timeout_ms}",
            **dest.connect_kwargs(),
        )
    except psycopg2.OperationalError as e:
        raise DestinationUnavailable(f"cannot connect to the destination: {e}") from e


def connect_source(src: cfgmod.Source):
    conn = psycopg2.connect(connect_timeout=30, options=_UTC, **src.connect_kwargs())
    conn.set_session(readonly=True)
    return conn


def acquire_lock(dest_conn, state_schema: str) -> None:
    """A session-level advisory lock on the destination.

    It lives where the writes happen, so it holds across machines, cron entries
    and working directories — a lock file only protects runs that agree on the
    same path. Postgres releases it if the process dies.
    """
    with dest_conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
                    (f"pgmirror:{state_schema}",))
        got = cur.fetchone()[0]
    dest_conn.commit()
    if not got:
        raise LockBusy(f"another pgmirror run is writing to state schema {state_schema!r}")


def _csv(value: str | None) -> set[str]:
    return {v.strip() for v in (value or "").split(",") if v.strip()}


def select(cfg: cfgmod.Config, tables: set[str], sources: set[str], schedule: str):
    unknown = (tables - {t.name for t in cfg.tables}) | (sources - set(cfg.sources))
    if unknown:
        # A typo in --tables must not turn into "ran nothing, exit 0".
        raise cfgmod.ConfigError(f"not in the config: {sorted(unknown)}")
    picked_tables = [t for t in cfg.tables if not tables or t.name in tables]
    if schedule == "hourly":
        excluded = [t.name for t in picked_tables if t.schedule != "hourly"]
        if tables and excluded:
            # Asked for by name and then dropped by the schedule: same trap.
            raise cfgmod.ConfigError(f"--schedule hourly excludes {excluded} (schedule: overnight)")
        picked_tables = [t for t in picked_tables if t.schedule == "hourly"]
    picked_sources = [s for n, s in cfg.sources.items() if not sources or n in sources]
    return picked_tables, picked_sources


# ---------------------------------------------------------------------------

def cmd_run(args, cfg: cfgmod.Config) -> int:
    tables, sources = select(cfg, _csv(args.tables), _csv(args.sources), args.schedule)
    # Every password must be present before anything runs: a missing variable is a
    # config error (exit 2), not a "source unreachable" failure halfway through.
    for target in [cfg.destination, *sources]:
        target.connect_kwargs()
    log(f"pgmirror: {len(sources)} source(s) x {len(tables)} table(s), schedule={args.schedule}"
        + (", DRY RUN" if args.dry_run else ""))

    dest_conn = connect_destination(cfg.destination)
    failures: list[str] = []
    try:
        if not args.no_lock:
            acquire_lock(dest_conn, cfg.destination.state_schema)
        for source in sources:
            try:
                src_conn = connect_source(source)
            except Exception as e:
                # One unreachable source fails its own pairs, never the whole run.
                if not args.dry_run:
                    for t in tables:
                        _record(dest_conn, cfg, source.name, t, "error",
                                f"connect: {type(e).__name__}: {e}")
                failures.append(f"{source.name}/*: cannot connect: {e}")
                log(f"  [{source.name}] cannot connect: {e}")
                continue
            try:
                for table in tables:
                    failures += _run_pair(args, cfg, dest_conn, src_conn, source, table)
            finally:
                src_conn.close()
    finally:
        dest_conn.close()  # also releases the advisory lock

    log(f"finished: {len(failures)} failure(s)")
    for f in failures:
        log(f"  FAIL {f}")
    return EXIT_FAILED if failures else EXIT_OK


def _record(dest_conn, cfg, source_name, table, status, error) -> None:
    try:
        run_id = state.start_run(dest_conn, cfg.destination.state_schema, source_name, table.name, table.mode)
        state.finish_run(dest_conn, cfg.destination.state_schema, run_id, 0, 0, status, error)
    except psycopg2.Error as e:
        raise DestinationUnavailable(f"cannot write the run log: {e}") from e


def _run_pair(args, cfg, dest_conn, src_conn, source, table) -> list[str]:
    schema = cfg.destination.state_schema
    t0 = time.monotonic()
    try:
        run_id = None if args.dry_run else state.start_run(dest_conn, schema, source.name,
                                                           table.name, table.mode)
    except psycopg2.Error as e:
        raise DestinationUnavailable(f"cannot write the run log: {e}") from e
    try:
        if table.mode == "incremental":
            res = sync.sync_incremental(dest_conn, src_conn, cfg.destination, source, table,
                                        dry_run=args.dry_run, overlap_hours=args.overlap_hours, log=log)
        else:
            res = sync.sync_full(dest_conn, src_conn, cfg.destination, source, table,
                                 dry_run=args.dry_run, log=log)
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        try:
            dest_conn.rollback()
        except psycopg2.Error as down:
            # The destination itself is gone: no other pair can be written or logged.
            raise DestinationUnavailable(f"lost the destination during {source.name}/{table.name}: "
                                         f"{down}") from e
        # If the source dropped, its next pair fails on its own.
        with contextlib.suppress(psycopg2.Error):
            src_conn.rollback()
        if run_id is not None:
            state.finish_run(dest_conn, schema, run_id, 0, 0, "error", err)
        log(f"  [{source.name}/{table.name}] ERROR {err}")
        return [f"{source.name}/{table.name}: {err}"]
    if run_id is not None:
        state.finish_run(dest_conn, schema, run_id, res.pulled, res.written, "ok", None)
    log(f"  [{source.name}/{table.name}] done in {time.monotonic() - t0:.1f}s")
    return []


def cmd_init(args, cfg: cfgmod.Config) -> int:
    conn = connect_destination(cfg.destination)
    try:
        state.bootstrap(conn, cfg.destination.state_schema, cfg.destination.schema)
    finally:
        conn.close()
    log(f"schemas {cfg.destination.schema!r} and {cfg.destination.state_schema!r} ready")
    return EXIT_OK


def cmd_ddl(args, cfg: cfgmod.Config) -> int:
    if args.source not in cfg.sources:
        raise cfgmod.ConfigError(f"unknown source {args.source!r}")
    source = cfg.sources[args.source]
    conn = connect_source(source)
    try:
        print(sync.create_table_ddl(conn, source, cfg.destination, args.table))
    finally:
        conn.close()
    return EXIT_OK


def cmd_check(args, cfg: cfgmod.Config) -> int:
    """Connect to every database and run SELECT 1. Touches no data."""
    bad = 0
    targets = [("destination", cfg.destination, connect_destination)]
    targets += [(n, s, connect_source) for n, s in cfg.sources.items()]
    for name, target, connect in targets:
        t0 = time.monotonic()
        try:
            conn = connect(target)
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            conn.close()
            log(f"  [{name}] ok in {time.monotonic() - t0:.2f}s")
        except Exception as e:
            bad += 1
            log(f"  [{name}] FAILED: {type(e).__name__}: {e}")
    return EXIT_FAILED if bad else EXIT_OK


# ---------------------------------------------------------------------------

def _non_negative(value: str) -> int:
    n = int(value)
    if n < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return n


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pgmirror", description="Config-driven incremental Postgres mirror")
    p.add_argument("--config", default=os.environ.get("PGMIRROR_CONFIG", "pgmirror.yaml"))
    sub = p.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="copy the configured tables")
    run.add_argument("--tables", help="comma-separated subset of table names")
    run.add_argument("--sources", help="comma-separated subset of source names")
    run.add_argument("--schedule", choices=["hourly", "all"], default="all",
                     help="hourly skips tables marked `schedule: overnight`")
    run.add_argument("--overlap-hours", type=_non_negative, default=None,
                     help="override every table's overlap_days for this run")
    run.add_argument("--dry-run", action="store_true", help="read and count, write nothing")
    run.add_argument("--no-lock", action="store_true", help="skip the lock (debugging only)")

    sub.add_parser("init", help="create the mirror and state schemas, and the state tables")
    ddl = sub.add_parser("ddl", help="print a CREATE TABLE for a mirror table")
    ddl.add_argument("--source", required=True)
    ddl.add_argument("--table", required=True)
    sub.add_parser("check", help="test every connection")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        cfg = cfgmod.load(Path(args.config))
        command = {"run": cmd_run, "init": cmd_init, "ddl": cmd_ddl, "check": cmd_check}
        return command[args.command](args, cfg)
    except (cfgmod.ConfigError, FileNotFoundError) as e:
        print(f"pgmirror: {e}", file=sys.stderr)
        return EXIT_CONFIG
    except LockBusy as e:
        log(f"ABORT: {e}")
        return EXIT_LOCKED
    except DestinationUnavailable as e:
        log(f"ABORT: {e}")
        return EXIT_DESTINATION


if __name__ == "__main__":
    raise SystemExit(main())

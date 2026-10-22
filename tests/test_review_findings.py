"""One test per defect found in review, each reproducing the situation it caused.

Every test here failed (or could not be written) against the version before the
fix. They are kept apart so the history of why each guard exists stays readable.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
import yaml

from pgmirror.cli import EXIT_CONFIG, EXIT_DESTINATION, EXIT_FAILED, EXIT_OK, main
from pgmirror.config import ConfigError, parse

NOW = datetime.now().replace(microsecond=0)
DAY = timedelta(days=1)


def run(world, *args):
    return main(["--config", world.write_config(), "run", "--no-lock", *args])


def count(world, table):
    return world.sql("warehouse", f"SELECT count(*) FROM raw.{table}")[0][0]


# ---------------------------------------------------------------------------
# A — destination in another time zone must not shift the DELETE window
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("dest_tz", ["<-03>+03", "<+02>-02"])  # POSIX: west of UTC, east of UTC
def test_timestamptz_window_is_the_same_on_both_sides(world, dest_tz):
    world._admin_sql([f"ALTER DATABASE {world.dbs['warehouse']} SET timezone = '{dest_tz}'"])
    for db in ("north", "south"):
        world.sql(db, "CREATE TABLE event (id int, at timestamptz)")
    world.init_warehouse("CREATE TABLE raw.event (id int, at timestamptz, _source text NOT NULL)")
    world.tables = [{"name": "event", "mode": "incremental", "watermark_column": "at",
                     "overlap_days": 0, "initial_watermark": "2020-01-01"}]
    # 11:00 and 12:00 UTC; the watermark ends at 12:00. Row 1 sits inside the 2h
    # band an east-of-UTC destination would wrongly delete.
    world.sql("north", "INSERT INTO event VALUES (1, '2026-05-01 11:00+00'), (2, '2026-05-01 12:00+00')")

    assert run(world) == EXIT_OK
    assert run(world) == EXIT_OK
    # West of UTC the old DELETE started 3h late → row 2 duplicated.
    # East of UTC it started 2h early → row 1 deleted and never re-read.
    assert world.sql("warehouse", "SELECT id FROM raw.event ORDER BY id") == [(1,), (2,)]


# ---------------------------------------------------------------------------
# B — a declared row filter never switches off in silence
# ---------------------------------------------------------------------------

def _stock(world, dest_ddl):
    for db in ("north", "south"):
        world.sql(db, "CREATE TABLE stock (item int, company_id int, qty int)")
        world.sql(db, "INSERT INTO stock VALUES (1, 1, 10), (2, 2, 20)")
    world.init_warehouse(dest_ddl)
    world.source_filters = {"north": {"column": "company_id", "values": [1]},
                            "south": {"column": "company_id", "values": [2]}}
    world.tables = [{"name": "stock", "mode": "full"}]


def test_filter_column_missing_in_the_mirror_fails_the_pair(world):
    _stock(world, "CREATE TABLE raw.stock (item int, qty int, _source text NOT NULL)")
    assert run(world) == EXIT_FAILED
    assert count(world, "stock") == 0  # not the double count


def test_filter_column_missing_in_the_source_is_logged(world, capsys):
    _stock(world, "CREATE TABLE raw.stock (item int, company_id int, qty int, _source text NOT NULL)")
    world.source_filters["north"] = {"column": "compnay_id", "values": [1]}  # typo
    assert run(world) == EXIT_OK
    assert "row_filter on 'compnay_id' not applied" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Atomicity — a failure after the DELETE leaves the mirror and watermark intact
# ---------------------------------------------------------------------------

def test_failure_mid_insert_rolls_back_the_delete(world):
    for db in ("north", "south"):
        world.sql(db, "CREATE TABLE sale (id int, amount int, issued_at timestamp)")
    world.init_warehouse("CREATE TABLE raw.sale (id int, amount smallint, issued_at timestamp, "
                         "_source text NOT NULL)")
    world.tables = [{"name": "sale", "mode": "incremental", "watermark_column": "issued_at",
                     "overlap_days": 3, "initial_watermark": (NOW - 30 * DAY).isoformat()}]
    world.sql("north", "INSERT INTO sale VALUES (1, 10, %s), (2, 20, %s)", (NOW - DAY, NOW - DAY))
    assert run(world) == EXIT_OK
    wm_before = world.sql("warehouse", "SELECT last_watermark FROM etl.etl_state WHERE source='north'")

    # A value the mirror's column cannot hold arrives inside the window.
    world.sql("north", "INSERT INTO sale VALUES (3, 100000, %s)", (NOW,))
    assert run(world) == EXIT_FAILED
    assert world.sql("warehouse", "SELECT id FROM raw.sale ORDER BY id") == [(1,), (2,)]
    wm_after = world.sql("warehouse", "SELECT last_watermark FROM etl.etl_state WHERE source='north'")
    assert wm_after == wm_before


# ---------------------------------------------------------------------------
# G — dry run on a populated mirror changes nothing at all
# ---------------------------------------------------------------------------

def test_dry_run_on_a_populated_mirror(world):
    for db in ("north", "south"):
        world.sql(db, "CREATE TABLE item (id int)")
    world.sql("north", "INSERT INTO item VALUES (1), (2)")
    world.init_warehouse("CREATE TABLE raw.item (id int, _source text NOT NULL)")
    world.tables = [{"name": "item", "mode": "full"}]
    assert run(world) == EXIT_OK
    log_rows = world.sql("warehouse", "SELECT count(*) FROM etl.etl_run_log")[0][0]

    world.sql("north", "DELETE FROM item")
    assert run(world, "--dry-run") == EXIT_OK
    assert count(world, "item") == 2
    assert world.sql("warehouse", "SELECT count(*) FROM etl.etl_run_log")[0][0] == log_rows


# ---------------------------------------------------------------------------
# E — destination down is its own exit code
# ---------------------------------------------------------------------------

def test_unreachable_destination_exits_4(world):
    world.init_warehouse()
    world.tables = [{"name": "item", "mode": "full"}]
    cfg = world.config_dict()
    cfg["destination"]["dbname"] = "no_such_warehouse"
    world.config_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    assert main(["--config", str(world.config_path), "run", "--no-lock"]) == EXIT_DESTINATION


# ---------------------------------------------------------------------------
# K — only _source and _synced_at are reserved
# ---------------------------------------------------------------------------

def test_source_column_starting_with_underscore_is_copied(world):
    for db in ("north", "south"):
        world.sql(db, "CREATE TABLE item (id int, _legacy_code text)")
    world.sql("north", "INSERT INTO item VALUES (1, 'X-1')")
    world.init_warehouse("CREATE TABLE raw.item (id int, _legacy_code text, _source text NOT NULL)")
    world.tables = [{"name": "item", "mode": "full"}]
    assert run(world) == EXIT_OK
    assert world.sql("warehouse", "SELECT _legacy_code FROM raw.item") == [("X-1",)]


# ---------------------------------------------------------------------------
# Guards that had no test producing the forbidden input
# ---------------------------------------------------------------------------

def _one_table(world, src_ddl, dst_ddl, table):
    for db in ("north", "south"):
        world.sql(db, src_ddl)
    world.init_warehouse(*([dst_ddl] if dst_ddl else []))
    world.tables = [table]


@pytest.mark.parametrize("src_ddl, dst_ddl, table, message", [
    ("CREATE TABLE t (id int)", "CREATE TABLE raw.t (id int)",
     {"name": "t", "mode": "full"}, "no _source column"),
    ("CREATE TABLE t (id int)", None,
     {"name": "t", "mode": "full"}, "destination table does not exist"),
    ("CREATE TABLE t (id int)", "CREATE TABLE raw.t (id int, _source text)",
     {"name": "t", "mode": "incremental", "watermark_column": "at", "initial_watermark": "2020-01-01"},
     "watermark column 'at' is missing"),
    ("CREATE TABLE t (id int, at timestamp)", "CREATE TABLE raw.t (id int, at timestamp, _source text)",
     {"name": "t", "mode": "incremental", "watermark_column": "at"},
     "no stored watermark and no initial_watermark"),
    ("CREATE TABLE t (id int, at int)", "CREATE TABLE raw.t (id int, at int, _source text)",
     {"name": "t", "mode": "incremental", "watermark_column": "at", "initial_watermark": "2020-01-01"},
     "expected date or timestamp"),
])
def test_pair_guards(world, capsys, src_ddl, dst_ddl, table, message):
    _one_table(world, src_ddl, dst_ddl, table)
    if "at int" in src_ddl:
        world.sql("north", "INSERT INTO t VALUES (1, 5)")
    assert run(world, "--sources", "north") == EXIT_FAILED
    assert message in capsys.readouterr().out


def test_unknown_source_in_subset_is_refused(world):
    world.init_warehouse()
    world.tables = [{"name": "t", "mode": "full"}]
    assert run(world, "--sources", "nort") == EXIT_CONFIG


def test_table_named_but_excluded_by_schedule_is_refused(world):
    world.init_warehouse()
    world.tables = [{"name": "stock", "mode": "full", "schedule": "overnight"}]
    assert run(world, "--schedule", "hourly", "--tables", "stock") == EXIT_CONFIG


def test_negative_overlap_hours_is_refused(world):
    world.init_warehouse()
    world.tables = [{"name": "t", "mode": "full"}]
    with pytest.raises(SystemExit) as e:
        run(world, "--overlap-hours", "-5")
    assert e.value.code == 2


# ---------------------------------------------------------------------------
# Config-only (J and statement_timeout)
# ---------------------------------------------------------------------------

BASE = {
    "destination": {"host": "h", "dbname": "wh", "user": "u", "password_env": "P"},
    "sources": {"north": {"host": "h", "dbname": "a", "user": "u", "password_env": "P"}},
    "tables": [{"name": "sale", "mode": "incremental", "watermark_column": "at",
                "initial_watermark": "2024-01-01T00:00:00-03:00"}],
}


def test_initial_watermark_with_offset_becomes_naive_utc():
    assert parse(BASE).tables[0].initial_watermark == datetime(2024, 1, 1, 3, 0)


@pytest.mark.parametrize("bad", ["30s", 0, -1, True])
def test_statement_timeout_must_be_a_positive_integer(bad):
    cfg = {**BASE, "destination": {**BASE["destination"], "statement_timeout_ms": bad}}
    with pytest.raises(ConfigError, match="positive integer"):
        parse(cfg)

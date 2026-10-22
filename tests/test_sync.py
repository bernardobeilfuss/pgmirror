"""End to end, through the command line, against real databases.

Each test is a situation that happens in production: a sale cancelled after
it was copied, a typo dated 2099, a column added on one side only, one source
down while the others are fine.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pgmirror.cli import EXIT_CONFIG, EXIT_FAILED, EXIT_LOCKED, EXIT_OK, main

SALE_SRC = """CREATE TABLE sale (id int PRIMARY KEY, company_id int, amount numeric,
                                  issued_at timestamp, note text)"""
SALE_DST = """CREATE TABLE raw.sale (id int, company_id int, amount numeric, issued_at timestamp,
                                      _source text NOT NULL, _synced_at timestamptz)"""
STOCK_SRC = "CREATE TABLE stock (item int, company_id int, qty int)"
STOCK_DST = "CREATE TABLE raw.stock (item int, company_id int, qty int, _source text NOT NULL)"

NOW = datetime.now().replace(microsecond=0)
DAY = timedelta(days=1)


def run(world, *args):
    return main(["--config", world.write_config(), "run", "--no-lock", *args])


def mirror(world, table, order="_source, 1"):
    return world.sql("warehouse", f"SELECT * FROM raw.{table} ORDER BY {order}")


@pytest.fixture
def sales(world):
    for db in ("north", "south"):
        world.sql(db, SALE_SRC)
    world.init_warehouse(SALE_DST)
    world.tables = [{"name": "sale", "mode": "incremental", "watermark_column": "issued_at",
                     "overlap_days": 3, "initial_watermark": (NOW - 30 * DAY).isoformat()}]
    return world


# ---------------------------------------------------------------------------
# Full copy and the row filter — the "3.5x stock" case
# ---------------------------------------------------------------------------

def test_full_copy_with_row_filter_counts_each_store_once(world):
    # Each database also stores a copy of the other company's stock. Summed
    # naively the warehouse would count every store twice.
    for db in ("north", "south"):
        world.sql(db, STOCK_SRC)
        world.sql(db, "INSERT INTO stock VALUES (1, 1, 10), (2, 2, 20)")
    world.init_warehouse(STOCK_DST)
    world.source_filters = {"north": {"column": "company_id", "values": [1]},
                            "south": {"column": "company_id", "values": [2]}}
    world.tables = [{"name": "stock", "mode": "full"}]

    assert run(world) == EXIT_OK
    assert world.sql("warehouse", "SELECT sum(qty) FROM raw.stock")[0][0] == 30
    assert mirror(world, "stock") == [(1, 1, 10, "north"), (2, 2, 20, "south")]

    # Running again replaces, never appends.
    world.sql("north", "UPDATE stock SET qty = 11 WHERE item = 1")
    assert run(world) == EXIT_OK
    assert world.sql("warehouse", "SELECT sum(qty) FROM raw.stock")[0][0] == 31


def test_unfiltered_full_copy_shows_the_double_count(world):
    """The same data without the filter: this is the bug the filter exists for."""
    for db in ("north", "south"):
        world.sql(db, STOCK_SRC)
        world.sql(db, "INSERT INTO stock VALUES (1, 1, 10), (2, 2, 20)")
    world.init_warehouse(STOCK_DST)
    world.tables = [{"name": "stock", "mode": "full"}]
    assert run(world) == EXIT_OK
    assert world.sql("warehouse", "SELECT sum(qty) FROM raw.stock")[0][0] == 60


# ---------------------------------------------------------------------------
# Incremental
# ---------------------------------------------------------------------------

def test_incremental_picks_up_corrections_inside_the_overlap(sales):
    w = sales
    w.sql("north", "INSERT INTO sale VALUES (1, 1, 100, %s, 'a'), (2, 1, 50, %s, 'b')",
          (NOW - 10 * DAY, NOW - 1 * DAY))
    assert run(w) == EXIT_OK
    assert [r[0] for r in mirror(w, "sale")] == [1, 2]

    # Sale 2 is cancelled the next day (deleted at the source), a new sale 3
    # comes in, and sale 1 — outside the 3-day overlap — is edited too.
    w.sql("north", "DELETE FROM sale WHERE id = 2")
    w.sql("north", "INSERT INTO sale VALUES (3, 1, 70, %s, 'c')", (NOW,))
    w.sql("north", "UPDATE sale SET amount = 999 WHERE id = 1")
    assert run(w) == EXIT_OK

    rows = {r[0]: r for r in mirror(w, "sale")}
    assert set(rows) == {1, 3}               # the cancellation reached the mirror
    assert rows[1][2] == 100                 # outside the window: documented limitation


def test_watermark_never_runs_into_the_future(sales):
    """A row dated 2099 must not freeze every later run (it did, in production)."""
    w = sales
    w.sql("north", "INSERT INTO sale VALUES (1, 1, 1, '2099-01-01', 'typo')")
    assert run(w) == EXIT_OK
    wm = w.sql("warehouse", "SELECT last_watermark FROM etl.etl_state WHERE source = 'north'")[0][0]
    assert wm <= datetime.now() + DAY

    w.sql("north", "INSERT INTO sale VALUES (2, 1, 2, %s, 'after')", (NOW,))
    assert run(w) == EXIT_OK
    assert sorted(r[0] for r in mirror(w, "sale")) == [1, 2]


def test_rows_outside_the_filter_are_purged_as_the_window_passes(sales):
    w = sales
    w.sql("north", "INSERT INTO sale VALUES (1, 1, 10, %s, ''), (2, 9, 10, %s, '')", (NOW, NOW))
    assert run(w) == EXIT_OK
    assert len(mirror(w, "sale")) == 2

    # Company 9 turns out to be leftover data from an old migration.
    w.source_filters["north"] = {"column": "company_id", "values": [1]}
    assert run(w) == EXIT_OK
    assert [r[0] for r in mirror(w, "sale")] == [1]


def test_timestamptz_watermark(world):
    for db in ("north", "south"):
        world.sql(db, "CREATE TABLE event (id int, at timestamptz)")
    world.init_warehouse("CREATE TABLE raw.event (id int, at timestamptz, _source text NOT NULL)")
    world.tables = [{"name": "event", "mode": "incremental", "watermark_column": "at",
                     "overlap_days": 0, "initial_watermark": "2020-01-01"}]
    world.sql("north", "INSERT INTO event VALUES (1, '2026-05-01 10:00-03')")
    assert run(world) == EXIT_OK
    wm = world.sql("warehouse", "SELECT last_watermark FROM etl.etl_state WHERE source = 'north'")[0][0]
    assert wm == datetime(2026, 5, 1, 13, 0)  # stored in UTC

    world.sql("north", "INSERT INTO event VALUES (2, '2026-05-01 10:00:01-03')")
    assert run(world) == EXIT_OK
    assert [r[0] for r in mirror(world, "event")] == [1, 2]


def test_schema_drift_is_tolerated(sales):
    w = sales
    # The destination wants a column the source does not have yet; the source
    # has `note`, which the mirror does not keep.
    w.sql("warehouse", "ALTER TABLE raw.sale ADD COLUMN discount numeric")
    w.sql("north", "INSERT INTO sale VALUES (1, 1, 10, %s, 'ignored')", (NOW,))
    assert run(w) == EXIT_OK
    assert w.sql("warehouse", "SELECT id, discount FROM raw.sale") == [(1, None)]


def test_dry_run_writes_nothing(sales):
    w = sales
    w.sql("north", "INSERT INTO sale VALUES (1, 1, 10, %s, '')", (NOW,))
    assert run(w, "--dry-run") == EXIT_OK
    assert mirror(w, "sale") == []
    assert w.sql("warehouse", "SELECT count(*) FROM etl.etl_state")[0][0] == 0


# ---------------------------------------------------------------------------
# Failure: one pair fails, the rest runs, and the failed pair is untouched
# ---------------------------------------------------------------------------

def test_failure_is_isolated_and_atomic(sales):
    w = sales
    w.sql("north", "INSERT INTO sale VALUES (1, 1, 10, %s, '')", (NOW,))
    w.sql("south", "INSERT INTO sale VALUES (5, 2, 10, %s, '')", (NOW,))
    assert run(w) == EXIT_OK

    # South's table is renamed by mistake; north keeps working.
    w.sql("south", "ALTER TABLE sale RENAME TO sale_old")
    w.sql("north", "INSERT INTO sale VALUES (2, 1, 10, %s, '')", (NOW,))
    assert run(w) == EXIT_FAILED

    rows = mirror(w, "sale")
    assert [(r[0], r[4]) for r in rows] == [(1, "north"), (2, "north"), (5, "south")]
    log = w.sql("warehouse", "SELECT source, status, error FROM etl.etl_run_log "
                             "WHERE status = 'error'")
    assert len(log) == 1 and log[0][0] == "south" and "source table does not exist" in log[0][2]


def test_unreachable_source_fails_its_pairs_only(sales):
    w = sales
    w.sql("north", "INSERT INTO sale VALUES (1, 1, 10, %s, '')", (NOW,))
    cfg = w.config_dict()
    w.write_config()
    import yaml
    cfg["sources"]["south"]["dbname"] = "does_not_exist"
    w.config_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    assert main(["--config", str(w.config_path), "run", "--no-lock"]) == EXIT_FAILED
    assert [r[0] for r in mirror(w, "sale")] == [1]


# ---------------------------------------------------------------------------
# Command line refusals
# ---------------------------------------------------------------------------

def test_unknown_table_in_subset_is_refused(sales):
    assert run(sales, "--tables", "sael") == EXIT_CONFIG


def test_missing_password_is_refused_before_anything_runs(sales, monkeypatch):
    path = sales.write_config()
    monkeypatch.delenv("PGMIRROR_TEST_PASSWORD")
    assert main(["--config", path, "run", "--no-lock"]) == EXIT_CONFIG
    assert sales.sql("warehouse", "SELECT count(*) FROM etl.etl_run_log")[0][0] == 0


def test_second_run_is_refused_while_the_first_holds_the_lock(sales):
    # The lock lives in the destination, so it holds across machines and cron entries.
    holder = sales.conn("warehouse")
    with holder.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(hashtextextended('pgmirror:etl', 0))")
    try:
        assert main(["--config", sales.write_config(), "run"]) == EXIT_LOCKED
    finally:
        holder.close()
    assert main(["--config", sales.write_config(), "run"]) == EXIT_OK  # released → runs


def test_ddl_output_creates_a_working_mirror(world, capsys):
    for db in ("north", "south"):
        world.sql(db, "CREATE TABLE item (id int, name varchar(40), price numeric(10,2))")
    world.sql("north", "INSERT INTO item VALUES (1, 'shoe', 99.90)")
    world.tables = [{"name": "item", "mode": "full"}]
    path = world.write_config()
    assert main(["--config", path, "ddl", "--source", "north", "--table", "item"]) == EXIT_OK
    world.sql("warehouse", capsys.readouterr().out)
    assert main(["--config", path, "init"]) == EXIT_OK
    assert run(world) == EXIT_OK
    assert world.sql("warehouse", "SELECT id, name, price::text, _source FROM raw.item") == [
        (1, "shoe", "99.90", "north")]

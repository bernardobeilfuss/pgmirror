"""A real Postgres for the tests, never a mock.

Set PGMIRROR_TEST_DSN to point at a server you own (CI does); otherwise an
embedded server is started with `pgserver`. Each test gets three fresh
databases — two sources and a warehouse — and drops them afterwards.
"""

from __future__ import annotations

import itertools
import os
import tempfile
from urllib.parse import urlparse

import psycopg2
import pytest
import yaml

from pgmirror import config as cfgmod
from pgmirror import state

_counter = itertools.count()


@pytest.fixture(scope="session")
def server_dsn():
    dsn = os.environ.get("PGMIRROR_TEST_DSN")
    if dsn:
        yield dsn
        return
    pgserver = pytest.importorskip("pgserver")
    srv = pgserver.get_server(tempfile.mkdtemp(prefix="pgmirror-test-"), cleanup_mode="stop")
    yield srv.get_uri()


class World:
    """Two source databases and a warehouse, plus a config that points at them."""

    def __init__(self, dsn: str, tmp_path):
        u = urlparse(dsn)
        self.dsn = dsn
        self.base = dict(host=u.hostname or "localhost", port=u.port or 5432,
                         user=u.username or "postgres")
        self.password = u.password or "unused"
        n = f"{os.getpid()}_{next(_counter)}"
        self.dbs = {"north": f"pgm_north_{n}", "south": f"pgm_south_{n}", "warehouse": f"pgm_wh_{n}"}
        self._admin_sql([f"DROP DATABASE IF EXISTS {db}" for db in self.dbs.values()]
                        + [f"CREATE DATABASE {db}" for db in self.dbs.values()])
        os.environ["PGMIRROR_TEST_PASSWORD"] = self.password
        self.config_path = tmp_path / "pgmirror.yaml"
        self.tables: list[dict] = []
        self.source_filters: dict[str, dict | None] = {"north": None, "south": None}

    def conn(self, name: str):
        return psycopg2.connect(dbname=self.dbs[name], password=self.password, **self.base)

    def sql(self, name: str, statement: str, params=None):
        with self.conn(name) as c, c.cursor() as cur:
            cur.execute(statement, params)
            rows = cur.fetchall() if cur.description else None
        c.close()
        return rows

    def config_dict(self) -> dict:
        def conn(db):
            return dict(host=self.base["host"], port=self.base["port"], dbname=self.dbs[db],
                        user=self.base["user"], password_env="PGMIRROR_TEST_PASSWORD")
        sources = {}
        for name in ("north", "south"):
            s = conn(name)
            if self.source_filters[name] is not None:
                s["row_filter"] = self.source_filters[name]
            sources[name] = s
        return {"destination": {**conn("warehouse"), "schema": "raw", "state_schema": "etl"},
                "sources": sources, "tables": self.tables}

    def write_config(self) -> str:
        self.config_path.write_text(yaml.safe_dump(self.config_dict()), encoding="utf-8")
        return str(self.config_path)

    def config(self) -> cfgmod.Config:
        return cfgmod.parse(self.config_dict())

    def init_warehouse(self, *ddl: str):
        c = self.conn("warehouse")
        state.bootstrap(c, "etl", "raw")
        c.close()
        for statement in ddl:
            self.sql("warehouse", statement)

    def drop(self):
        self._admin_sql([f"DROP DATABASE IF EXISTS {db} WITH (FORCE)" for db in self.dbs.values()])

    def _admin_sql(self, statements: list[str]):
        # Not `with conn:` — psycopg2 opens a transaction block there even in
        # autocommit, and CREATE/DROP DATABASE refuse to run inside one.
        conn = psycopg2.connect(self.dsn)
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                for statement in statements:
                    cur.execute(statement)
        finally:
            conn.close()


@pytest.fixture
def world(server_dsn, tmp_path):
    w = World(server_dsn, tmp_path)
    yield w
    w.drop()

"""Every rule of the config validator is shown refusing the input it forbids."""

from __future__ import annotations

import copy

import pytest

from pgmirror.config import ConfigError, parse

BASE = {
    "destination": {"host": "h", "dbname": "wh", "user": "u", "password_env": "P"},
    "sources": {"north": {"host": "h", "dbname": "a", "user": "u", "password_env": "P"}},
    "tables": [{"name": "sale", "mode": "incremental", "watermark_column": "issued_at",
                "initial_watermark": "2024-01-01"}],
}


def with_(path, value):
    cfg = copy.deepcopy(BASE)
    node = cfg
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    return cfg


def test_base_is_valid():
    cfg = parse(BASE)
    assert cfg.tables[0].overlap_days == 7
    assert cfg.destination.schema == "raw"


@pytest.mark.parametrize("cfg, message", [
    (with_(["destination", "passwrod_env"], "P"), "unknown key"),
    (with_(["tables", 0, "mode"], "incremetal"), "is not one of"),
    (with_(["tables", 0, "schedule"], "nightly"), "is not one of"),
    (with_(["tables", 0, "name"], "sale; DROP TABLE x"), "not a plain SQL identifier"),
    (with_(["tables", 0, "watermark_column"], "issued_at)--"), "not a plain SQL identifier"),
    (with_(["destination", "schema"], 'raw"'), "not a plain SQL identifier"),
    (with_(["tables", 0, "overlap_days"], -1), "overlap_days"),
    (with_(["tables", 0, "initial_watermark"], "yesterday"), "not an ISO date"),
    (with_(["tables", 0, "row_filter_by_source"], {"west": None}), "unknown source"),
    (with_(["sources", "north", "row_filter"], {"column": "company_id", "values": []}), "non-empty"),
    (with_(["tables"], []), "non-empty list"),
])
def test_refuses(cfg, message):
    with pytest.raises(ConfigError, match=message):
        parse(cfg)


def test_incremental_needs_a_watermark_column():
    cfg = copy.deepcopy(BASE)
    del cfg["tables"][0]["watermark_column"]
    with pytest.raises(ConfigError, match="needs watermark_column"):
        parse(cfg)


def test_watermark_settings_on_a_full_table_are_a_mistake():
    cfg = with_(["tables", 0], {"name": "stock", "mode": "full", "overlap_days": 3})
    with pytest.raises(ConfigError, match="only apply to incremental"):
        parse(cfg)


def test_same_table_twice_is_refused():
    cfg = copy.deepcopy(BASE)
    cfg["tables"].append(copy.deepcopy(cfg["tables"][0]))
    with pytest.raises(ConfigError, match="declared twice"):
        parse(cfg)


def test_row_filter_precedence():
    cfg = copy.deepcopy(BASE)
    cfg["sources"]["north"]["row_filter"] = {"column": "company_id", "values": [1]}
    cfg["sources"]["south"] = {**cfg["sources"]["north"], "row_filter": {"column": "company_id",
                                                                         "values": [2]}}
    cfg["tables"] = [
        {"name": "inherit", "mode": "full"},
        {"name": "off_everywhere", "mode": "full", "row_filter": None},
        {"name": "off_on_north", "mode": "full", "row_filter_by_source": {"north": None}},
    ]
    c = parse(cfg)
    north, south = c.sources["north"], c.sources["south"]
    inherit, off, off_north = c.tables
    assert inherit.filter_for(north).values == (1,)
    assert off.filter_for(north) is None and off.filter_for(south) is None
    assert off_north.filter_for(north) is None
    assert off_north.filter_for(south).values == (2,)


def test_missing_password_variable_is_a_config_error(monkeypatch):
    monkeypatch.delenv("P", raising=False)
    with pytest.raises(ConfigError, match="P is not set"):
        parse(BASE).destination.connect_kwargs()

"""Load and validate the YAML config.

Validation fails closed: an unknown key, a misspelled mode or an identifier
that is not a plain SQL name stops the run before any connection is opened.
A typo in a config that drives DELETE statements should never be "probably
fine".
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

MODES = ("incremental", "full")
SCHEDULES = ("hourly", "overnight")

# Plain identifiers only. Everything is also quoted with psycopg2.sql.Identifier,
# so this is a second lock, not the only one: a config value never becomes SQL.
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ConfigError(ValueError):
    """The config cannot be trusted to drive a run."""


@dataclass(frozen=True)
class RowFilter:
    column: str
    values: tuple[Any, ...]


@dataclass(frozen=True)
class Connection:
    host: str
    port: int
    dbname: str
    user: str
    password_env: str

    def connect_kwargs(self) -> dict[str, Any]:
        password = os.environ.get(self.password_env)
        if not password:
            raise ConfigError(f"environment variable {self.password_env} is not set")
        return dict(host=self.host, port=self.port, dbname=self.dbname,
                    user=self.user, password=password)


@dataclass(frozen=True)
class Destination(Connection):
    schema: str = "raw"
    state_schema: str = "etl"
    statement_timeout_ms: int = 1_800_000


@dataclass(frozen=True)
class Source(Connection):
    name: str = ""
    schema: str = "public"
    row_filter: RowFilter | None = None


@dataclass(frozen=True)
class Table:
    name: str
    mode: str
    schedule: str = "hourly"
    watermark_column: str | None = None
    overlap_days: int = 7
    initial_watermark: datetime | None = None
    # Sentinel-free override: `inherit` means "use the source's filter".
    row_filter_inherit: bool = True
    row_filter: RowFilter | None = None
    row_filter_by_source: dict[str, RowFilter | None] = field(default_factory=dict)

    def filter_for(self, source: Source) -> RowFilter | None:
        if source.name in self.row_filter_by_source:
            return self.row_filter_by_source[source.name]
        if not self.row_filter_inherit:
            return self.row_filter
        return source.row_filter


@dataclass(frozen=True)
class Config:
    destination: Destination
    sources: dict[str, Source]
    tables: list[Table]


# ---------------------------------------------------------------------------

def _ident(value: Any, where: str) -> str:
    if not isinstance(value, str) or not _IDENT.match(value):
        raise ConfigError(f"{where}: {value!r} is not a plain SQL identifier")
    return value


def _only_keys(d: Any, allowed: set[str], required: set[str], where: str) -> dict:
    if not isinstance(d, dict):
        raise ConfigError(f"{where}: expected a mapping, got {type(d).__name__}")
    unknown = set(d) - allowed
    if unknown:
        raise ConfigError(f"{where}: unknown key(s) {sorted(unknown)}")
    missing = required - set(d)
    if missing:
        raise ConfigError(f"{where}: missing key(s) {sorted(missing)}")
    return d


def _row_filter(d: Any, where: str) -> RowFilter | None:
    if d is None:
        return None
    _only_keys(d, {"column", "values"}, {"column", "values"}, where)
    values = d["values"]
    if not isinstance(values, list) or not values:
        raise ConfigError(f"{where}.values: expected a non-empty list")
    return RowFilter(column=_ident(d["column"], f"{where}.column"), values=tuple(values))


_CONN_KEYS = {"host", "port", "dbname", "user", "password_env"}


def _connection_fields(d: dict, where: str) -> dict[str, Any]:
    port = d.get("port", 5432)
    if not isinstance(port, int):
        raise ConfigError(f"{where}.port: expected an integer")
    return dict(host=str(d["host"]), port=port, dbname=str(d["dbname"]),
                user=str(d["user"]), password_env=str(d["password_env"]))


def _positive_int(value: Any, where: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigError(f"{where}: expected a positive integer, got {value!r}")
    return value


def _parse_watermark(value: Any, where: str) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError as e:
        raise ConfigError(f"{where}: {value!r} is not an ISO date") from e
    # Watermarks are naive; an offset given here is honoured by moving to UTC.
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def parse(raw: Any) -> Config:
    top = _only_keys(raw, {"destination", "sources", "tables"},
                     {"destination", "sources", "tables"}, "config")

    d = _only_keys(top["destination"],
                   _CONN_KEYS | {"schema", "state_schema", "statement_timeout_ms"},
                   {"host", "dbname", "user", "password_env"}, "destination")
    destination = Destination(
        **_connection_fields(d, "destination"),
        schema=_ident(d.get("schema", "raw"), "destination.schema"),
        state_schema=_ident(d.get("state_schema", "etl"), "destination.state_schema"),
        statement_timeout_ms=_positive_int(d.get("statement_timeout_ms", 1_800_000),
                                           "destination.statement_timeout_ms"),
    )

    if not isinstance(top["sources"], dict) or not top["sources"]:
        raise ConfigError("sources: expected a non-empty mapping")
    sources: dict[str, Source] = {}
    for name, s in top["sources"].items():
        where = f"sources.{name}"
        _ident(name, where)
        _only_keys(s, _CONN_KEYS | {"schema", "row_filter"},
                   {"host", "dbname", "user", "password_env"}, where)
        sources[name] = Source(
            **_connection_fields(s, where),
            name=name,
            schema=_ident(s.get("schema", "public"), f"{where}.schema"),
            row_filter=_row_filter(s.get("row_filter"), f"{where}.row_filter"),
        )

    if not isinstance(top["tables"], list) or not top["tables"]:
        raise ConfigError("tables: expected a non-empty list")
    tables: list[Table] = []
    seen: set[str] = set()
    for i, t in enumerate(top["tables"]):
        where = f"tables[{i}]"
        _only_keys(t, {"name", "mode", "schedule", "watermark_column", "overlap_days",
                       "initial_watermark", "row_filter", "row_filter_by_source"},
                   {"name", "mode"}, where)
        name = _ident(t["name"], f"{where}.name")
        where = f"tables.{name}"
        if name in seen:
            raise ConfigError(f"{where}: declared twice")
        seen.add(name)

        mode = t["mode"]
        if mode not in MODES:
            raise ConfigError(f"{where}.mode: {mode!r} is not one of {MODES}")
        schedule = t.get("schedule", "hourly")
        if schedule not in SCHEDULES:
            raise ConfigError(f"{where}.schedule: {schedule!r} is not one of {SCHEDULES}")

        wm_col = t.get("watermark_column")
        if mode == "incremental":
            if wm_col is None:
                raise ConfigError(f"{where}: incremental mode needs watermark_column")
            wm_col = _ident(wm_col, f"{where}.watermark_column")
        elif wm_col is not None or "overlap_days" in t or "initial_watermark" in t:
            raise ConfigError(f"{where}: watermark settings only apply to incremental mode")

        overlap = t.get("overlap_days", 7)
        if not isinstance(overlap, int) or isinstance(overlap, bool) or overlap < 0:
            raise ConfigError(f"{where}.overlap_days: expected an integer >= 0")

        by_source_raw = t.get("row_filter_by_source") or {}
        if not isinstance(by_source_raw, dict):
            raise ConfigError(f"{where}.row_filter_by_source: expected a mapping")
        by_source: dict[str, RowFilter | None] = {}
        for src_name, f in by_source_raw.items():
            if src_name not in sources:
                raise ConfigError(f"{where}.row_filter_by_source: unknown source {src_name!r}")
            by_source[src_name] = _row_filter(f, f"{where}.row_filter_by_source.{src_name}")

        tables.append(Table(
            name=name, mode=mode, schedule=schedule, watermark_column=wm_col,
            overlap_days=overlap,
            initial_watermark=_parse_watermark(t.get("initial_watermark"),
                                               f"{where}.initial_watermark"),
            row_filter_inherit="row_filter" not in t,
            row_filter=_row_filter(t.get("row_filter"), f"{where}.row_filter"),
            row_filter_by_source=by_source,
        ))

    return Config(destination=destination, sources=sources, tables=tables)


def load(path: str | Path) -> Config:
    with Path(path).open("r", encoding="utf-8") as f:
        return parse(yaml.safe_load(f))

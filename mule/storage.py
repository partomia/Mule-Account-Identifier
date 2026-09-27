"""Read gold features and write the daily outputs, on CDW Impala or local parquet.

  impala   CDW Impala virtual warehouse over HTTPS (impyla). The run first
           REFRESHes gold (CDE commits Iceberg snapshots Impala has not seen
           yet), then pins every feature read to one Iceberg snapshot
           (FOR SYSTEM_VERSION AS OF), so a run reads one consistent version
           even if a CDE load commits mid-run, and the endpoint can rebuild
           the exact same context later. Outputs are Iceberg v2 tables; a
           rerun for a run_date does DELETE + INSERT for that date.
  parquet  one file per table under storage.parquet_dir, for a laptop, Docker,
           CI or an offline demo. Same queries and replace-by-run_date semantics.

Both backends pick context non-mules and holdout non-mules with a hash of
(account_id, snapshot_date), so a selection is reproducible from its window
and counts alone. The hash differs between the backends (Impala fnv_hash vs
pandas), which only matters if you compare a parquet run with an Impala run.
"""

from __future__ import annotations

import logging
import math
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from mule.config import ROOT, settings, table
from mule.features import COLUMNS, LABEL
from mule.schema import INVESTIGATOR_DECISIONS, OUTPUT_TABLES

logger = logging.getLogger(__name__)

TABLE_COLUMNS = {**OUTPUT_TABLES, "investigator_decisions": INVESTIGATOR_DECISIONS}
PARTITION = {"mule_alerts": "run_date", "mule_rings": "run_date", "mule_holdout": "run_date",
             "mule_model_run": "run_date", "investigator_decisions": "month(decided_at)"}
HASH_BUCKETS = 10_000


def get_storage(backend: str | None = None):
    backend = backend or settings()["storage"]["backend"]
    if backend == "impala":
        return ImpalaStorage(settings()["impala"])
    if backend == "parquet":
        return ParquetStorage(Path(settings()["storage"]["parquet_dir"]))
    raise ValueError(f"unknown storage backend {backend!r} (impala | parquet)")


def _cast(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    df = df.copy()
    for col, typ in columns:
        if col not in df.columns:
            continue
        if typ == "DATE":
            df[col] = pd.to_datetime(df[col]).dt.date
        elif typ == "TIMESTAMP":
            df[col] = pd.to_datetime(df[col])
    return df


def _normalise_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=COLUMNS)
    df = df.copy()
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    for c in COLUMNS[len(COLUMNS) - 19:]:          # the 18 features and the label
        df[c] = pd.to_numeric(df[c])
    for c in ("account_id", "cif", "branch_code", "product_code", "person_cluster_id", "ring_id"):
        df[c] = df[c].astype(str)
    return df.reset_index(drop=True)


class ParquetStorage:
    name = "parquet"

    def __init__(self, directory: Path):
        self.dir = directory if directory.is_absolute() else ROOT / directory
        self._features = None

    def _path(self, key: str) -> Path:
        return self.dir / f"{table(key).split('.')[-1]}.parquet"

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def refresh(self, key: str) -> None:
        if key == "mule_features":
            self._features = None

    def read(self, key: str, where_run_date: date | None = None) -> pd.DataFrame:
        path = self._path(key)
        if not path.exists():
            raise FileNotFoundError(f"{path} not found - run scripts/run_cde_local.py all, or copy an export")
        df = pd.read_parquet(path)
        if where_run_date is not None and "run_date" in df.columns:
            df = df[pd.to_datetime(df["run_date"]).dt.date == where_run_date]
        return df.reset_index(drop=True)

    def _all(self) -> pd.DataFrame:
        if self._features is None:
            df = _normalise_features(self.read("mule_features")[COLUMNS])
            key = df["account_id"] + "|" + df["snapshot_date"].astype(str)
            df["_bucket"] = (pd.util.hash_array(key.to_numpy()) % HASH_BUCKETS).astype(int)
            self._features = df
        return self._features

    def snapshot_id(self, key: str) -> str | None:
        return None

    def labelled_dates(self, snapshot_id: str | None = None) -> list[date]:
        df = self._all()
        return sorted(df.loc[df[LABEL].notna(), "snapshot_date"].unique())

    def latest_snapshot_date(self, snapshot_id: str | None = None) -> date:
        return self._all()["snapshot_date"].max()

    def _window(self, date_from, date_to) -> pd.DataFrame:
        df = self._all()
        return df[(df["snapshot_date"] >= date_from) & (df["snapshot_date"] <= date_to)]

    def features(self, *, date_from: date, date_to: date, snapshot_id: str | None = None) -> pd.DataFrame:
        df = self._window(date_from, date_to).sort_values(["snapshot_date", "account_id"])
        return df[COLUMNS].reset_index(drop=True)

    def context(self, *, date_from: date, date_to: date, max_mules: int, negatives_per_mule: int,
                snapshot_id: str | None = None) -> pd.DataFrame:
        df = self._window(date_from, date_to)
        mules = (df[df[LABEL] == 1].sort_values(["snapshot_date", "account_id"], ascending=[False, True])
                 .head(max_mules))
        neg = (df[df[LABEL] == 0].sort_values(["_bucket", "account_id", "snapshot_date"])
               .head(len(mules) * negatives_per_mule))
        return pd.concat([mules, neg])[COLUMNS].reset_index(drop=True)

    def holdout_sample(self, *, date_from: date, date_to: date, negative_share: float,
                       snapshot_id: str | None = None) -> pd.DataFrame:
        df = self._window(date_from, date_to)
        df = df[df[LABEL].notna()]
        keep = (df[LABEL] == 1) | (df["_bucket"] < int(negative_share * HASH_BUCKETS))
        out = df[keep][COLUMNS].copy()
        out["weight"] = np.where(out[LABEL] == 1, 1.0, 1.0 / negative_share)
        return out.reset_index(drop=True)

    def labelled_rate(self, *, date_from: date, date_to: date, snapshot_id: str | None = None) -> float | None:
        df = self._window(date_from, date_to)
        v = df[LABEL].mean()
        return None if pd.isna(v) else float(v)

    def replace_run(self, key: str, df: pd.DataFrame, run_date: date) -> None:
        cols = OUTPUT_TABLES[key]
        df = _cast(df[[c for c, _ in cols]], cols)
        path = self._path(key)
        if path.exists():
            old = pd.read_parquet(path)
            old = old[pd.to_datetime(old["run_date"]).dt.date != run_date]
            df = pd.concat([_cast(old, cols), df], ignore_index=True)
        self.dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)
        logger.info("wrote %d rows for %s to %s", (df["run_date"] == run_date).sum(), run_date, path)

    def append(self, key: str, df: pd.DataFrame) -> None:
        cols = TABLE_COLUMNS[key]
        df = _cast(df[[c for c, _ in cols]], cols)
        path = self._path(key)
        if path.exists():
            df = pd.concat([_cast(pd.read_parquet(path), cols), df], ignore_index=True)
        self.dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)


def _sql_literal(value, typ: str) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)) or value is pd.NaT:
        return "NULL"
    if typ in ("DOUBLE", "INT", "BIGINT"):
        return repr(float(value)) if typ == "DOUBLE" else str(int(value))
    if typ == "BOOLEAN":
        return "true" if bool(value) else "false"
    if typ == "DATE":
        return f"DATE '{pd.Timestamp(value).date().isoformat()}'"
    if typ == "TIMESTAMP":
        return f"CAST('{pd.Timestamp(value).strftime('%Y-%m-%d %H:%M:%S')}' AS TIMESTAMP)"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _d(x: date) -> str:
    return f"DATE '{x.isoformat()}'"


class ImpalaStorage:
    name = "impala"
    INSERT_CHUNK = 500

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self._conn = None
        self._ensured: set[str] = set()

    def _connect(self):
        if self._conn is None:
            from impala.dbapi import connect

            c = self.cfg
            if not c["host"]:
                raise RuntimeError("MULE_IMPALA_HOST is not set (CDW Impala virtual warehouse host)")
            kwargs = dict(host=c["host"], port=int(c["port"]), use_ssl=c["use_ssl"],
                          use_http_transport=c["use_http_transport"], http_path=c["http_path"],
                          auth_mechanism=c["auth_mechanism"])
            if c["auth_mechanism"].upper() == "GSSAPI":
                kwargs["kerberos_service_name"] = c["kerberos_service_name"]
            if c.get("user"):
                kwargs["user"] = c["user"]
            if c.get("password"):
                kwargs["password"] = c["password"]
            self._conn = connect(**kwargs)
        return self._conn

    def query(self, sql: str) -> pd.DataFrame:
        cur = self._connect().cursor()
        try:
            cur.execute(sql)
            if cur.description is None:
                return pd.DataFrame()
            cols = [d[0].split(".")[-1] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
        finally:
            cur.close()

    def execute(self, sql: str) -> None:
        cur = self._connect().cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()

    def exists(self, key: str) -> bool:
        db, name = table(key).split(".")
        return not self.query(f"SHOW TABLES IN {db} LIKE '{name}'").empty

    def refresh(self, key: str) -> None:
        """Pick up snapshots committed by CDE since Impala last loaded the table's metadata."""
        self.execute(f"REFRESH {table(key)}")

    def read(self, key: str, where_run_date: date | None = None) -> pd.DataFrame:
        sql = f"SELECT * FROM {table(key)}"
        if where_run_date is not None:
            sql += f" WHERE run_date = {_d(where_run_date)}"
        return self.query(sql)

    def snapshot_id(self, key: str) -> str | None:
        """Current Iceberg snapshot of a table, recorded for lineage."""
        try:
            hist = self.query(f"DESCRIBE HISTORY {table(key)}")
            return str(hist.iloc[-1]["snapshot_id"]) if not hist.empty else None
        except Exception as e:  # lineage is best-effort, never fail the run on it
            logger.warning("could not read snapshot history for %s: %s", key, e)
            return None

    @staticmethod
    def _source(snapshot_id: str | None) -> str:
        t = table("mule_features")
        return f"{t} FOR SYSTEM_VERSION AS OF {int(snapshot_id)}" if snapshot_id else t

    @staticmethod
    def _bucket() -> str:
        return (f"pmod(fnv_hash(concat(account_id, '|', cast(snapshot_date AS STRING))), {HASH_BUCKETS})")

    def labelled_dates(self, snapshot_id: str | None = None) -> list[date]:
        df = self.query(f"SELECT DISTINCT snapshot_date FROM {self._source(snapshot_id)} "
                        f"WHERE {LABEL} IS NOT NULL")
        return sorted(pd.to_datetime(df["snapshot_date"]).dt.date)

    def latest_snapshot_date(self, snapshot_id: str | None = None) -> date:
        df = self.query(f"SELECT MAX(snapshot_date) AS d FROM {self._source(snapshot_id)}")
        return pd.Timestamp(df.iloc[0]["d"]).date()

    def _select(self, where: str, snapshot_id, order: str = "snapshot_date, account_id", limit: int | None = None,
                extra: str = "") -> pd.DataFrame:
        sql = f"SELECT {', '.join(COLUMNS)}{extra} FROM {self._source(snapshot_id)} WHERE {where} ORDER BY {order}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        return self.query(sql)

    def features(self, *, date_from: date, date_to: date, snapshot_id: str | None = None) -> pd.DataFrame:
        return _normalise_features(self._select(
            f"snapshot_date BETWEEN {_d(date_from)} AND {_d(date_to)}", snapshot_id))

    def context(self, *, date_from: date, date_to: date, max_mules: int, negatives_per_mule: int,
                snapshot_id: str | None = None) -> pd.DataFrame:
        win = f"snapshot_date BETWEEN {_d(date_from)} AND {_d(date_to)}"
        mules = self._select(f"{win} AND {LABEL} = 1", snapshot_id, "snapshot_date DESC, account_id", max_mules)
        n_neg = len(mules) * negatives_per_mule
        neg = self._select(f"{win} AND {LABEL} = 0", snapshot_id, f"{self._bucket()}, account_id, snapshot_date",
                           n_neg) if n_neg else pd.DataFrame(columns=COLUMNS)
        return _normalise_features(pd.concat([mules, neg], ignore_index=True))

    def holdout_sample(self, *, date_from: date, date_to: date, negative_share: float,
                       snapshot_id: str | None = None) -> pd.DataFrame:
        cut = int(negative_share * HASH_BUCKETS)
        df = _normalise_features(self._select(
            f"snapshot_date BETWEEN {_d(date_from)} AND {_d(date_to)} AND {LABEL} IS NOT NULL "
            f"AND ({LABEL} = 1 OR {self._bucket()} < {cut})", snapshot_id))
        df["weight"] = np.where(df[LABEL] == 1, 1.0, 1.0 / negative_share)
        return df

    def labelled_rate(self, *, date_from: date, date_to: date, snapshot_id: str | None = None) -> float | None:
        df = self.query(f"SELECT AVG({LABEL}) AS r FROM {self._source(snapshot_id)} "
                        f"WHERE snapshot_date BETWEEN {_d(date_from)} AND {_d(date_to)}")
        v = df.iloc[0]["r"] if not df.empty else None
        return None if v is None or pd.isna(v) else float(v)

    def ensure_table(self, key: str) -> None:
        if key in self._ensured:
            return
        full = table(key)
        cols = ", ".join(f"{c} {t}" for c, t in TABLE_COLUMNS[key])
        self.execute(f"CREATE DATABASE IF NOT EXISTS {full.split('.')[0]}")
        self.execute(f"CREATE TABLE IF NOT EXISTS {full} ({cols}) PARTITIONED BY SPEC ({PARTITION[key]}) "
                     f"STORED AS ICEBERG TBLPROPERTIES ('format-version'='2')")
        self._ensured.add(key)

    def _insert(self, key: str, df: pd.DataFrame) -> None:
        cols = TABLE_COLUMNS[key]
        full = table(key)
        records = df[[c for c, _ in cols]].to_dict("records")
        for i in range(0, len(records), self.INSERT_CHUNK):
            values = ",\n".join(
                "(" + ", ".join(_sql_literal(r[c], t) for c, t in cols) + ")"
                for r in records[i:i + self.INSERT_CHUNK])
            self.execute(f"INSERT INTO {full} ({', '.join(c for c, _ in cols)}) VALUES {values}")

    def replace_run(self, key: str, df: pd.DataFrame, run_date: date) -> None:
        self.ensure_table(key)
        self.execute(f"DELETE FROM {table(key)} WHERE run_date = {_d(run_date)}")
        self._insert(key, df)
        logger.info("wrote %d rows for %s to %s", len(df), run_date, table(key))

    def append(self, key: str, df: pd.DataFrame) -> None:
        self.ensure_table(key)
        self._insert(key, df)

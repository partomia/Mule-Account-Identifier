import importlib.util
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from mule.features import COLUMNS, LABEL  # noqa: E402


def _load(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def generator():
    return _load(ROOT / "cde" / "jobs" / "generate_mule_bronze.py")


@pytest.fixture(scope="session")
def gold_job():
    pytest.importorskip("pyspark")
    return _load(ROOT / "cde" / "jobs" / "build_gold_features.py")


@pytest.fixture(scope="session")
def graph_job():
    pytest.importorskip("pyspark")
    return _load(ROOT / "cde" / "jobs" / "build_identity_graph.py")


def synthetic_features(weeks: int = 60, per_week: int = 40, seed: int = 0,
                       last: date = date(2026, 9, 25)) -> pd.DataFrame:
    """Small gold-like feature table: p_mule rises with pass-through and proximity
    to a known mule. The first 3 accounts of each week form a small ring (shared
    device / mobile, one own-bank transfer edge); the rest are solo accounts.
    config/policy.yaml needs 90 (label horizon) + 90 (holdout gap) + 26 weeks
    (context lookback) of history behind the test window, so this covers
    60 weeks - comfortably past the ~56 weeks that chain requires."""
    rng = np.random.default_rng(seed)
    rows = []
    for w in range(weeks):
        d = last - timedelta(weeks=weeks - 1 - w)
        labelled = d + timedelta(days=90) <= last
        for i in range(per_week):
            ring = i < 3
            ring_id = f"RING-W{w:02d}" if ring else f"SOLO-W{w:02d}-{i:03d}"
            pass_through = rng.uniform(0.6, 0.95) if ring else rng.uniform(0.0, 0.5)
            hops = 1 if ring else int(rng.integers(1, 4))
            mule = 1.0 if (ring and rng.random() < 0.6) else 0.0
            rows.append({
                "account_id": f"ACC-W{w:02d}-{i:03d}", "snapshot_date": d, "cif": f"CIF-W{w:02d}-{i:03d}",
                "branch_code": "BR01", "product_code": "SB_BSBD", "person_cluster_id": ring_id, "ring_id": ring_id,
                "account_age_days": int(rng.integers(30, 2000)), "min_kyc_flag": int(rng.integers(0, 2)),
                "dormant_reactivated_flag": int(rng.integers(0, 2)),
                "inflow_to_declared_income_30d": float(rng.uniform(0.2, 6.0)),
                "distinct_senders_7d": int(rng.integers(0, 20)), "distinct_receivers_7d": int(rng.integers(0, 10)),
                "pass_through_ratio_7d": float(pass_through), "median_hold_hours_30d": float(rng.uniform(1, 200)),
                "night_txn_share_30d": float(rng.uniform(0, 0.5)), "round_amount_share_30d": float(rng.uniform(0, 0.5)),
                "new_device_logins_30d": int(rng.integers(0, 3)), "vpa_or_mobile_changes_30d": int(rng.integers(0, 2)),
                "accounts_on_same_device": 3 if ring else int(rng.integers(1, 2)),
                "cifs_sharing_mobile": 2 if ring else 1, "person_cluster_size": 3 if ring else 1,
                "ring_size": 3 if ring else 1, "hops_to_known_mule": hops,
                "credits_from_complainants_30d": int(rng.integers(0, 2)), LABEL: (mule if labelled else np.nan),
            })
    return pd.DataFrame(rows)[COLUMNS]


@pytest.fixture()
def parquet_storage(tmp_path, monkeypatch):
    """ParquetStorage over a temp dir holding a synthetic features table, plus
    the identity / money-flow edges and customer rows for the ring accounts,
    so the app's linked-identity and ring-view tabs have something to render."""
    from mule.config import table
    from mule.storage import ParquetStorage

    def path(key: str) -> Path:
        return tmp_path / f"{table(key).split('.')[-1]}.parquet"

    df = synthetic_features()
    df.to_parquet(path("mule_features"), index=False)

    rings = df[df["ring_id"].str.startswith("RING-")]
    id_edges, graph_edges, customers = [], [], []
    for ring_id, grp in rings.groupby("ring_id"):
        cifs = grp["cif"].tolist()
        d = grp["snapshot_date"].iloc[0]
        for a, b in zip(cifs, cifs[1:]):
            id_edges.append({"src_cif": a, "dst_cif": b, "link_type": "DEVICE", "first_seen": d - timedelta(days=30),
                             "identifier": f"device-{ring_id}"})
            graph_edges.append({"snapshot_date": d, "src_cif": a, "dst_cif": b, "link_type": "TRANSFER", "weight": 1})
    for cif in df["cif"].unique():
        customers.append({"cif": cif, "customer_name": f"Customer {cif}", "occupation": "SALARIED",
                          "home_branch": "BR01", "kyc_type": "FULL_KYC"})
    pd.DataFrame(id_edges).to_parquet(path("identity_edges"), index=False)
    pd.DataFrame(graph_edges).to_parquet(path("graph_edges"), index=False)
    pd.DataFrame(customers).to_parquet(path("customer"), index=False)

    monkeypatch.setattr("mule.pipeline.CONTEXT_FILE", tmp_path / "models" / "mule_context.parquet")
    return ParquetStorage(tmp_path)

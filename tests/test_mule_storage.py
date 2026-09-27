"""The parquet storage backend: same read/write contract as Impala, for a
laptop, Docker, CI or an offline demo."""

from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from mule.features import COLUMNS, FEATURES, LABEL
from mule.storage import ParquetStorage, get_storage


def _row(account_id, snapshot_date, label, **overrides):
    row = {c: 0 for c in COLUMNS}
    row.update(account_id=account_id, snapshot_date=snapshot_date, cif=f"cif-{account_id}", branch_code="B1",
               product_code="SAVINGS", person_cluster_id=f"pc-{account_id}", ring_id=f"ring-{account_id}",
               **{f: 1.0 for f in FEATURES}, **overrides)
    row[LABEL] = label
    return row


@pytest.fixture
def storage(tmp_path):
    d0 = date(2026, 1, 2)
    rows = []
    for week in range(6):
        snap = d0 + timedelta(weeks=week)
        for i in range(20):
            label = 1 if i == 0 and week % 2 == 0 else (None if week == 5 else 0)
            rows.append(_row(f"acct-{i}", snap, label))
    df = pd.DataFrame(rows)
    s = ParquetStorage(tmp_path)
    s.dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(s._path("mule_features"), index=False)
    return s


def test_get_storage_dispatches_on_backend(tmp_path):
    s = get_storage("parquet")
    assert s.name == "parquet"
    with pytest.raises(ValueError):
        get_storage("not-a-backend")


def test_labelled_dates_excludes_null_label_weeks(storage):
    dates = storage.labelled_dates()
    assert len(dates) == 5  # week 5 is all-NULL (too recent)
    assert dates == sorted(dates)


def test_latest_snapshot_date_is_the_max(storage):
    assert storage.latest_snapshot_date() == date(2026, 1, 2) + timedelta(weeks=5)


def test_features_window_is_inclusive(storage):
    d0 = date(2026, 1, 2)
    df = storage.features(date_from=d0, date_to=d0)
    assert len(df) == 20
    assert set(df["snapshot_date"]) == {d0}


def test_context_pulls_mules_and_a_multiple_of_negatives(storage):
    d0 = date(2026, 1, 2)
    ctx = storage.context(date_from=d0, date_to=d0 + timedelta(weeks=4), max_mules=100, negatives_per_mule=3)
    n_mules = int(ctx[LABEL].sum())
    assert n_mules == 3  # weeks 0, 2, 4 each plant one mule
    assert len(ctx) == n_mules * 4  # mules + 3x negatives


def test_context_respects_max_mules(storage):
    d0 = date(2026, 1, 2)
    ctx = storage.context(date_from=d0, date_to=d0 + timedelta(weeks=4), max_mules=1, negatives_per_mule=2)
    assert int(ctx[LABEL].sum()) == 1
    assert len(ctx) == 3


def test_holdout_sample_weights_are_inverse_of_negative_share(storage):
    d0 = date(2026, 1, 2)
    out = storage.holdout_sample(date_from=d0, date_to=d0 + timedelta(weeks=4), negative_share=0.5)
    assert out["weight"].isin([1.0, 2.0]).all()
    assert (out.loc[out[LABEL] == 1, "weight"] == 1.0).all()
    assert not out[LABEL].isna().any()  # week 5 (unlabelled) is out of range anyway


def test_labelled_rate_is_none_when_all_null(storage, tmp_path):
    d0 = date(2026, 1, 2)
    week5 = d0 + timedelta(weeks=5)
    assert storage.labelled_rate(date_from=week5, date_to=week5) is None
    assert storage.labelled_rate(date_from=d0, date_to=d0) == pytest.approx(0.05)


def test_replace_run_swaps_only_the_given_run_date(tmp_path):
    from mule.schema import OUTPUT_TABLES
    s = ParquetStorage(tmp_path)
    cols = [c for c, _ in OUTPUT_TABLES["mule_model_run"]]
    day1, day2 = date(2026, 1, 2), date(2026, 1, 9)

    def rows(run_date, n):
        return pd.DataFrame([{c: None for c in cols} | {"run_date": run_date, "run_id": f"{run_date}-{i}",
                                                          "scored_accounts": n} for i in range(1)])

    s.replace_run("mule_model_run", rows(day1, 10), day1)
    s.replace_run("mule_model_run", rows(day2, 20), day2)
    all_rows = s.read("mule_model_run")
    assert set(pd.to_datetime(all_rows["run_date"]).dt.date) == {day1, day2}

    s.replace_run("mule_model_run", rows(day1, 99), day1)
    all_rows = s.read("mule_model_run")
    assert len(all_rows) == 2
    day1_row = all_rows[pd.to_datetime(all_rows["run_date"]).dt.date == day1].iloc[0]
    assert day1_row["scored_accounts"] == 99


def test_append_accumulates_rows(tmp_path):
    from mule.schema import INVESTIGATOR_DECISIONS
    s = ParquetStorage(tmp_path)
    cols = [c for c, _ in INVESTIGATOR_DECISIONS]
    row = {c: None for c in cols}
    s.append("investigator_decisions", pd.DataFrame([{**row, "decision_id": "d1"}]))
    s.append("investigator_decisions", pd.DataFrame([{**row, "decision_id": "d2"}]))
    out = s.read("investigator_decisions")
    assert sorted(out["decision_id"]) == ["d1", "d2"]


def test_read_missing_table_raises(tmp_path):
    s = ParquetStorage(tmp_path)
    with pytest.raises(FileNotFoundError):
        s.read("mule_features")

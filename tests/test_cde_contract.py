"""The CDE jobs ship without the mule package, so their copies of the schema must match it."""

from datetime import date

from mule import features


def test_gold_schema_matches_package(gold_job):
    assert gold_job.FEATURES == features.FEATURES
    assert gold_job.LABEL == features.LABEL
    assert gold_job.INFO == features.INFO
    assert gold_job.COLUMNS == features.COLUMNS


def test_graph_and_gold_share_snapshot_calendar(gold_job, graph_job):
    assert gold_job.FIRST_SNAPSHOT == graph_job.FIRST_SNAPSHOT
    assert gold_job.FIRST_SNAPSHOT.weekday() == 4
    assert gold_job.FRAUD_FREEZES == graph_job.FRAUD_FREEZES
    for as_of in (date(2026, 9, 25), date(2026, 9, 27), date(2025, 5, 2)):
        snaps = gold_job.snapshot_dates(as_of)
        assert snaps == graph_job.snapshot_dates(as_of)
        assert snaps[-1] == as_of and snaps[0] == gold_job.FIRST_SNAPSHOT
        assert all(d.weekday() == 4 for d in snaps[:-1])
        assert len(set(snaps)) == len(snaps)

"""Headless render of every tab of the Investigator Console against a small
parquet lakehouse scored with the logistic stand-in (no checkpoint needed)."""

from datetime import date

import pytest

st = pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

from mule.config import ROOT  # noqa: E402
from mule.model import StubClassifier  # noqa: E402
from mule.pipeline import run_daily  # noqa: E402


@pytest.fixture
def parquet_env(parquet_storage, monkeypatch):
    for d in (date(2026, 9, 18), date(2026, 9, 25)):
        run_daily(parquet_storage, factory=StubClassifier, run_date=d, publish_on_fail=True)
    monkeypatch.setenv("MULE_STORAGE_BACKEND", "parquet")
    monkeypatch.setenv("MULE_STORAGE_PARQUET_DIR", str(parquet_storage.dir))
    monkeypatch.setenv("MULE_ENDPOINT_URL", "")
    from mule import client, config
    from mule.scoring import load_scorer

    config.settings.cache_clear()
    st.cache_data.clear()
    st.cache_resource.clear()  # app/data.py's storage() is cache_resource; a stale one leaks across tests
    monkeypatch.setattr(client, "_local", load_scorer("file", factory=StubClassifier))
    yield
    config.settings.cache_clear()
    st.cache_data.clear()
    st.cache_resource.clear()


def test_app_renders_every_tab(parquet_env, parquet_storage):
    at = AppTest.from_file(str(ROOT / "app" / "streamlit_app.py"), default_timeout=60)
    at.run()
    assert not at.exception, at.exception
    assert len(at.tabs) == 7
    assert any("Accounts scored" in m.label for m in at.metric)
    assert any("Top 1% of the book catches" in m.label for m in at.metric)


def test_linked_identity_and_ring_tabs_show_the_synthetic_ring(parquet_env):
    at = AppTest.from_file(str(ROOT / "app" / "streamlit_app.py"), default_timeout=60)
    at.run()
    assert not at.exception, at.exception
    # every ring in the synthetic fixture has 3 members: the ring-view metric should say so
    assert any(m.label == "Accounts in ring" and m.value == "3" for m in at.metric)
    # the linked-identity tab found at least one shared-device edge for the selected account
    assert any(not df.value.empty for df in at.dataframe)


def test_whatif_scores_and_shows_a_delta(parquet_env):
    at = AppTest.from_file(str(ROOT / "app" / "streamlit_app.py"), default_timeout=60)
    at.run()
    at.button(key="whatif_score").click().run()
    assert not at.exception, at.exception
    assert any("with changes" in m.label for m in at.metric)


def test_decision_is_appended_to_bronze(parquet_env, parquet_storage):
    at = AppTest.from_file(str(ROOT / "app" / "streamlit_app.py"), default_timeout=60)
    at.run()
    next(b for b in at.button if b.label == "Save decision").click().run()
    assert not at.exception, at.exception
    saved = parquet_storage.read("investigator_decisions")
    assert len(saved) == 1
    assert saved.iloc[0]["decision"] in {"CONFIRMED_MULE", "FALSE_POSITIVE", "NEEDS_MORE_INFO"}

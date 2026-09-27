import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


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

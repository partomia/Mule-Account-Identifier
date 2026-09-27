"""
CAI Model Deployment: mule probability for accounts sent on demand, e.g. when
an investigator opens an account or a what-if changes a feature such as
pass_through_ratio_7d or new_device_logins_30d.

Model Deployments > New Model: File cai/model/predict.py, Function predict,
Python 3.11 runtime, GPU profile if available (4 vCPU / 16 GB otherwise). The
model build runs cdsw-build.sh.

At start-up each replica rebuilds the context of the latest daily run from
gold via Impala (same Iceberg snapshot, window and row count; needs
MULE_IMPALA_USER / MULE_IMPALA_PASSWORD in the model's environment), or reads
models/mule_context.parquet if Impala is not configured. Restart the model to
pick up a newer daily run. Request/response format: see mule/scoring.py.
"""

import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path(os.getcwd())


sys.path.insert(0, str(_repo_root()))

from mule.scoring import load_scorer, score  # noqa: E402

try:
    import cml.models_v1 as models

    cml_model = models.cml_model
except ImportError:  # outside CAI (local tests)
    def cml_model(fn):
        return fn

CLF, META = load_scorer(os.environ.get("MULE_ENDPOINT_CONTEXT", "auto"))


@cml_model
def predict(args):
    return score(args, CLF, META)

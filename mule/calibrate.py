"""Prior correction from the context's mule share to the book's.

The context is enriched (every mule in the window plus a few non-mules per
mule), so the model's probabilities are calibrated to the context share,
not to the book. Bayes' rule with the odds ratio of the two priors maps them
back; ranking is unchanged, only the probability an investigator reads is.

  r     = (pi_book / (1 - pi_book)) / (pi_ctx / (1 - pi_ctx))
  p_adj = p r / (p r + 1 - p)
"""

from __future__ import annotations

import numpy as np


def odds_ratio(ctx_rate: float, book_rate: float) -> float:
    for name, v in (("ctx_rate", ctx_rate), ("book_rate", book_rate)):
        if not 0.0 < v < 1.0:
            raise ValueError(f"{name} must be in (0, 1), got {v}")
    return (book_rate / (1 - book_rate)) / (ctx_rate / (1 - ctx_rate))


def prior_correct(p, ctx_rate: float, book_rate: float) -> np.ndarray:
    p = np.clip(np.asarray(p, dtype=float), 0.0, 1.0)
    r = odds_ratio(ctx_rate, book_rate)
    return p * r / (p * r + 1.0 - p)

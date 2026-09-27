"""Prior correction: Bayes' rule mapping the context's mule share back to the book's."""

import numpy as np
import pytest

from mule.calibrate import odds_ratio, prior_correct


def test_identity_when_rates_match():
    p = np.array([0.0, 0.1, 0.5, 0.9, 1.0])
    out = prior_correct(p, ctx_rate=0.25, book_rate=0.25)
    assert np.allclose(out, p)


def test_corrects_toward_the_rarer_book_rate():
    # context is enriched (25% mules), the book is much rarer: p_adj should shrink.
    out = prior_correct(np.array([0.5]), ctx_rate=0.25, book_rate=0.001)
    assert out[0] < 0.5


def test_monotonic_in_p():
    p = np.linspace(0.0, 1.0, 21)
    out = prior_correct(p, ctx_rate=0.25, book_rate=0.001)
    assert np.all(np.diff(out) >= 0)


def test_bounds_are_preserved():
    out = prior_correct(np.array([-1.0, 0.0, 1.0, 2.0]), ctx_rate=0.25, book_rate=0.001)
    assert out[0] == out[1]  # clipped to 0
    assert out[2] == out[3]  # clipped to 1
    assert 0.0 <= out.min() and out.max() <= 1.0


@pytest.mark.parametrize("bad", [0.0, 1.0, -0.1, 1.1])
def test_rejects_rates_outside_open_interval(bad):
    with pytest.raises(ValueError):
        odds_ratio(bad, 0.5)
    with pytest.raises(ValueError):
        odds_ratio(0.5, bad)

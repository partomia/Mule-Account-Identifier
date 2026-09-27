"""The pure-Python world simulator behind cde/jobs/generate_mule_bronze.py."""

import collections
import re

N_UNITS = 6000


def _world(generator):
    return [generator.simulate_unit(i, generator.DEFAULT_SEED) for i in range(N_UNITS)]


def test_simulation_is_deterministic(generator):
    for i in (0, 17, 4242):
        a, b = generator.simulate_unit(i, generator.DEFAULT_SEED), generator.simulate_unit(i, generator.DEFAULT_SEED)
        assert generator.unit_rows(a) == generator.unit_rows(b)


def test_ids_are_unique(generator):
    world = _world(generator)
    for pos, key in ((1, 0), (2, 0), (3, 0), (4, 0)):
        ids = [row[key] for u in world for row in generator.unit_rows(u)[pos]]
        assert len(ids) == len(set(ids)), f"duplicate ids in table {pos}"
    cifs = [c[0] for u in world for c in generator.unit_rows(u)[0]]
    assert len(cifs) == len(set(cifs))


def test_mule_share_and_patterns(generator):
    world = _world(generator)
    accounts = [a for u in world for a in u.accounts]
    mules = [a for a in accounts if a.get("mule_pattern")]
    assert 0.004 < len(mules) / len(accounts) < 0.014
    assert {a["mule_pattern"] for a in mules} == {"RECRUITED", "RENTED", "SYNTHETIC"}
    rings = [u for u in world if generator.unit_kind(u.idx, generator.DEFAULT_SEED) == "RING"]
    assert all(3 <= len(u.accounts) <= 25 for u in rings)


def test_rings_leave_the_planted_traces(generator):
    world = _world(generator)
    rings = [u for u in world if generator.unit_kind(u.idx, generator.DEFAULT_SEED) == "RING"]
    shared_device = reused_identity = confirmed = members = 0
    for u in rings:
        devices = collections.defaultdict(set)
        for s in u.sessions:
            devices[s[4]].add(s[1])
        shared_device += any(len(cifs) >= 2 for d, cifs in devices.items() if not d.startswith("bc-"))
        if u.accounts[0]["mule_pattern"] == "SYNTHETIC":
            pans = [c["pan"] for c in u.customers]
            mobiles = [c["mobile"] for c in u.customers]
            addrs = [c["address"] for c in u.customers]
            reused_identity += len(set(pans)) < len(pans) or len(set(mobiles)) < len(mobiles) or len(set(addrs)) < len(addrs)
        members += len(u.accounts)
        confirmed += sum(1 for a in u.accounts if a["freeze_date"])
        credits = [t for t in u.txns if t[4] == "CR"]
        debits = [t for t in u.txns if t[4] == "DR"]
        assert credits and sum(t[5] for t in debits) > 0.7 * sum(t[5] for t in credits)
    assert shared_device >= 0.6 * len(rings)
    assert reused_identity >= 1
    assert 0.5 < confirmed / members < 0.98       # some mules are never confirmed


def test_frozen_accounts_stop_transacting(generator):
    for u in _world(generator):
        frozen = {a["account_id"]: a["freeze_date"] for a in u.accounts if a["freeze_date"]}
        assert not [t for t in u.txns if t[3] in frozen and t[2].date() > frozen[t[3]]]


def test_look_alikes_exist(generator):
    world = _world(generator)
    households = [u for u in world if generator.unit_kind(u.idx, generator.DEFAULT_SEED) == "HOUSEHOLD"]
    assert households
    shared_mobile = sum(len({c["mobile"] for c in u.customers}) < len(u.customers) for u in households)
    assert shared_mobile >= 0.3 * len(households)
    occupations = collections.Counter(c["occupation"] for u in world for c in u.customers)
    assert {"STUDENT", "GIG_WORKER", "SMALL_TRADER"} <= set(occupations)
    disputes = [r for u in world for r in u.reports if r[10] == "goods not delivered"]
    assert all(a["freeze_date"] is None or a["freeze_reason"] == "KYC_PENDING"
               for u in world for a in u.accounts if not a.get("mule_pattern"))
    assert disputes


def test_raw_pan_only_in_the_pan_column(generator):
    pan = re.compile(r"[A-Z]{5}[0-9]{4}[A-Z]")
    for u in _world(generator)[:500]:
        cust, _, txns, sessions, reports = generator.unit_rows(u)
        assert all(pan.fullmatch(c[4]) for c in cust)
        for row in [*txns, *sessions, *reports]:
            assert not any(isinstance(v, str) and pan.search(v) for v in row)

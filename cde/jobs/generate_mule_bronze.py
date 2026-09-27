"""
Stage 1 - Generate (bronze)

Synthesises the five source systems a bank's fraud team would have to join to
find mule accounts, and lands them raw in bronze:

  kyc_onboarding     customer (CIF): raw PAN, Aadhaar vault ref, mobile, email, address,
                     KYC type, declared income, occupation, onboarding branch / channel
  cbs_accounts       account: CIF, product, branch, open date, status, dormancy, freeze
  upi_transactions   one row per leg on our side: CR / DR, amount, payer / payee VPA,
                     counterparty account when it is also ours
  digital_sessions   mobile / internet banking sessions: device fingerprint, IP, city,
                     device binds, mobile changes, VPA creation
  fraud_reports      1930 / NCRP complaints, I4C Suspect Registry matches, internal FRM
  ref.branch_map, ref.product_map, ref.bank_devices (branch kiosks, BC-agent terminals)

In production this job would read the onboarding, CBS, UPI switch, digital
banking and complaint extracts instead.

How the synthetic bank behaves. The world is a list of units simulated from a
per-unit RNG:

  * single customers: salaried, self-employed, small traders (many small
    credits from many payers), gig workers (frequent platform payouts, spend
    fast), students (often minimum KYC, small P2P credits from a college
    circle, spend fast), homemakers / pensioners / farmers (low activity)
  * households: 2-4 customers at one address, often one shared phone and a
    shared device, with monthly own-bank transfers (legitimate look-alikes)
  * mule rings of 3-25 accounts run by one controller, in three patterns:
      recruited  old dormant account reactivated, credentials often handed over
      rented     fresh minimum-KYC account, operated from the controller's devices
      synthetic  new CIF reusing a PAN, mobile or address of another member
    Victims (other banks' customers) pay one to three members; each credit is
    sent on within hours to the controller's beneficiaries or to a second-tier
    member. About half the victims complain on 1930 / NCRP some days later; the
    named account is frozen. After the first freeze, I4C Suspect Registry
    matches and internal FRM reviews catch some of the rest. Some mules are
    never confirmed (label 0), as in real life.

Branch kiosks and BC-agent devices are shared by hundreds of customers
(identity hubs the graph job must ignore). Customers who are not digital
never log in.

History is prefix-stable: the world is simulated from a fixed seed up to
WORLD_END and --as-of only truncates it. A later as-of date adds new days
without changing any earlier fact, so daily runs behave like real loads.
0.02% of records are re-sent (duplicates for silver to remove).

--inject-bad-data adds transactions for an unknown account, null keys and a
raw PAN in a free-text column, so the validation gate fails on cue.

Writes (drop + recreate every run):
  <prefix>_bronze.{kyc_onboarding, cbs_accounts, upi_transactions, digital_sessions, fraud_reports}
  <prefix>_ref.{branch_map, product_map, bank_devices}

Usage:
  spark-submit generate_mule_bronze.py [--as-of YYYY-MM-DD] [--db-prefix P]
                                       [--customers N] [--seed S] [--inject-bad-data]
"""

from __future__ import annotations

import argparse
import logging
import math
import random
from datetime import date, datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_DB_PREFIX = "rsingh_mule_acct"
DEFAULT_SEED = 20250401
DEFAULT_CUSTOMERS = 200_000
HISTORY_START = date(2025, 4, 1)   # first day of the transaction / session / report extracts
WORLD_END = date(2027, 3, 31)
OPEN_START = date(2008, 1, 1)
DUPLICATE_RATE = 0.0002
RING_UNIT_RATE = 0.0018            # ~0.8% of accounts are mules
HOUSEHOLD_UNIT_RATE = 0.045
BANK_HANDLE = "dbank"
N_MERCHANTS, N_EMPLOYERS, N_COLLEGES, N_BC_DEVICES = 2000, 800, 600, 300
EXCHANGE_VPAS = [f"p2p.exchange{k}@ybl" for k in range(8)]   # shared cash-out hubs used by many rings
PLATFORMS = ["payouts.swiggy@icici", "payouts.zomato@hdfcbank", "driver.uber@axisbank",
             "partner.olacabs@icici", "rider.zepto@ybl", "payout.urbanco@hdfcbank"]
GOVT = {"PENSIONER": "pension.cpao@sbi", "FARMER": "pfms.dbt@sbi", "HOMEMAKER": "pfms.dbt@sbi"}
EXT_HANDLES = ["okaxis", "ybl", "paytm", "oksbi", "ibl", "okhdfcbank", "apl"]
IFSC = ["HDFC0001234", "ICIC0004321", "SBIN0007788", "UTIB0002211", "KKBK0005566", "PUNB0110200"]

REGIONS = {
    "NORTH": [("DEL", "Delhi", "DL"), ("LKO", "Lucknow", "UP"), ("JAI", "Jaipur", "RJ"), ("CHD", "Chandigarh", "CH")],
    "SOUTH": [("CHE", "Chennai", "TN"), ("BLR", "Bengaluru", "KA"), ("HYD", "Hyderabad", "TS"), ("KOC", "Kochi", "KL")],
    "EAST": [("KOL", "Kolkata", "WB"), ("PAT", "Patna", "BR"), ("BBS", "Bhubaneswar", "OD"), ("GUW", "Guwahati", "AS")],
    "WEST": [("MUM", "Mumbai", "MH"), ("PUN", "Pune", "MH"), ("AHD", "Ahmedabad", "GJ"), ("SUR", "Surat", "GJ")],
    "CENTRAL": [("BPL", "Bhopal", "MP"), ("IND", "Indore", "MP"), ("RAI", "Raipur", "CG"), ("NAG", "Nagpur", "MH")],
}
BRANCHES = [(f"{code}{n:02d}", f"{city} Branch {n}", city, region, state)
            for region, cities in REGIONS.items() for code, city, state in cities for n in (1, 2, 3)]
PRODUCTS = {
    "SB_REGULAR": ("Savings account", "SB"),
    "SB_SALARY": ("Salary savings account", "SB"),
    "SB_STUDENT": ("Student savings account", "SB"),
    "SB_BSBD": ("Basic savings (BSBD, small account)", "BS"),
    "CA_CURRENT": ("Current account", "CA"),
}
FIRST = ["Aarav", "Vivaan", "Aditya", "Arjun", "Sai", "Reyansh", "Krishna", "Ishaan", "Rohan", "Rahul", "Amit",
         "Vikram", "Suresh", "Ramesh", "Manoj", "Deepak", "Imran", "Salman", "Joseph", "Harpreet", "Gurpreet",
         "Ananya", "Diya", "Priya", "Pooja", "Sneha", "Kavya", "Meera", "Lakshmi", "Sunita", "Anita", "Rekha",
         "Fatima", "Ayesha", "Mary", "Simran", "Neha", "Divya", "Riya", "Nisha", "Karthik", "Venkat", "Srinivas",
         "Ravi", "Ganesh", "Mohan", "Prakash", "Sanjay", "Ajay", "Vijay", "Kiran", "Swati", "Shweta", "Jyoti"]
LAST = ["Sharma", "Verma", "Gupta", "Singh", "Kumar", "Yadav", "Patel", "Shah", "Mehta", "Reddy", "Rao", "Naidu",
        "Iyer", "Nair", "Menon", "Pillai", "Das", "Ghosh", "Banerjee", "Mukherjee", "Khan", "Ansari", "Sheikh",
        "Fernandes", "D'Souza", "Gill", "Sandhu", "Joshi", "Kulkarni", "Deshpande", "Mishra", "Tiwari", "Pandey",
        "Chauhan", "Rathore", "Jain", "Agarwal", "Bose", "Sahu", "Behera"]
STREETS = ["MG Road", "Station Road", "Gandhi Nagar", "Nehru Street", "Shivaji Chowk", "Park Street",
           "Lake View Road", "Temple Street", "Market Road", "Ring Road", "Civil Lines", "Anna Salai"]
OCCUPATIONS = [("SALARIED", 0.33), ("SELF_EMPLOYED", 0.12), ("SMALL_TRADER", 0.05), ("GIG_WORKER", 0.06),
               ("STUDENT", 0.09), ("HOMEMAKER", 0.12), ("PENSIONER", 0.10), ("FARMER", 0.13)]
DIGITAL = {"SALARIED": 0.88, "SELF_EMPLOYED": 0.8, "SMALL_TRADER": 0.75, "GIG_WORKER": 0.97, "STUDENT": 0.97,
           "HOMEMAKER": 0.45, "PENSIONER": 0.35, "FARMER": 0.3, "UNEMPLOYED": 0.9}
INCOME = {"SALARIED": (720_000, 0.5), "SELF_EMPLOYED": (600_000, 0.5), "SMALL_TRADER": (240_000, 0.4),
          "GIG_WORKER": (216_000, 0.3), "STUDENT": (36_000, 0.8), "HOMEMAKER": (60_000, 0.7),
          "PENSIONER": (300_000, 0.4), "FARMER": (150_000, 0.5), "UNEMPLOYED": (60_000, 0.5)}
SCAMS = ["PART_TIME_JOB_SCAM", "INVESTMENT_SCAM", "UPI_COLLECT_FRAUD", "LOAN_APP_FRAUD", "SEXTORTION",
         "DIGITAL_ARREST", "ONLINE_SHOPPING_FRAUD"]


def _pick(rng: random.Random, weighted):
    x, acc = rng.random(), 0.0
    for value, w in weighted:
        acc += w
        if x < acc:
            return value
    return weighted[-1][0]


def _perm(key: int, mult: int) -> int:
    """Bijection on [1, 2^31 - 2]: distinct keys give distinct, unordered-looking numbers."""
    return (key * mult) % 2147483647


def _mix64(key: int) -> str:
    return f"{(key * 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF:016x}"


def _rand_date(rng: random.Random, lo: date, hi: date) -> date:
    return lo + timedelta(days=rng.randint(0, max(0, (hi - lo).days)))


def _ts(d: date, hour: int, rng: random.Random) -> datetime:
    return datetime(d.year, d.month, d.day, hour % 24, rng.randint(0, 59), rng.randint(0, 59))


def _poisson(rng: random.Random, lam: float) -> int:
    if lam <= 0:
        return 0
    if lam > 30:
        return max(0, int(round(rng.gauss(lam, math.sqrt(lam)))))
    k, p, limit = 0, 1.0, math.exp(-lam)
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


def _day_hour(rng: random.Random, night_share: float) -> int:
    if rng.random() < night_share:
        return rng.choice([23, 0, 1, 2, 3, 4])
    return rng.choice([8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22])


def _round_amount(rng: random.Random, lo: int, hi: int) -> float:
    return float(rng.choice([k for k in (500, 1000, 2000, 2500, 5000, 10000, 15000, 20000, 25000, 49999, 50000)
                             if lo <= k <= hi] or [lo]))


def _lognormal(rng: random.Random, median: float, sigma: float, lo: float, hi: float) -> float:
    return float(round(min(hi, max(lo, median * math.exp(rng.gauss(0.0, sigma)))), 2))


def _pan(rng: random.Random) -> str:
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return ("".join(rng.choice(letters) for _ in range(3)) + "P" + rng.choice(letters)
            + f"{rng.randint(0, 9999):04d}" + rng.choice(letters))


def _mobile(rng: random.Random) -> str:
    return str(rng.choice([6, 7, 8, 9])) + f"{rng.randint(0, 999_999_999):09d}"


def _fmt_mobile(m: str, rng: random.Random) -> str:
    return rng.choice([m, m, "+91" + m, "+91-" + m, "0" + m, f"{m[:5]} {m[5:]}"])


def _ext_vpa(rng: random.Random, stem: str | None = None) -> str:
    stem = stem or (rng.choice(FIRST).lower() + str(rng.randint(10, 99999)))
    return f"{stem}@{rng.choice(EXT_HANDLES)}"


def _device(rng: random.Random) -> str:
    return f"{rng.getrandbits(64):016x}"


def _ip(rng: random.Random, prefix: str | None = None) -> str:
    return f"{prefix or f'{rng.randint(27, 223)}.{rng.randint(0, 255)}.{rng.randint(0, 255)}'}.{rng.randint(1, 254)}"


def _months(lo: date, hi: date):
    y, m = lo.year, lo.month
    while date(y, m, 1) <= hi:
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def _days_in(y: int, m: int) -> int:
    nxt = date(y + (m == 12), m % 12 + 1, 1)
    return (nxt - timedelta(days=1)).day


class Unit:
    """Rows produced by one unit of the world (a customer, a household or a ring)."""

    def __init__(self, idx: int, rng: random.Random):
        self.idx, self.rng = idx, rng
        self.customers, self.accounts, self.txns, self.sessions, self.reports = [], [], [], [], []
        self._seq = 0
        self.n_cif = 0

    def _next(self) -> int:
        self._seq += 1
        return self._seq

    def new_cif(self) -> str:
        self.n_cif += 1
        return f"{_perm(self.idx * 64 + self.n_cif, 48271):010d}"

    def account_id(self, cif_no: int, k: int, product: str) -> str:
        return f"{PRODUCTS[product][1]}-{_perm(self.idx * 512 + cif_no * 8 + k + 1, 69621):010d}"

    def customer(self, **kw) -> dict:
        rng = self.rng
        first, last = rng.choice(FIRST), rng.choice(LAST)
        c = dict(cif=self.new_cif(), cif_no=self.n_cif, customer_name=kw.pop("name", f"{first} {last}"),
                 dob=kw.pop("dob", _rand_date(rng, date(1950, 1, 1), date(2006, 1, 1))),
                 gender=rng.choice(["M", "F"]), pan=kw.pop("pan", _pan(rng)),
                 aadhaar_ref=kw.pop("aadhaar_ref", f"{rng.randint(2, 9)}{rng.randint(0, 10**11 - 1):011d}"),
                 mobile=kw.pop("mobile", _mobile(rng)),
                 email=f"{first.lower()}.{last.lower().replace(chr(39), '')}{rng.randint(1, 999)}@"
                       f"{rng.choice(['gmail.com', 'yahoo.co.in', 'outlook.com', 'rediffmail.com'])}",
                 address=kw.pop("address", None), **kw)
        if c["address"] is None:
            branch = BRANCHES[c["branch_idx"]]
            c["address"] = (f"{rng.randint(1, 450)}, {rng.choice(STREETS)}", branch[2], f"{rng.randint(110001, 855999)}")
        if "declared_income" not in c:
            med, sig = INCOME[c["occupation"]]
            c["declared_income"] = float(round(med * math.exp(rng.gauss(0.0, sig)), -3))
        self.customers.append(c)
        return c

    def account(self, cust: dict, k: int, product: str, open_date: date, **kw) -> dict:
        a = dict(account_id=self.account_id(cust["cif_no"], k, product), cif=cust["cif"], product_code=product,
                 branch_code=BRANCHES[cust["branch_idx"]][0], open_date=open_date, dormant_from=None,
                 reactivated_on=None, freeze_date=None, freeze_reason=None,
                 vpa=f"{cust['customer_name'].split()[0].lower()}{self.rng.randint(10, 99999)}@{BANK_HANDLE}", **kw)
        self.accounts.append(a)
        return a

    def txn(self, acct: dict, ts: datetime, direction: str, amount: float, cpty_vpa: str,
            cpty_account: str | None = None, channel: str = "UPI", remarks: str = "", utr: str | None = None,
            own_vpa: str | None = None) -> str:
        utr = utr or f"{_perm(self.idx * 4096 + self._seq % 4096 + 1, 16807) % 10**8:08d}{self.rng.randint(0, 9999):04d}"
        own = own_vpa or acct["vpa"]
        payer, payee = (cpty_vpa, own) if direction == "CR" else (own, cpty_vpa)
        ifsc = "DBNK0000001" if cpty_account else self.rng.choice(IFSC)
        self.txns.append((_mix64((self.idx << 24) | self._next()), utr, ts, acct["account_id"], direction,
                          float(round(amount, 2)), payer, payee, cpty_account, ifsc, channel, remarks))
        return utr

    def transfer(self, src: dict, dst: dict, ts: datetime, amount: float, remarks: str = "") -> None:
        """Own-bank transfer: a DR leg on src and a CR leg on dst with one UTR."""
        utr = self.txn(src, ts, "DR", amount, dst["vpa"], dst["account_id"], "UPI", remarks)
        self.txn(dst, ts + timedelta(seconds=self.rng.randint(1, 30)), "CR", amount, src["vpa"], src["account_id"],
                 "UPI", remarks, utr=utr)

    def session(self, cif: str, ts: datetime, device: str, ip: str, city: str, event: str = "LOGIN",
                new_value: str | None = None, account_id: str | None = None, channel: str = "MOBILE_APP") -> None:
        self.sessions.append((_mix64((self.idx << 24) | self._next() | (1 << 23)), cif, ts, channel, device, ip,
                              city, event, new_value, account_id))

    def report(self, report_date: date, source: str, account_id=None, vpa=None, mobile=None,
               complainant_vpa=None, amount=None, utr=None, category=None, remarks: str = "") -> None:
        self.reports.append((f"R{_mix64((self.idx << 24) | self._next() | (1 << 22))[:12].upper()}",
                             _ts(report_date, self.rng.randint(9, 20), self.rng), source, account_id, vpa, mobile,
                             complainant_vpa, amount, utr, category, remarks))


# ------------------------------------------------------------------ legit customers

def _legit_customer(u: Unit, occupation: str, branch_idx: int, **kw) -> dict:
    rng = u.rng
    onboard = kw.pop("onboarding_date", _rand_date(rng, OPEN_START, WORLD_END - timedelta(days=30)))
    if occupation == "STUDENT":
        onboard = max(onboard, date(2019, 1, 1))
    digital = rng.random() < DIGITAL[occupation]
    min_kyc = occupation in ("STUDENT", "GIG_WORKER", "FARMER", "HOMEMAKER") and rng.random() < 0.25 and onboard.year >= 2018
    via_bc = min_kyc and rng.random() < 0.5
    kyc_type = "MIN_KYC" if min_kyc else ("VIDEO_KYC" if onboard.year >= 2021 and digital and rng.random() < 0.3
                                           else "FULL_KYC")
    channel = "BC_AGENT" if via_bc else ("DIGITAL" if kyc_type != "FULL_KYC" else "BRANCH")
    return u.customer(occupation=occupation, branch_idx=branch_idx, onboarding_date=onboard, kyc_type=kyc_type,
                      onboarding_channel=channel, digital=digital, **kw)


def _primary_product(rng: random.Random, c: dict) -> str:
    occ = c["occupation"]
    if c["kyc_type"] == "MIN_KYC":
        return "SB_BSBD"
    if occ == "SALARIED":
        return "SB_SALARY" if rng.random() < 0.6 else "SB_REGULAR"
    if occ == "STUDENT":
        return "SB_STUDENT" if rng.random() < 0.6 else "SB_REGULAR"
    if occ == "SMALL_TRADER":
        return "CA_CURRENT" if rng.random() < 0.5 else "SB_REGULAR"
    return "SB_REGULAR"


def _open_accounts(u: Unit, c: dict) -> list[dict]:
    rng = u.rng
    accts = [u.account(c, 0, _primary_product(rng, c), c["onboarding_date"], role="primary")]
    n_extra = _pick(rng, [(0, 0.76), (1, 0.19), (2, 0.05)])
    for k in range(1, n_extra + 1):
        accts.append(u.account(c, k, "SB_REGULAR",
                               _rand_date(rng, c["onboarding_date"], WORLD_END - timedelta(days=10)), role="secondary"))
    for a in accts:
        if a["open_date"] < date(2023, 1, 1) and rng.random() < 0.07:
            a["dormant_from"] = _rand_date(rng, a["open_date"] + timedelta(days=730), HISTORY_START - timedelta(days=1)) \
                if a["open_date"] + timedelta(days=730) < HISTORY_START else None
            if a["dormant_from"] and rng.random() < 0.06:
                a["reactivated_on"] = _rand_date(rng, HISTORY_START + timedelta(days=30), WORLD_END - timedelta(days=30))
        if rng.random() < 0.003:
            a["freeze_date"] = _rand_date(rng, max(a["open_date"], HISTORY_START), WORLD_END)
            a["freeze_reason"] = "KYC_PENDING"
    return accts


def _legit_sessions(u: Unit, c: dict, accts: list[dict], home_ip: str, shared_device: str | None = None) -> None:
    rng = u.rng
    city = BRANCHES[c["branch_idx"]][2]
    start = max(c["onboarding_date"], HISTORY_START)
    if c["onboarding_channel"] == "BC_AGENT" and c["onboarding_date"] >= HISTORY_START:
        u.session(c["cif"], _ts(c["onboarding_date"], rng.randint(10, 17), rng), f"bc-agent-{rng.randrange(N_BC_DEVICES):03d}",
                  _ip(rng), city, "LOGIN", channel="BC_AGENT_APP")
    if not c["digital"]:
        if rng.random() < 0.15:   # occasional branch kiosk use
            kiosk = f"kiosk-{BRANCHES[c['branch_idx']][0]}-{rng.randint(1, 2)}"
            for y, m in _months(start, WORLD_END):
                if rng.random() < 0.35:
                    u.session(c["cif"], _ts(date(y, m, rng.randint(1, _days_in(y, m))), rng.randint(10, 16), rng),
                              kiosk, _ip(rng, "10.20.1"), city, channel="BRANCH_KIOSK")
        return
    device = _device(rng)
    bound = False
    for a in accts:
        if a["open_date"] >= HISTORY_START:
            u.session(c["cif"], _ts(a["open_date"], rng.randint(9, 21), rng), device, _ip(rng, home_ip), city,
                      "VPA_CREATE", a["vpa"], a["account_id"])
    for y, m in _months(start, WORLD_END):
        if rng.random() < 1 / 22:           # new phone
            device = _device(rng)
            bound = False
        n = _poisson(rng, 1.5)
        for _ in range(n):
            d = date(y, m, rng.randint(1, _days_in(y, m)))
            if d < start:
                continue
            dev = shared_device if shared_device and rng.random() < 0.4 else device
            event = "LOGIN"
            if not bound and dev == device:
                event, bound = "DEVICE_BIND", True
            u.session(c["cif"], _ts(d, _day_hour(rng, 0.04), rng), dev, _ip(rng, home_ip), city, event)
        if rng.random() < 0.0015:
            u.session(c["cif"], _ts(date(y, m, rng.randint(1, 28)), 11, rng), device, _ip(rng, home_ip), city,
                      "MOBILE_CHANGE", _fmt_mobile(_mobile(rng), rng))


def _active_months(a: dict):
    start = max(a["open_date"], HISTORY_START)
    if a["dormant_from"] is not None:
        if a["reactivated_on"] is None:
            return
        start = max(start, a["reactivated_on"])
    for y, m in _months(start, WORLD_END):
        yield y, m


def _legit_txns(u: Unit, c: dict, a: dict) -> None:
    rng = u.rng
    occ = c["occupation"]
    income_m = max(5000.0, c["declared_income"] / 12)
    contacts = [_ext_vpa(rng) for _ in range(rng.randint(3, 6))]
    merchants = [f"m{int(N_MERCHANTS * rng.random() ** 2.2):04d}.store@ybl" for _ in range(12)]
    employer = f"payroll{rng.randrange(N_EMPLOYERS):03d}@hdfcbank"
    college = rng.randrange(N_COLLEGES)
    friends = [f"c{college:03d}f{rng.randrange(40):02d}@okaxis" for _ in range(rng.randint(4, 7))]
    parent = _ext_vpa(rng)
    clients = [_ext_vpa(rng) for _ in range(rng.randint(5, 15))]
    suppliers = [_ext_vpa(rng, f"supplier{rng.randint(100, 99999)}") for _ in range(3)]
    regulars = [_ext_vpa(rng) for _ in range(30)]
    platform = rng.sample(PLATFORMS, rng.randint(1, 2))
    secondary = a["role"] == "secondary"
    first_active = max(a["open_date"], HISTORY_START, a["reactivated_on"] or HISTORY_START)

    def when(y, m, night=0.04):
        d = date(y, m, rng.randint(1, _days_in(y, m)))
        return None if d < first_active else _ts(d, _day_hour(rng, night), rng)

    def spend(y, m, n, median, night=0.04, fast_after: datetime | None = None, share=0.0):
        for _ in range(n):
            ts = when(y, m, night)
            if fast_after is not None and rng.random() < share:
                ts = fast_after + timedelta(hours=rng.uniform(0.5, 30))
            if ts is None:
                continue
            if rng.random() < 0.75:
                u.txn(a, ts, "DR", _lognormal(rng, median, 0.8, 20, 60000), rng.choice(merchants))
            else:
                amt = _round_amount(rng, 200, 10000) if rng.random() < 0.6 else _lognormal(rng, median, 0.8, 50, 20000)
                u.txn(a, ts, "DR", amt, rng.choice(contacts))

    for y, m in _active_months(a):
        if a["freeze_date"] and date(y, m, 1) > a["freeze_date"]:
            break
        if secondary:
            if rng.random() < 0.3:
                ts = when(y, m)
                if ts:
                    u.txn(a, ts, "CR", _round_amount(rng, 500, 20000), rng.choice(contacts))
            if rng.random() < 0.3:
                spend(y, m, 1, 800)
            continue
        if occ == "SALARIED":
            d = date(y, m, rng.randint(1, 3))
            if d >= first_active:
                pay_ts = _ts(d, rng.randint(9, 12), rng)
                u.txn(a, pay_ts, "CR", round(income_m * rng.uniform(0.97, 1.03), -1), employer, channel="NEFT",
                      remarks="SALARY")
                if rng.random() < 0.6:   # rent / EMI right after salary
                    u.txn(a, pay_ts + timedelta(hours=rng.uniform(2, 40)), "DR",
                          _round_amount(rng, 5000, 25000), rng.choice(contacts), remarks="rent")
            spend(y, m, _poisson(rng, 3), 700)
        elif occ == "SELF_EMPLOYED":
            for _ in range(_poisson(rng, 2)):
                ts = when(y, m)
                if ts:
                    u.txn(a, ts, "CR", _lognormal(rng, income_m / 2.5, 0.7, 1000, 300000), rng.choice(clients))
            spend(y, m, _poisson(rng, 3.5), 1200)
        elif occ == "SMALL_TRADER":
            for _ in range(_poisson(rng, 14)):
                ts = when(y, m, 0.02)
                if ts:
                    payer = rng.choice(regulars) if rng.random() < 0.5 else _ext_vpa(rng)
                    amt = _round_amount(rng, 500, 2000) if rng.random() < 0.2 else _lognormal(rng, 280, 0.9, 10, 8000)
                    u.txn(a, ts, "CR", amt, payer, remarks="")
            for _ in range(rng.randint(2, 4)):
                ts = when(y, m)
                if ts:
                    u.txn(a, ts, "DR", _lognormal(rng, 9000, 0.6, 2000, 90000), rng.choice(suppliers),
                          channel=rng.choice(["UPI", "IMPS"]))
            spend(y, m, _poisson(rng, 2), 600)
        elif occ == "GIG_WORKER":
            last = None
            for _ in range(_poisson(rng, 7)):
                ts = when(y, m, 0.2)
                if ts:
                    u.txn(a, ts, "CR", _lognormal(rng, 900, 0.5, 100, 6000), rng.choice(platform), remarks="payout")
                    last = ts
            spend(y, m, _poisson(rng, 5), 350, 0.2, last, 0.5)
        elif occ == "STUDENT":
            last = None
            if rng.random() < 0.8:
                ts = when(y, m)
                if ts:
                    u.txn(a, ts, "CR", _round_amount(rng, 2000, 15000), parent, remarks="pocket money")
                    last = ts
            for _ in range(_poisson(rng, 2.5)):
                ts = when(y, m, 0.12)
                if ts:
                    u.txn(a, ts, "CR", _round_amount(rng, 200, 2000), rng.choice(friends))
            for _ in range(_poisson(rng, 2)):
                ts = when(y, m, 0.12)
                if ts:
                    u.txn(a, ts, "DR", _round_amount(rng, 200, 2000), rng.choice(friends))
            spend(y, m, _poisson(rng, 3), 250, 0.12, last, 0.4)
        else:   # HOMEMAKER, PENSIONER, FARMER
            src = GOVT.get(occ, parent)
            if occ == "PENSIONER" or rng.random() < 0.35:
                ts = when(y, m)
                if ts:
                    amt = round(income_m * rng.uniform(0.95, 1.05), -1) if occ == "PENSIONER" else _round_amount(rng, 1000, 6000)
                    u.txn(a, ts, "CR", amt, src, channel="NEFT" if occ == "PENSIONER" else "UPI")
            if rng.random() < 0.25:
                ts = when(y, m)
                if ts:
                    u.txn(a, ts, "CR", _round_amount(rng, 1000, 10000), rng.choice(contacts))
            spend(y, m, _poisson(rng, 1.2), 900)

    if occ in ("SELF_EMPLOYED", "SMALL_TRADER"):   # disputes: complaints that are not fraud and lead to no freeze
        for y in (2025, 2026):
            if rng.random() < 0.004:
                d = _rand_date(rng, max(first_active, date(y, 4, 1)), date(y, 12, 31))
                if d <= WORLD_END:
                    u.report(d, "NCRP_1930", account_id=a["account_id"], complainant_vpa=rng.choice(clients + regulars),
                             amount=_lognormal(rng, 3000, 0.8, 200, 50000), category="ONLINE_SHOPPING_FRAUD",
                             remarks="goods not delivered")


def simulate_single(u: Unit) -> None:
    rng = u.rng
    occ = _pick(rng, OCCUPATIONS)
    c = _legit_customer(u, occ, rng.randrange(len(BRANCHES)))
    accts = _open_accounts(u, c)
    _legit_sessions(u, c, accts, f"{rng.randint(27, 223)}.{rng.randint(0, 255)}.{rng.randint(0, 255)}")
    for a in accts:
        _legit_txns(u, c, a)


def simulate_household(u: Unit) -> None:
    """2-4 customers at one address; often one shared phone number and a shared tablet."""
    rng = u.rng
    branch_idx = rng.randrange(len(BRANCHES))
    city = BRANCHES[branch_idx][2]
    addr = (f"{rng.randint(1, 450)}, {rng.choice(STREETS)}", city, f"{rng.randint(110001, 855999)}")
    shared_mobile = _mobile(rng) if rng.random() < 0.5 else None
    shared_device = _device(rng) if rng.random() < 0.6 else None
    home_ip = f"{rng.randint(27, 223)}.{rng.randint(0, 255)}.{rng.randint(0, 255)}"
    n = rng.randint(2, 4)
    roles = ["SALARIED" if rng.random() < 0.6 else "SELF_EMPLOYED", "HOMEMAKER", "STUDENT", "PENSIONER"]
    members = []
    for j in range(n):
        occ = roles[j]
        variant = addr[0].upper() if j % 2 else addr[0].replace(",", " ,").lower()
        kw = dict(address=(variant, addr[1], addr[2]), name=f"{rng.choice(FIRST)} {LAST[u.idx % len(LAST)]}")
        if shared_mobile and j in (0, n - 1):
            kw["mobile"] = shared_mobile
        c = _legit_customer(u, occ, branch_idx, **kw)
        accts = _open_accounts(u, c)
        _legit_sessions(u, c, accts, home_ip, shared_device)
        for a in accts:
            _legit_txns(u, c, a)
        members.append((c, accts[0]))
    head = members[0][1]
    for c, a in members[1:]:
        start = max(head["open_date"], a["open_date"], HISTORY_START)
        for y, m in _months(start, WORLD_END):
            if rng.random() < 0.7:
                d = date(y, m, rng.randint(1, min(10, _days_in(y, m))))
                if d >= start:
                    u.transfer(head, a, _ts(d, rng.randint(8, 21), rng), _round_amount(rng, 1000, 15000), "family")


# ------------------------------------------------------------------ mule rings

def simulate_ring(u: Unit) -> None:
    rng = u.rng
    pattern = _pick(rng, [("RECRUITED", 0.4), ("RENTED", 0.35), ("SYNTHETIC", 0.25)])
    start = _rand_date(rng, date(2025, 5, 15), WORLD_END - timedelta(days=40))
    n = min(25, 3 + int(rng.expovariate(1 / 6.0)))
    ctrl_devices = [_device(rng) for _ in range(rng.randint(1, 3))]
    ctrl_mobiles = [_mobile(rng) for _ in range(rng.randint(1, 2))]
    ctrl_ip = f"{rng.randint(27, 223)}.{rng.randint(0, 255)}.{rng.randint(0, 255)}"
    beneficiaries = [_ext_vpa(rng) for _ in range(rng.randint(2, 5))]
    if rng.random() < 0.3:
        beneficiaries.append(rng.choice(EXCHANGE_VPAS))
    branches = [rng.randrange(len(BRANCHES)) for _ in range(2)]
    scam = rng.choice(SCAMS)
    two_tier = n >= 5 and rng.random() < 0.35

    members = []   # (customer, account, active_from, active_to)
    for j in range(n):
        if pattern == "RECRUITED":
            occ = rng.choice(["STUDENT", "HOMEMAKER", "FARMER", "SALARIED", "UNEMPLOYED"])
            onboard = _rand_date(rng, date(2012, 1, 1), date(2022, 6, 30))
            c = u.customer(occupation=occ, branch_idx=rng.randrange(len(BRANCHES)), onboarding_date=onboard,
                           kyc_type="FULL_KYC", onboarding_channel="BRANCH", digital=rng.random() < 0.6)
            a = u.account(c, 0, "SB_REGULAR", onboard, role="mule")
            a["dormant_from"] = _rand_date(rng, onboard + timedelta(days=400), HISTORY_START - timedelta(days=30)) \
                if onboard + timedelta(days=400) < HISTORY_START - timedelta(days=30) else HISTORY_START - timedelta(days=30)
            a["reactivated_on"] = start + timedelta(days=rng.randint(0, 40))
            act = a["reactivated_on"] + timedelta(days=rng.randint(7, 45))
        else:
            occ = rng.choice(["STUDENT", "UNEMPLOYED", "HOMEMAKER", "GIG_WORKER"])
            onboard = start + timedelta(days=rng.randint(-60, 10))
            kyc = "MIN_KYC" if rng.random() < (0.65 if pattern == "RENTED" else 0.5) else "VIDEO_KYC"
            kw = {}
            if pattern == "SYNTHETIC" and members:
                other = rng.choice(members)[0]
                reuse = _pick(rng, [("pan", 0.5), ("mobile", 0.3), ("address", 0.2)])
                kw[reuse] = other[reuse]
            elif rng.random() < 0.4:
                kw["mobile"] = rng.choice(ctrl_mobiles)
            c = u.customer(occupation=occ, branch_idx=rng.choice(branches), onboarding_date=onboard, kyc_type=kyc,
                           onboarding_channel=rng.choice(["DIGITAL", "BC_AGENT"]) if kyc == "MIN_KYC" else "DIGITAL",
                           digital=True, **kw)
            a = u.account(c, 0, "SB_BSBD" if kyc == "MIN_KYC" else "SB_REGULAR", onboard, role="mule")
            act = onboard + timedelta(days=rng.randint(10, 60))
        members.append((c, a, act, act + timedelta(days=rng.randint(20, 100))))

    # Digital footprint: device binds from the controller's phones, mobile / VPA changes.
    for c, a, act, end in members:
        city = BRANCHES[c["branch_idx"]][2]
        own_dev = _device(rng)
        if a["open_date"] >= HISTORY_START:
            dev0 = f"bc-agent-{rng.randrange(N_BC_DEVICES):03d}" if c["onboarding_channel"] == "BC_AGENT" else own_dev
            u.session(c["cif"], _ts(a["open_date"], rng.randint(10, 18), rng), dev0, _ip(rng), city, "VPA_CREATE",
                      a["vpa"], a["account_id"], channel="BC_AGENT_APP" if dev0.startswith("bc-") else "MOBILE_APP")
        handed_over = pattern != "RECRUITED" or rng.random() < 0.6
        dev = rng.choice(ctrl_devices) if handed_over else own_dev
        pre = act - timedelta(days=rng.randint(0, 3))
        u.session(c["cif"], _ts(pre, _day_hour(rng, 0.3), rng), dev, _ip(rng, ctrl_ip if handed_over else None), city,
                  "DEVICE_BIND")
        if handed_over and rng.random() < 0.45:
            new_mobile = rng.choice(ctrl_mobiles)
            c["mobile_changes"] = [(pre, new_mobile)]
            u.session(c["cif"], _ts(pre, _day_hour(rng, 0.3), rng), dev, _ip(rng, ctrl_ip), city, "MOBILE_CHANGE",
                      _fmt_mobile(new_mobile, rng))
        if rng.random() < 0.5:
            new_vpa = f"{c['customer_name'].split()[0].lower()}{rng.randint(100, 99999)}@{BANK_HANDLE}"
            u.session(c["cif"], _ts(pre, _day_hour(rng, 0.3), rng), dev, _ip(rng, ctrl_ip), city, "VPA_CREATE",
                      new_vpa, a["account_id"])
            a["vpa"] = new_vpa
        d = pre
        while d <= end:
            for _ in range(rng.randint(0, 3)):
                u.session(c["cif"], _ts(d, _day_hour(rng, 0.3), rng), rng.choice(ctrl_devices) if handed_over else dev,
                          _ip(rng, ctrl_ip if handed_over else None), city)
            d += timedelta(days=1)
        # Seasoning before activation: small top-ups and spends so the account looks used.
        warm_from = a["reactivated_on"] or a["open_date"]
        for _ in range(max(1, (act - warm_from).days // 5)):
            ts = _ts(_rand_date(rng, warm_from, act), rng.randint(9, 21), rng)
            if rng.random() < 0.4:
                u.txn(a, ts, "CR", _round_amount(rng, 500, 3000), rng.choice(beneficiaries + [_ext_vpa(rng)]))
            else:
                u.txn(a, ts, "DR", _lognormal(rng, 250, 0.6, 20, 2000), f"m{rng.randrange(N_MERCHANTS):04d}.store@ybl")

    tier1 = members[: max(2, n // 2)] if two_tier else members
    tier2 = members[max(2, n // 2):] if two_tier else []

    # Victims: each pays one to three tier-1 members active on the day, in tranches.
    ring_from = min(m[2] for m in tier1)
    ring_to = max(m[3] for m in tier1)
    victims = []
    d = ring_from
    while d <= ring_to:
        for _ in range(_poisson(rng, 0.5 * sum(1 for m in tier1 if m[2] <= d <= m[3]))):
            victims.append((d, _ext_vpa(rng)))
        d += timedelta(days=1)

    payments = {}   # victim vpa -> list of (member index, ts, amount, utr)
    for d, vpa in victims:
        active = [i for i, m in enumerate(tier1) if m[2] <= d <= m[3]]
        if not active:
            continue
        total = _round_amount(rng, 2000, 50000) if rng.random() < 0.55 else _lognormal(rng, 12000, 1.0, 999, 300000)
        k = min(len(active), _pick(rng, [(1, 0.55), (2, 0.3), (3, 0.15)]))
        for i in rng.sample(active, k):
            ts = _ts(d + timedelta(days=rng.randint(0, 1)), _day_hour(rng, 0.3), rng)
            amt = total if k == 1 else round(total * rng.uniform(0.3, 0.7), -2 if rng.random() < 0.5 else 0)
            payments.setdefault(vpa, []).append((i, ts, amt))

    mule_credit = []   # (member, ts, amount) for forwarding
    for vpa, pays in payments.items():
        for i, ts, amt in pays:
            c, a, act, end = tier1[i]
            utr = u.txn(a, ts, "CR", amt, vpa, remarks=rng.choice(["", "task reward", "investment", "refund", "order"]))
            pays[pays.index((i, ts, amt))] = (i, ts, amt, utr)
            mule_credit.append((tier1[i], ts, amt))

    def forward(member, ts, amt, depth):
        c, a, act, end = member
        hold = min(72.0, max(0.1, rng.lognormvariate(math.log(2.5), 0.9)))
        out_ts = ts + timedelta(hours=hold)
        out_amt = round(amt * rng.uniform(0.9, 0.99), 2)
        if depth == 0 and tier2:
            nxt = rng.choice(tier2)
            u.transfer(a, nxt[1], out_ts, out_amt)
            forward(nxt, out_ts, out_amt, 1)
        else:
            parts = 1 if rng.random() < 0.7 else 2
            for _ in range(parts):
                u.txn(a, out_ts + timedelta(minutes=rng.randint(0, 50)), "DR", round(out_amt / parts, 2),
                      rng.choice(beneficiaries), channel=rng.choice(["UPI", "UPI", "IMPS"]))

    for member, ts, amt in mule_credit:
        forward(member, ts, amt, 0)

    # Complaints: ~40% of victims complain, often weeks later; the account that got the most is named and frozen.
    first_freeze = None
    by_acct = {m[1]["account_id"]: m for m in members}
    for vpa, pays in payments.items():
        if rng.random() >= 0.4:
            continue
        i, ts, amt, utr = max(pays, key=lambda p: p[2])
        c, a, act, end = tier1[i]
        rdate = max(p[1] for p in pays).date() + timedelta(days=int(min(60, rng.lognormvariate(math.log(10), 0.9))))
        via_vpa = rng.random() < 0.6
        u.report(rdate, rng.choice(["NCRP_1930", "NCRP_1930", "NCRP_PORTAL"]),
                 account_id=None if via_vpa else a["account_id"], vpa=a["vpa"] if via_vpa else None,
                 complainant_vpa=vpa, amount=sum(p[2] for p in pays), utr=utr, category=scam)
        fdate = rdate + timedelta(days=rng.randint(0, 4))
        if a["freeze_date"] is None or fdate < a["freeze_date"]:
            a["freeze_date"], a["freeze_reason"] = fdate, "CYBER_COMPLAINT"
        first_freeze = fdate if first_freeze is None else min(first_freeze, fdate)

    # Linked intelligence after the first freeze: Suspect Registry matches, internal FRM reviews.
    if first_freeze is not None:
        for c, a, act, end in members:
            if a["freeze_date"] is not None and a["freeze_date"] <= first_freeze + timedelta(days=5):
                continue
            x = rng.random()
            if x < 0.45:
                rdate = first_freeze + timedelta(days=rng.randint(7, 60))
                how = _pick(rng, [("mobile", 0.4), ("vpa", 0.3), ("account", 0.3)])
                mobile = (c.get("mobile_changes") or [(None, c["mobile"])])[-1][1]
                u.report(rdate, "SUSPECT_REGISTRY", account_id=a["account_id"] if how == "account" else None,
                         vpa=a["vpa"] if how == "vpa" else None, mobile=_fmt_mobile(mobile, rng) if how == "mobile" else None,
                         category=scam, remarks="I4C suspect registry match")
                reason = "SUSPECT_REGISTRY"
            elif x < 0.6:
                rdate = first_freeze + timedelta(days=rng.randint(3, 30))
                u.report(rdate, "INTERNAL_FRM", account_id=a["account_id"], category=scam,
                         remarks="linked to frozen account")
                reason = "INTERNAL_FRM"
            else:
                continue
            fdate = rdate + timedelta(days=rng.randint(0, 5))
            if a["freeze_date"] is None or fdate < a["freeze_date"]:
                a["freeze_date"], a["freeze_reason"] = fdate, reason

    for c, a, act, end in members:
        c["mule_pattern"] = pattern
        a["mule_pattern"] = pattern
    del by_acct


def unit_kind(idx: int, seed: int) -> str:
    x = random.Random(seed * 7_919 + idx * 104_729 + 17).random()
    if x < RING_UNIT_RATE:
        return "RING"
    if x < RING_UNIT_RATE + HOUSEHOLD_UNIT_RATE:
        return "HOUSEHOLD"
    return "SINGLE"


def simulate_unit(idx: int, seed: int) -> Unit:
    """Everything one unit produces up to WORLD_END. Pure Python, so it can be unit tested without Spark."""
    u = Unit(idx, random.Random(seed * 1_000_003 + idx))
    {"RING": simulate_ring, "HOUSEHOLD": simulate_household, "SINGLE": simulate_single}[unit_kind(idx, seed)](u)
    # A frozen account cannot move money: drop its later transactions.
    frozen = {a["account_id"]: a["freeze_date"] for a in u.accounts if a["freeze_date"]}
    if frozen:
        u.txns = [t for t in u.txns if not (t[3] in frozen and t[2].date() > frozen[t[3]])]
    return u


# ------------------------------------------------------------------ Spark

def _schema():
    from pyspark.sql.types import (ArrayType, DateType, DoubleType, StringType, StructField, StructType,
                                   TimestampType)
    s, d, dbl, ts = StringType(), DateType(), DoubleType(), TimestampType()
    cust = StructType([StructField(n, t) for n, t in [
        ("cif", s), ("customer_name", s), ("dob", d), ("gender", s), ("pan", s), ("aadhaar_ref", s), ("mobile", s),
        ("email", s), ("address_line", s), ("city", s), ("pincode", s), ("kyc_type", s), ("onboarding_channel", s),
        ("onboarding_date", d), ("home_branch", s), ("occupation", s), ("declared_annual_income", dbl),
        ("mule_pattern", s)]])
    acct = StructType([StructField(n, t) for n, t in [
        ("account_id", s), ("cif", s), ("product_code", s), ("branch_code", s), ("open_date", d),
        ("dormant_from", d), ("reactivated_on", d), ("freeze_date", d), ("freeze_reason", s), ("mule_pattern", s)]])
    txn = StructType([StructField(n, t) for n, t in [
        ("txn_id", s), ("utr", s), ("txn_ts", ts), ("account_id", s), ("direction", s), ("amount", dbl),
        ("payer_vpa", s), ("payee_vpa", s), ("counterparty_account_id", s), ("counterparty_ifsc", s),
        ("channel", s), ("remarks", s)]])
    sess = StructType([StructField(n, t) for n, t in [
        ("session_id", s), ("cif", s), ("session_ts", ts), ("channel", s), ("device_id", s), ("ip_address", s),
        ("geo_city", s), ("event_type", s), ("new_value", s), ("account_id", s)]])
    rep = StructType([StructField(n, t) for n, t in [
        ("report_id", s), ("report_ts", ts), ("source", s), ("reported_account_id", s), ("reported_vpa", s),
        ("reported_mobile", s), ("complainant_vpa", s), ("amount", dbl), ("txn_utr", s), ("category", s),
        ("remarks", s)]])
    return StructType([StructField("customers", ArrayType(cust)), StructField("accounts", ArrayType(acct)),
                       StructField("txns", ArrayType(txn)), StructField("sessions", ArrayType(sess)),
                       StructField("reports", ArrayType(rep))])


def unit_rows(u: Unit) -> tuple:
    customers = [(c["cif"], c["customer_name"], c["dob"], c["gender"], c["pan"], c["aadhaar_ref"],
                  _fmt_mobile(c["mobile"], u.rng), c["email"], c["address"][0], c["address"][1], c["address"][2],
                  c["kyc_type"], c["onboarding_channel"], c["onboarding_date"], BRANCHES[c["branch_idx"]][0],
                  c["occupation"], c["declared_income"], c.get("mule_pattern")) for c in u.customers]
    accounts = [(a["account_id"], a["cif"], a["product_code"], a["branch_code"], a["open_date"], a["dormant_from"],
                 a["reactivated_on"], a["freeze_date"], a["freeze_reason"], a.get("mule_pattern"))
                for a in u.accounts]
    return customers, accounts, u.txns, u.sessions, u.reports


def _simulate_partition(seed: int):
    def run(rows):
        for row in rows:
            yield unit_rows(simulate_unit(int(row.id), seed))
    return run


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("--as-of", default=None, help="last business date in the extracts, YYYY-MM-DD (default: yesterday UTC)")
    p.add_argument("--db-prefix", default=DEFAULT_DB_PREFIX)
    p.add_argument("--customers", type=int, default=DEFAULT_CUSTOMERS, help="approximate customers in the world")
    p.add_argument("--seed", type=int, default=DEFAULT_SEED)
    p.add_argument("--inject-bad-data", action="store_true", help="add bad rows so validate_bronze fails")
    args, _ = p.parse_known_args(argv)
    return args


def resolve_as_of(value: str | None) -> date:
    if value:
        return datetime.strptime(value, "%Y-%m-%d").date()
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def _with_dupes(df, *keys):
    from pyspark.sql import functions as F
    dupes = df.where((F.abs(F.xxhash64(*keys)) % 1_000_000) < DUPLICATE_RATE * 1_000_000)
    return df.unionByName(dupes.withColumn("ingested_at", F.col("ingested_at") + F.expr("INTERVAL 1 SECOND")))


def _write(df, name: str, partition_col=None) -> None:
    w = df.writeTo(name).using("iceberg").tableProperty("format-version", "2")
    if partition_col is not None:
        w = w.partitionedBy(partition_col)
    w.createOrReplace()


def run(spark, args: argparse.Namespace) -> None:
    from pyspark.sql import functions as F

    as_of = resolve_as_of(args.as_of)
    bronze_db, ref_db = f"{args.db_prefix}_bronze", f"{args.db_prefix}_ref"
    n_units = max(10, int(args.customers / 1.1))
    logger.info("Simulating %d units (~%d customers), events %s -> %s (world to %s), seed %d",
                n_units, args.customers, HISTORY_START, as_of, WORLD_END, args.seed)

    parts = max(8, n_units // 4000)
    sim = spark.range(n_units, numPartitions=parts).rdd.mapPartitions(_simulate_partition(args.seed))
    world = spark.createDataFrame(sim, _schema()).cache()
    cut = F.lit(as_of).cast("date")
    end_of_day = F.to_timestamp(F.lit(f"{as_of.isoformat()} 23:59:59"))
    batch = [cut.alias("batch_as_of"),
             F.lit(datetime.now(timezone.utc).replace(tzinfo=None)).cast("timestamp").alias("ingested_at")]
    after = lambda c: F.when(F.col(c) <= cut, F.col(c))  # noqa: E731

    kyc = (world.select(F.explode("customers").alias("c")).select("c.*")
           .where(F.col("onboarding_date") <= cut).drop("mule_pattern").select("*", *batch))
    acct = (world.select(F.explode("accounts").alias("a")).select("a.*")
            .where(F.col("open_date") <= cut)
            .withColumn("reactivated_on", after("reactivated_on"))
            .withColumn("freeze_reason", F.when(F.col("freeze_date") <= cut, F.col("freeze_reason")))
            .withColumn("freeze_date", after("freeze_date"))
            .withColumn("status", F.when(F.col("freeze_date").isNotNull(), "FROZEN")
                        .when(F.col("dormant_from").isNotNull() & F.col("reactivated_on").isNull(), "DORMANT")
                        .otherwise("ACTIVE"))
            .select("account_id", "cif", "product_code", "branch_code", "open_date", "status", "dormant_from",
                    "reactivated_on", "freeze_date", "freeze_reason", *batch))
    txn = (world.select(F.explode("txns").alias("t")).select("t.*")
           .where((F.col("txn_ts") <= end_of_day) & (F.to_date("txn_ts") >= F.lit(HISTORY_START)))
           .select("*", *batch))
    sess = (world.select(F.explode("sessions").alias("s")).select("s.*")
            .where((F.col("session_ts") <= end_of_day) & (F.to_date("session_ts") >= F.lit(HISTORY_START)))
            .select("*", *batch))
    rep = (world.select(F.explode("reports").alias("r")).select("r.*")
           .where(F.col("report_ts") <= end_of_day).select("*", *batch))

    if args.inject_bad_data:
        logger.warning("--inject-bad-data: adding orphan transactions, null keys and a raw PAN in remarks")
        bad = txn.limit(2000)
        txn = (txn.unionByName(bad.withColumn("account_id", F.lit("SB-9999999999")))
               .unionByName(bad.limit(50).withColumn("txn_id", F.lit(None).cast("string")))
               .unionByName(bad.limit(5).withColumn("remarks", F.lit("refund to ABCPK1234Z"))))

    branches = spark.createDataFrame(BRANCHES, "branch_code string, branch_name string, city string, region string, "
                                               "state string")
    products = spark.createDataFrame([(k, v[0], v[1]) for k, v in PRODUCTS.items()],
                                     "product_code string, product_name string, account_prefix string")
    bank_devices = spark.createDataFrame(
        [(f"kiosk-{b[0]}-{k}", "BRANCH_KIOSK", b[0]) for b in BRANCHES for k in (1, 2)]
        + [(f"bc-agent-{k:03d}", "BC_AGENT", None) for k in range(N_BC_DEVICES)],
        "device_id string, device_type string, branch_code string")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_db}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ref_db}")
    _write(branches.withColumn("ingested_at", F.current_timestamp()), f"{ref_db}.branch_map")
    _write(products.withColumn("ingested_at", F.current_timestamp()), f"{ref_db}.product_map")
    _write(bank_devices.withColumn("ingested_at", F.current_timestamp()), f"{ref_db}.bank_devices")
    _write(_with_dupes(kyc, "cif"), f"{bronze_db}.kyc_onboarding")
    _write(acct, f"{bronze_db}.cbs_accounts")
    _write(_with_dupes(txn, "txn_id"), f"{bronze_db}.upi_transactions", F.months("txn_ts"))
    _write(_with_dupes(sess, "session_id"), f"{bronze_db}.digital_sessions", F.months("session_ts"))
    _write(rep, f"{bronze_db}.fraud_reports")
    world.unpersist()

    for t in ("kyc_onboarding", "cbs_accounts", "upi_transactions", "digital_sessions", "fraud_reports"):
        logger.info("%s.%s: %d rows", bronze_db, t, spark.table(f"{bronze_db}.{t}").count())
    logger.info("Bronze load as of %s complete", as_of)


def main(argv=None, spark=None) -> None:
    from pyspark.sql import SparkSession

    args = parse_args(argv)
    own = spark is None
    spark = spark or SparkSession.builder.appName("mule-generate-bronze").getOrCreate()
    try:
        run(spark, args)
    finally:
        if own:
            spark.stop()


if __name__ == "__main__":
    main()

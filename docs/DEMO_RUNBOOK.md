# Demo runbook (about 12 minutes)

Before the demo (30 minutes ahead):

- CDE Job Runs: today's 02:00 IST DAG run succeeded (all six tasks green,
  including `validate_bronze`).
- App Lineage tab: today's run with `triggered_by = airflow`, plus 4+
  backfilled run dates (`cai/jobs/backfill_history.py --weeks 8`).
- Model `mule-scorer` restarted after today's run; its Test tab shows today's
  `run_date` and the demo account's what-if (see step 5) scores as expected.
- Open the app and run one Hue query 5 minutes before: the Impala virtual
  warehouse auto-suspends and the first query after a pause can take minutes.
- Hue open on `sql/reports.sql`; the Airflow UI open on the DAG grid.
- Showing CDE live? Trigger the DAG 25 minutes before (a cold vcluster needs
  a few minutes to scale up).

## 1. The question (1 min)

Fraudsters recruit or rent bank accounts to receive and forward stolen money
before a victim's complaint catches up. At well under 0.1% of the book, an
investigation team can't review every account — which ones get a debit
freeze today, which get held and watched, and which just go on a watchlist?

## 2. The pipeline (2 min): CDE Airflow UI

- DAG `mule_account_identifier_pipeline`, daily: KYC / core banking / UPI /
  digital session / fraud-report extracts → `validate_bronze` (the hard gate:
  null keys, orphans, a raw PAN outside its governed column, missing days) →
  silver (dedupe, salted-hash keys, identity edges) → identity graph
  (point-in-time person clusters and money-flow rings, hub identifiers like
  branch kiosks excluded) → gold features (Iceberg MERGE) → CAI scoring job.
- A `validate_bronze` failure stops the DAG before silver: yesterday's alert
  queue stands, nothing bad reaches an investigator.
- The gold table has one row per active account per weekly snapshot, 18
  numeric features, and the label "confirmed mule within 90 days" fills in as
  it matures: each load is one Iceberg snapshot.

## 3. Alert queue (2 min): app, first tab

- Accounts scored, alert count and share of the book, rings with an alert,
  the measured book mule rate (real run: 20,133 accounts scored, 604 alerts —
  40 T1 / 161 T2 / 403 T3 — across 164 rings, book mule rate 0.065%).
  "Alerts by tier" bar chart and a filterable, rank-ordered queue.
- Point at the reasons column ("2 hop(s) from a reported mule; minimum-KYC
  (OTP) account"): business rules on the inputs, what the investigator opens
  the case with — not an explanation of the model's score.

## 4. Linked identities and ring view (2 min): app, next two tabs

- Pick a T1 account in Linked identities: the graph shows every other
  customer sharing a non-hub device, mobile, PAN hash or address hash, with
  whether they're alerted this run too.
- Switch to Ring view for the same account's ring: identity links plus
  own-bank money-flow edges (transfers, shared counterparties), point in
  time as of this run's snapshot. This is the evidence behind "ring of
  3-25 accounts" in the synthetic design — a rented min-KYC account usually
  sits one hop from an already-frozen one.

## 5. Can we trust it? (2 min): Holdout & trust tab

- The talking point: "the top 1% of the book catches X% of the mules that
  actually matured, against rules alone" (real run: **100% capture at the
  top 1%, vs. 54% for rules alone — a 1.85x lift**; precision at the T1
  queue (top 0.2%) is 16.7%, holdout AUC 1.000).
- Why the gate reads capture/precision/lift instead of accuracy: at well
  under 0.1% mule rate, a model that flags nobody is "99.9% accurate."
- The holdout is honest: context ends 90 days before the test weeks, as if
  the model had been deployed then.

## 6. Live what-if (2 min): What-if tab

- Pick an account near the T2/T1 boundary. Score it as is, then simulate a
  new device login, a mobile/VPA change, and pass-through jumping to 92%:
  the tier moves from **T2 (hold + monitor) to T1 (freeze review)** live,
  scored by the CAI model endpoint (or in-app if the endpoint isn't
  configured — the tab says which).
- Mitra-v2 / TabICL have no training step: `fit` stores the labelled context
  and learning happens at prediction time. Restarting the model after a new
  daily run is how it picks up fresh context, not a retraining job.

## 7. Close the loop (1 min): Decisions tab

- Record `CONFIRMED_MULE` (or `FALSE_POSITIVE`) with a checker and a note.
  It's maker-checker: nothing here freezes an account by itself. It lands in
  `bronze.investigator_decisions`; tomorrow's silver load turns a
  `CONFIRMED_MULE` into a label (and a known mule for the graph) with no
  retraining cycle.

## 8. Audit and time travel (1 min): Lineage tab + Hue

- Lineage table: model family/checkpoint, context window, and the Iceberg
  snapshot id of gold each run read.
- In Hue: `DESCRIBE HISTORY` on `mule_features`, then query 8 in
  `sql/reports.sql` with `FOR SYSTEM_VERSION AS OF <source_snapshot_id>` to
  rebuild the exact context a past run learned from. Query 6 checks whether
  a past run's alerts held up against labels that have since matured.

## Honest framing (close)

Synthetic data, demo pipeline, not a validated fraud model. It ranks
accounts for human review; it never freezes one on its own — every tier is
a recommended action that goes through the maker-checker decision flow. The
context holds account and identity rows, so it stays inside the governed
project with the same masking policies as the gold table.

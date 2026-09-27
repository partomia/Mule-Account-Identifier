"""
Stage 4 - Identity graph (entity resolution, point in time)

Each customer (CIF) is a node. For every weekly snapshot date d (Fridays from
FIRST_SNAPSHOT, plus the as-of date) the graph holds only what the bank knew
on d:

  identity edges   two CIFs share a non-hub device, mobile, PAN hash or address
                   hash, first held by both on or before d (silver.identity_edges)
  money edges      in the 90 days up to d: an own-bank transfer between the two,
                   or at least --min-shared counterparties in common (the same
                   victims paying both, the same beneficiaries receiving from
                   both). Counterparties used by more than --max-hub-cifs
                   customers (merchants, employers, gig platforms, exchanges) are
                   ignored, as are counterparties only one customer ever used.

Connected components by min-label propagation (no GraphFrames package in the
CDE python-env), run once over (snapshot_date, cif) nodes so every snapshot's
graph is solved in the same pass:

  person_cluster_id / size   identity edges only: likely the same controller
  ring_id / ring_size        identity + money edges: the wider money-flow ring

hops_to_known_mule: graph distance (identity + money edges) from a customer
with an account already reported (1930 / NCRP, Suspect Registry, FRM), frozen
for fraud or confirmed by an investigator on or before d. 1, 2, or 3 = 3+ or
not connected. Reports after d are never used, so the holdout stays honest.

accounts_on_same_device / cifs_sharing_mobile: for shared non-hub devices and
mobiles, the accounts / customers behind them on d.

Only CIFs with a link, a shared identifier or a known mule within two hops get
a row; the gold job fills defaults (cluster of one, 3 hops) for the rest.

Reads:
  <prefix>_silver.{account, txn, report, decision, identity_links, identity_edges, identity_hubs}
Writes (drop + recreate every run):
  <prefix>_silver.graph_edges        snapshot_date x CIF pair x link type (for the app's ring view)
  <prefix>_silver.identity_clusters  snapshot_date x CIF

Usage:
  spark-submit build_identity_graph.py [--db-prefix P] [--max-hub-cifs N] [--min-shared N]
"""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_DB_PREFIX = "rsingh_mule_acct"
FIRST_SNAPSHOT = date(2025, 5, 2)    # a Friday, one month after the extracts start
MONEY_WINDOW_DAYS = 90
MAX_HUB_CIFS = 25
MIN_SHARED = 2
MAX_ITER = 30
FRAUD_FREEZES = ("CYBER_COMPLAINT", "SUSPECT_REGISTRY", "INTERNAL_FRM")


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-prefix", default=DEFAULT_DB_PREFIX)
    p.add_argument("--max-hub-cifs", type=int, default=MAX_HUB_CIFS)
    p.add_argument("--min-shared", type=int, default=MIN_SHARED)
    args, _ = p.parse_known_args(argv)
    return args


def snapshot_dates(as_of: date) -> list[date]:
    out, d = [], FIRST_SNAPSHOT
    while d <= as_of:
        out.append(d)
        d += timedelta(days=7)
    if not out or out[-1] != as_of:
        out.append(as_of)
    return out


def in_window(date_col: str, snaps: list[date], days: int):
    """Array of the snapshot dates d with date_col in (d - days, d]."""
    arr = F.array(*[F.lit(d) for d in snaps])
    t = F.col(date_col)
    return F.filter(arr, lambda s: (s >= t) & (s < F.date_add(t, days)))


def from_date(date_col: str, snaps: list[date]):
    """Array of the snapshot dates on or after date_col."""
    arr = F.array(*[F.lit(d) for d in snaps])
    return F.filter(arr, lambda s: s >= F.col(date_col))


def connected_components(edges: DataFrame, max_iter: int = MAX_ITER) -> DataFrame:
    """edges: (snapshot_date, src, dst), undirected. Returns (snapshot_date, cif, comp) with
    comp = the smallest CIF reachable within the same snapshot's graph."""
    und = (edges.select("snapshot_date", "src", "dst")
           .unionByName(edges.select("snapshot_date", F.col("dst").alias("src"), F.col("src").alias("dst")))
           .distinct().localCheckpoint())
    labels = (und.groupBy("snapshot_date", F.col("src").alias("cif"))
              .agg(F.least(F.col("src"), F.min("dst")).alias("comp")).localCheckpoint())
    for i in range(max_iter):
        prop = (und.join(labels.withColumnRenamed("cif", "src"), ["snapshot_date", "src"])
                .groupBy("snapshot_date", F.col("dst").alias("cif")).agg(F.min("comp").alias("cand")))
        new = (labels.join(prop, ["snapshot_date", "cif"], "left")
               .select("snapshot_date", "cif", F.least("comp", F.coalesce("cand", "comp")).alias("comp"))
               .localCheckpoint())
        changed = new.join(labels.withColumnRenamed("comp", "old"), ["snapshot_date", "cif"]).where("comp <> old").count()
        labels = new
        logger.info("  label propagation iteration %d: %d labels changed", i + 1, changed)
        if changed == 0:
            break
    else:
        logger.warning("connected components did not converge in %d iterations", max_iter)
    return labels


def build(spark: SparkSession, args: argparse.Namespace) -> tuple[DataFrame, DataFrame, date, list[date]]:
    s = f"{args.db_prefix}_silver"
    account = spark.table(f"{s}.account")
    as_of = account.agg(F.max("batch_as_of")).first()[0]
    snaps = snapshot_dates(as_of)
    logger.info("as of %s: %d snapshot dates %s -> %s", as_of, len(snaps), snaps[0], snaps[-1])

    # Identity edges known on each snapshot date.
    ident = (spark.table(f"{s}.identity_edges")
             .withColumn("snapshot_date", F.explode(from_date("first_seen", snaps)))
             .select("snapshot_date", F.col("src_cif").alias("src"), F.col("dst_cif").alias("dst"), "link_type",
                     F.lit(1).alias("weight")))

    # Money edges in the 90 days up to each snapshot.
    txn = spark.table(f"{s}.txn")
    acct_cif = account.select(F.col("account_id").alias("counterparty_account_id"), F.col("cif").alias("cpty_cif"))
    transfers = (txn.where(F.col("counterparty_account_id").isNotNull()).join(acct_cif, "counterparty_account_id")
                 .where(F.col("cif") != F.col("cpty_cif"))
                 .select(F.least("cif", "cpty_cif").alias("src"), F.greatest("cif", "cpty_cif").alias("dst"),
                         "txn_date").distinct()
                 .withColumn("snapshot_date", F.explode(in_window("txn_date", snaps, MONEY_WINDOW_DAYS)))
                 .groupBy("snapshot_date", "src", "dst").agg(F.count("*").alias("weight"))
                 .withColumn("link_type", F.lit("TRANSFER")))
    ext = txn.where(F.col("counterparty_account_id").isNull() & F.col("counterparty_vpa").isNotNull())
    cpty_cifs = ext.groupBy("counterparty_vpa").agg(F.countDistinct("cif").alias("n"))
    usable = cpty_cifs.where(F.col("n").between(2, args.max_hub_cifs)).select("counterparty_vpa")
    touches = (ext.join(usable, "counterparty_vpa").select("counterparty_vpa", "cif", "txn_date").distinct()
               .withColumn("snapshot_date", F.explode(in_window("txn_date", snaps, MONEY_WINDOW_DAYS)))
               .select("snapshot_date", "counterparty_vpa", "cif").distinct())
    a, b = touches.alias("a"), touches.alias("b")
    shared = (a.join(b, (F.col("a.snapshot_date") == F.col("b.snapshot_date"))
                     & (F.col("a.counterparty_vpa") == F.col("b.counterparty_vpa")) & (F.col("a.cif") < F.col("b.cif")))
              .groupBy(F.col("a.snapshot_date").alias("snapshot_date"), F.col("a.cif").alias("src"),
                       F.col("b.cif").alias("dst"))
              .agg(F.count("*").alias("weight"))
              .where(F.col("weight") >= args.min_shared)
              .withColumn("link_type", F.lit("SHARED_COUNTERPARTY")))
    edges = (ident.unionByName(transfers.select(*ident.columns)).unionByName(shared.select(*ident.columns))
             .localCheckpoint())

    person = connected_components(edges.where(F.col("link_type").isin("DEVICE", "MOBILE", "PAN", "ADDRESS")))
    ring = connected_components(edges)
    sizes = lambda df, name: (df.groupBy("snapshot_date", "comp").agg(F.count("*").alias(f"{name}_size"))  # noqa: E731
                              .withColumnRenamed("comp", f"{name}_id"))
    person = (person.withColumnRenamed("comp", "person_cluster_id")
              .join(sizes(person, "person_cluster"), ["snapshot_date", "person_cluster_id"]))
    ring = ring.withColumnRenamed("comp", "ring_id").join(sizes(ring, "ring"), ["snapshot_date", "ring_id"])

    # Customers already known as mules on each snapshot date.
    report = spark.table(f"{s}.report").where(F.col("cif").isNotNull()).select("cif", F.col("report_date").alias("d"))
    frozen = account.where(F.col("freeze_reason").isin(*FRAUD_FREEZES)).select("cif", F.col("freeze_date").alias("d"))
    confirmed = (spark.table(f"{s}.decision").where(F.col("decision") == "CONFIRMED_MULE")
                 .select("cif", F.col("decision_date").alias("d")))
    known = (report.unionByName(frozen).unionByName(confirmed).groupBy("cif").agg(F.min("d").alias("d"))
             .withColumn("snapshot_date", F.explode(from_date("d", snaps))).select("snapshot_date", "cif"))
    und = (edges.select("snapshot_date", "src", "dst")
           .unionByName(edges.select("snapshot_date", F.col("dst").alias("src"), F.col("src").alias("dst"))).distinct())
    hop1 = (und.join(known.withColumnRenamed("cif", "src"), ["snapshot_date", "src"])
            .select("snapshot_date", F.col("dst").alias("cif")).distinct()
            .join(known, ["snapshot_date", "cif"], "left_anti"))
    hop2 = (und.join(hop1.withColumnRenamed("cif", "src"), ["snapshot_date", "src"])
            .select("snapshot_date", F.col("dst").alias("cif")).distinct()
            .join(known, ["snapshot_date", "cif"], "left_anti").join(hop1, ["snapshot_date", "cif"], "left_anti"))
    hops = (known.withColumn("hops_to_known_mule", F.lit(1))       # another account of the same customer is known
            .unionByName(hop1.withColumn("hops_to_known_mule", F.lit(1)))
            .unionByName(hop2.withColumn("hops_to_known_mule", F.lit(2))))

    # Shared devices and mobiles: accounts / customers behind the busiest one on each date.
    hubs = spark.table(f"{s}.identity_hubs").select("link_type", "identifier")
    links = spark.table(f"{s}.identity_links").join(hubs, ["link_type", "identifier"], "left_anti")
    multi = (links.groupBy("link_type", "identifier").agg(F.countDistinct("cif").alias("n"))
             .where(F.col("n") >= 2).select("link_type", "identifier"))
    shared_links = (links.join(multi, ["link_type", "identifier"])
                    .withColumn("snapshot_date", F.explode(from_date("first_seen", snaps))))
    accts = account.select("cif", "account_id", "open_date")
    device = (shared_links.where(F.col("link_type") == "DEVICE").join(accts, "cif")
              .where(F.col("open_date") <= F.col("snapshot_date"))
              .groupBy("snapshot_date", "identifier").agg(F.countDistinct("account_id").alias("n"),
                                                          F.collect_set("cif").alias("cifs"))
              .select("snapshot_date", "n", F.explode("cifs").alias("cif"))
              .groupBy("snapshot_date", "cif").agg(F.max("n").alias("accounts_on_same_device")))
    mobile = (shared_links.where(F.col("link_type") == "MOBILE")
              .groupBy("snapshot_date", "identifier").agg(F.collect_set("cif").alias("cifs"))
              .select("snapshot_date", F.size("cifs").alias("n"), F.explode("cifs").alias("cif"))
              .groupBy("snapshot_date", "cif").agg(F.max("n").alias("cifs_sharing_mobile")))

    keys = ["snapshot_date", "cif"]
    clusters = (ring.join(person, keys, "full").join(hops.groupBy(*keys).agg(F.min("hops_to_known_mule")
                                                                            .alias("hops_to_known_mule")), keys, "full")
                .join(device, keys, "full").join(mobile, keys, "full")
                .select("snapshot_date", "cif",
                        F.coalesce("person_cluster_id", "cif").alias("person_cluster_id"),
                        F.coalesce("person_cluster_size", F.lit(1)).cast("int").alias("person_cluster_size"),
                        F.coalesce("ring_id", "cif").alias("ring_id"),
                        F.coalesce("ring_size", F.lit(1)).cast("int").alias("ring_size"),
                        F.coalesce("hops_to_known_mule", F.lit(3)).cast("int").alias("hops_to_known_mule"),
                        F.col("accounts_on_same_device").cast("int"), F.col("cifs_sharing_mobile").cast("int")))
    graph_edges = edges.select("snapshot_date", F.col("src").alias("src_cif"), F.col("dst").alias("dst_cif"),
                               "link_type", F.col("weight").cast("int"))
    return graph_edges, clusters, as_of, snaps


def _write(df: DataFrame, name: str, partition_col=None) -> None:
    w = df.writeTo(name).using("iceberg").tableProperty("format-version", "2")
    if partition_col is not None:
        w = w.partitionedBy(partition_col)
    w.createOrReplace()


def main(argv=None, spark: SparkSession | None = None) -> None:
    args = parse_args(argv)
    own = spark is None
    spark = spark or SparkSession.builder.appName("mule-build-identity-graph").getOrCreate()
    try:
        s = f"{args.db_prefix}_silver"
        graph_edges, clusters, as_of, snaps = build(spark, args)
        _write(graph_edges, f"{s}.graph_edges", F.months("snapshot_date"))
        _write(clusters, f"{s}.identity_clusters", F.months("snapshot_date"))
        latest = spark.table(f"{s}.identity_clusters").where(F.col("snapshot_date") == F.lit(as_of))
        (spark.table(f"{s}.graph_edges").where(F.col("snapshot_date") == F.lit(as_of))
         .groupBy("link_type").count().orderBy("link_type").show())
        (latest.where("ring_size > 1").groupBy("ring_id").agg(F.max("ring_size").alias("ring_size"))
         .groupBy(F.when(F.col("ring_size") <= 4, "2-4").when(F.col("ring_size") <= 10, "5-10")
                  .when(F.col("ring_size") <= 25, "11-25").otherwise("26+").alias("ring_size_band"))
         .count().orderBy("ring_size_band").show())
        logger.info("graph_edges %d rows, identity_clusters %d rows (%d on %s)",
                    spark.table(f"{s}.graph_edges").count(), spark.table(f"{s}.identity_clusters").count(),
                    latest.count(), as_of)
    finally:
        if own:
            spark.stop()


if __name__ == "__main__":
    main()

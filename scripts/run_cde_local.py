#!/usr/bin/env python3
"""
Run the CDE Spark jobs on a laptop (or in CI) against a local Iceberg (Hadoop)
catalog, so the same job files can be tested without a vcluster. Optionally
exports the tables the CAI job and app read to parquet (parquet backend).

  python scripts/run_cde_local.py all --as-of 2026-09-25
  python scripts/run_cde_local.py all --as-of 2026-09-25 --customers 5000     # CI size
  python scripts/run_cde_local.py gold export
  python scripts/run_cde_local.py history

Stages: generate, validate, silver, graph, gold, export, all (= the five jobs + export),
history (list Iceberg snapshots of a gold table).
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
JOBS = ROOT / "cde" / "jobs"
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"
DEFAULT_PREFIX = "rsingh_mule_acct"
STAGES = {
    "generate": "generate_mule_bronze.py",
    "validate": "validate_bronze.py",
    "silver": "build_silver.py",
    "graph": "build_identity_graph.py",
    "gold": "build_gold_features.py",
}
# (layer, table) exported for the parquet backend: what the CAI job, endpoint and app read.
EXPORT_TABLES = [("gold", "mule_features"), ("silver", "customer"), ("silver", "account"),
                 ("silver", "identity_edges"), ("silver", "graph_edges")]


def local_spark(warehouse: Path, driver_memory: str = "6g"):
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    return (SparkSession.builder.appName("mule-local")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.jars.packages", ICEBERG_PACKAGE)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", str(warehouse))
            .config("spark.sql.defaultCatalog", "local")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.memory", driver_memory)
            .config("spark.sql.shuffle.partitions", "16")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate())


def load_job(filename: str):
    spec = importlib.util.spec_from_file_location(filename[:-3], JOBS / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def export_parquet(spark, db_prefix: str, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for layer, name in EXPORT_TABLES:
        path = out_dir / f"{name}.parquet"
        spark.table(f"{db_prefix}_{layer}.{name}").toPandas().to_parquet(path, index=False)
        print(f"exported {layer}.{name} -> {path.relative_to(ROOT) if path.is_relative_to(ROOT) else path}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("stages", nargs="+", choices=[*STAGES, "export", "all", "history"])
    p.add_argument("--as-of", default=None)
    p.add_argument("--db-prefix", default=DEFAULT_PREFIX)
    p.add_argument("--customers", type=int, default=None)
    p.add_argument("--inject-bad-data", action="store_true")
    p.add_argument("--warehouse", default=str(ROOT / "data" / "warehouse"))
    p.add_argument("--parquet-dir", default=str(ROOT / "data" / "parquet"))
    p.add_argument("--driver-memory", default="6g")
    p.add_argument("--table", default="mule_features")
    args = p.parse_args()

    stages = [*STAGES, "export"] if "all" in args.stages else args.stages
    job_args = ["--db-prefix", args.db_prefix]
    gen_args = list(job_args)
    if args.as_of:
        gen_args += ["--as-of", args.as_of]
    if args.customers:
        gen_args += ["--customers", str(args.customers)]
    if args.inject_bad_data:
        gen_args += ["--inject-bad-data"]

    spark = local_spark(Path(args.warehouse), args.driver_memory)
    spark.sparkContext.setLogLevel("WARN")
    jvm = spark.sparkContext._jvm
    # The Hadoop catalog logs a stack trace for every new table (no version-hint.text yet).
    jvm.org.apache.logging.log4j.core.config.Configurator.setLevel(
        "org.apache.iceberg.hadoop.HadoopTableOperations", jvm.org.apache.logging.log4j.Level.ERROR)
    try:
        for stage in stages:
            print(f"\n=== {stage} ===", flush=True)
            if stage == "export":
                export_parquet(spark, args.db_prefix, Path(args.parquet_dir))
            elif stage == "history":
                spark.sql(f"SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added, "
                          f"summary['deleted-records'] AS deleted "
                          f"FROM {args.db_prefix}_gold.{args.table}.snapshots ORDER BY committed_at").show(truncate=False)
            else:
                try:
                    load_job(STAGES[stage]).main(gen_args if stage == "generate" else job_args, spark=spark)
                except SystemExit as e:
                    if e.code:
                        sys.exit(f"stage '{stage}' failed (exit {e.code})")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

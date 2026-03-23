"""
transform.py  —  PySpark batch job for Industrial Digital Twin dataset
Reads all 5 source Parquet files from GCS, applies type casting and
basic validation, and writes each to its own BigQuery table.

Submitted by Kestra to Dataproc. Run once (dataset covers full year 2025).

Usage:
    gcloud dataproc batches submit pyspark \
        --region=europe-west1 \
        --project=<gcp_project_id> \
        gs://<bucket>/code/transform.py \
        -- \
        --input_prefix=gs://<bucket>/raw/year=2025 \
        --project=<gcp_project_id> \
        --dataset=industrial_twin_raw
"""

import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
    BooleanType,
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("industrial_twin_transform")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ── Timestamp helper ───────────────────────────────────────────────────────
# Parquet files store timestamps as INT64 nanoseconds without the Parquet
# metadata annotation that Spark needs to auto-convert. We read all timestamp
# columns as LongType and convert them explicitly.


def ns_to_timestamp(col_name: str):
    """Convert a nanosecond INT64 column to TimestampType."""
    return (F.col(col_name) / 1_000_000_000).cast(TimestampType())


# ── Schemas ────────────────────────────────────────────────────────────────
# All columns that contain timestamps are declared as LongType here.
# Conversion to TimestampType happens in the transform functions below.


def schema_telemetry() -> StructType:
    return StructType(
        [
            StructField("timestamp", LongType(), False),
            StructField("machine_id", StringType(), False),
            StructField("shift_id", LongType(), True),
            StructField("demand_index", DoubleType(), True),
            StructField("rush_factor", DoubleType(), True),
            StructField("maintenance_deferral", DoubleType(), True),
            StructField("is_weekend", LongType(), True),
            StructField("month_pulse_index", DoubleType(), True),
            StructField("trend_index", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("active_order_id", StringType(), True),
            StructField("steel_grade_active", StringType(), True),
            StructField("hardness_index_active", DoubleType(), True),
            StructField("load_index", DoubleType(), True),
            StructField("bearing_temp_c", DoubleType(), True),
            StructField("vibration_rms", DoubleType(), True),
            StructField("motor_torque_nm", DoubleType(), True),
            StructField("bearing_health", DoubleType(), True),
            StructField("lube_health", DoubleType(), True),
            StructField("align_health", DoubleType(), True),
            StructField("motor_health", DoubleType(), True),
            StructField("cumulative_op_hours", DoubleType(), True),
            StructField("op_hours_since_lube", DoubleType(), True),
            StructField("op_hours_since_calib", DoubleType(), True),
            StructField("op_hours_since_bearing", DoubleType(), True),
            StructField("op_hours_since_major", DoubleType(), True),
            StructField("active_maintenance_id", StringType(), True),
            StructField("active_maintenance_type", StringType(), True),
            StructField("last_failure_id", StringType(), True),
            StructField("label_anomaly", LongType(), True),
            StructField("label_failure_in_24h", LongType(), True),
            StructField("label_failure_in_72h", LongType(), True),
            StructField("label_failure_type_next", StringType(), True),
            StructField("is_running", LongType(), True),
            StructField("vib_norm", DoubleType(), True),
            StructField("temp_norm", DoubleType(), True),
            StructField("vib_z", DoubleType(), True),
            StructField("temp_z", DoubleType(), True),
        ]
    )


def schema_failures() -> StructType:
    return StructType(
        [
            StructField("failure_id", StringType(), False),
            StructField("machine_id", StringType(), False),
            StructField("timestamp_occurrence", LongType(), False),
            StructField("failure_category", StringType(), True),
            StructField("root_cause", StringType(), True),
            StructField("severity_level", LongType(), True),
            StructField("steel_grade_active", StringType(), True),
            StructField("hardness_index_active", DoubleType(), True),
            StructField("load_index_at_failure", DoubleType(), True),
            StructField("bearing_temp_c_at_failure", DoubleType(), True),
            StructField("vibration_rms_at_failure", DoubleType(), True),
            StructField("motor_torque_nm_at_failure", DoubleType(), True),
            StructField("bearing_health_at_failure", DoubleType(), True),
            StructField("lube_health_at_failure", DoubleType(), True),
            StructField("align_health_at_failure", DoubleType(), True),
            StructField("motor_health_at_failure", DoubleType(), True),
            StructField("demand_index", DoubleType(), True),
            StructField("rush_factor", DoubleType(), True),
            StructField("maintenance_deferral", DoubleType(), True),
            StructField("is_weekend", LongType(), True),
            StructField("month_pulse_index", DoubleType(), True),
            StructField("trend_index", DoubleType(), True),
        ]
    )


def schema_maintenance() -> StructType:
    return StructType(
        [
            StructField("maintenance_id", StringType(), False),
            StructField("machine_id", StringType(), False),
            StructField("timestamp_start", LongType(), False),
            StructField("maint_type", StringType(), True),
            StructField("is_planned", BooleanType(), True),
            StructField("linked_failure_id", StringType(), True),
            StructField("duration_minutes", LongType(), True),
        ]
    )


def schema_orders() -> StructType:
    return StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("timestamp_planned", LongType(), True),
            StructField("timestamp_start", LongType(), True),
            StructField("timestamp_end", LongType(), True),
            StructField("market", StringType(), True),
            StructField("steel_grade", StringType(), True),
            StructField("hardness_index", DoubleType(), True),
            StructField("total_weight_tons", DoubleType(), True),
            StructField("demand_index_at_intake", DoubleType(), True),
            StructField("rush_factor_at_intake", DoubleType(), True),
            StructField("month_pulse_index_at_intake", DoubleType(), True),
            StructField("trend_index_at_intake", DoubleType(), True),
            StructField("is_weekend_intake", LongType(), True),
            StructField("machine_id_started", StringType(), True),
            StructField("duration_minutes_planned", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("is_completed", BooleanType(), True),
        ]
    )


# ── Per-table transformations ──────────────────────────────────────────────


def transform_telemetry(df: DataFrame) -> DataFrame:
    return (
        df.dropna(subset=["timestamp", "machine_id"])
        .withColumn("timestamp", ns_to_timestamp("timestamp"))
        .withColumn("date", F.to_date("timestamp"))
        .withColumn("is_anomaly", F.col("label_anomaly").cast(BooleanType()))
        .withColumn("fail_in_24h", F.col("label_failure_in_24h").cast(BooleanType()))
        .withColumn("fail_in_72h", F.col("label_failure_in_72h").cast(BooleanType()))
        .withColumn("is_running", F.col("is_running").cast(BooleanType()))
        .withColumn("is_weekend", F.col("is_weekend").cast(BooleanType()))
        .withColumn("loaded_at", F.current_timestamp())
    )


def transform_failures(df: DataFrame) -> DataFrame:
    return (
        df.dropna(subset=["failure_id", "machine_id", "timestamp_occurrence"])
        .withColumn("timestamp_occurrence", ns_to_timestamp("timestamp_occurrence"))
        .withColumn("date", F.to_date("timestamp_occurrence"))
        .withColumn("year", F.year(F.col("date")).cast(LongType()))
        .withColumn("is_weekend", F.col("is_weekend").cast(BooleanType()))
        .withColumn("loaded_at", F.current_timestamp())
    )


def transform_maintenance(df: DataFrame) -> DataFrame:
    return (
        df.dropna(subset=["maintenance_id", "machine_id", "timestamp_start"])
        .withColumn("timestamp_start", ns_to_timestamp("timestamp_start"))
        .withColumn("date", F.to_date("timestamp_start"))
        .withColumn("year", F.year(F.col("date")).cast(LongType()))
        .withColumn("loaded_at", F.current_timestamp())
    )


def transform_orders(df: DataFrame) -> DataFrame:
    return (
        df.dropna(subset=["order_id"])
        .withColumn("timestamp_planned", ns_to_timestamp("timestamp_planned"))
        .withColumn("timestamp_start", ns_to_timestamp("timestamp_start"))
        .withColumn("timestamp_end", ns_to_timestamp("timestamp_end"))
        .withColumn("date_planned", F.to_date("timestamp_planned"))
        .withColumn("date_start", F.to_date("timestamp_start"))
        .withColumn("year", F.year(F.col("date_planned")).cast(LongType()))
        .withColumn("is_weekend_intake", F.col("is_weekend_intake").cast(BooleanType()))
        .withColumn(
            "actual_duration_minutes",
            F.when(
                F.col("timestamp_end").isNotNull()
                & F.col("timestamp_start").isNotNull(),
                (
                    F.unix_timestamp("timestamp_end")
                    - F.unix_timestamp("timestamp_start")
                )
                / 60,
            ),
        )
        .withColumn(
            "duration_variance_minutes",
            F.when(
                F.col("actual_duration_minutes").isNotNull(),
                F.col("actual_duration_minutes") - F.col("duration_minutes_planned"),
            ),
        )
        .withColumn("loaded_at", F.current_timestamp())
    )


def transform_demand(df: DataFrame) -> DataFrame:
    # demand.date is inferred as DateType by Spark — no conversion needed.
    return (
        df.dropna(subset=["date"])
        .withColumn("is_weekend", F.col("is_weekend").cast(BooleanType()))
        .withColumn("year", F.year(F.col("date")).cast(LongType()))
        .withColumn("loaded_at", F.current_timestamp())
    )


# ── BQ write ──────────────────────────────────────────────────────────────


def write_to_bq(
    df: DataFrame,
    project: str,
    dataset: str,
    table: str,
    temp_bucket: str,
    partition_field: str = None,
) -> None:
    full_table = f"{project}:{dataset}.{table}"

    if partition_field:
        # Partitioned table: overwrite only the partitions present in this
        # batch. Safe to run for any year without touching other years' data.
        writer = (
            df.write.format("bigquery")
            .mode("overwrite")
            .option("table", full_table)
            .option("createDisposition", "CREATE_IF_NEEDED")
            .option("writeDisposition", "WRITE_TRUNCATE")
            .option("partitionField", partition_field)
            .option("partitionType", "DAY")
            .option("partitionOverwriteMode", "DYNAMIC")
            .option("temporaryGcsBucket", temp_bucket)
        )
    else:
        # Unpartitioned tables (failures, maintenance, orders, demand):
        writer = (
            df.write.format("bigquery")
            .mode("append")
            .option("table", full_table)
            .option("createDisposition", "CREATE_IF_NEEDED")
            .option("writeDisposition", "WRITE_APPEND")
            .option("temporaryGcsBucket", temp_bucket)
        )

    writer.save()
    print(f"  Written → {full_table}")


# ── Main ──────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_prefix",
        required=True,
        help="GCS prefix for source files, e.g. gs://bucket/raw/year=2025",
    )
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--dataset", default="industrial_twin_raw")
    parser.add_argument(
        "--temp_bucket",
        required=True,
        help="GCS bucket name for Spark temporary files (no gs:// prefix)",
    )
    parser.add_argument("--year", required=True, help="Processing year e.g. 2025")
    args = parser.parse_args()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    p = args.input_prefix.rstrip("/")

    tasks = [
        ("telemetry", schema_telemetry(), transform_telemetry, "date"),
        ("failures", schema_failures(), transform_failures, None),
        ("maintenance", schema_maintenance(), transform_maintenance, None),
        ("orders", schema_orders(), transform_orders, None),
        (
            "demand",
            None,
            transform_demand,
            None,
        ),  # schema inferred — date is INT32 Parquet DATE
    ]

    for name, schema, transform_fn, partition_field in tasks:
        print(f"\nProcessing {name}...")
        path = f"{p}/{name}_{args.year}.parquet"
        if schema is not None:
            raw = spark.read.schema(schema).parquet(path)
        else:
            raw = spark.read.parquet(path)
        print(f"  Raw rows: {raw.count():,}")
        clean = transform_fn(raw)
        print(f"  Clean rows: {clean.count():,}")
        write_to_bq(
            clean,
            args.project,
            args.dataset,
            name,
            args.temp_bucket,
            partition_field=partition_field,
        )

    print("\nAll 5 tables written successfully.")
    spark.stop()


if __name__ == "__main__":
    main()

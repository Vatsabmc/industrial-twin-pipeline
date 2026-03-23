"""
ingest.py  —  One-time data preparation script
Takes the Industrial Digital Twin dataset from Kaggle,
splits it into monthly Parquet files, and uploads them to GCS.

Run this locally ONCE before starting the pipeline.

Prerequisites:
    pip install kaggle pandas pyarrow google-cloud-storage

    Set up Kaggle credentials: ~/.kaggle/kaggle.json
    Set GOOGLE_APPLICATION_CREDENTIALS env var or use --key-file flag.

Usage:
    python ingest.py \
        --bucket it_datalake_<YOUR_PROJECT_ID> \
        --dataset <kaggle-owner>/<dataset-slug>
"""

import argparse
from pathlib import Path
from google.cloud import storage


LOCAL_DOWNLOAD_DIR = Path("data")
TIMESTAMP_COLUMN = "timestamp"  # adjust if the dataset uses a different name


def upload_to_gcs(bucket_name: str, src_dir: Path, filenames: list[str]) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for filename in filenames:
        blob_name = f"raw/year=2025/{filename}"
        blob = bucket.blob(blob_name)
        print(f"  Uploading {filename} → gs://{bucket_name}/{blob_name}")
        blob.upload_from_filename(str(src_dir / filename))

    spark_script = Path(__file__).parent / "spark" / "transform.py"
    if spark_script.exists():
        blob = bucket.blob("code/transform.py")
        blob.upload_from_filename(str(spark_script))
        print(f"  Uploaded Spark script → gs://{bucket_name}/code/transform.py")


def main():
    parser = argparse.ArgumentParser(
        description="Upload Industrial Digital Twin data to GCS"
    )
    parser.add_argument(
        "--bucket", required=True, help="GCS bucket name (without gs://)"
    )
    parser.add_argument(
        "--src-dir", default="data", help="Directory containing the 5 Parquet files"
    )
    args = parser.parse_args()

    src = Path(args.src_dir)
    expected = [
        "telemetry_2025.parquet",
        "failures_2025.parquet",
        "maintenance_2025.parquet",
        "orders_2025.parquet",
        "demand_2025.parquet",
    ]

    # Verify all files are present before touching GCS
    missing = [f for f in expected if not (src / f).exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing files in {src}:\n" + "\n".join(f"  {f}" for f in missing)
        )

    print(f"All 5 files found in {src}. Uploading to gs://{args.bucket}/")
    upload_to_gcs(args.bucket, src, expected)
    print("\nDone. You can now trigger the Kestra pipeline.")


if __name__ == "__main__":
    main()

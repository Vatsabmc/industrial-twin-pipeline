output "gcs_bucket_name" {
  description = "Name of the created GCS data lake bucket"
  value       = google_storage_bucket.data_lake.name
}

output "bq_raw_dataset" {
  description = "BigQuery raw dataset ID"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "bq_dbt_dataset" {
  description = "BigQuery dbt production dataset ID"
  value       = google_bigquery_dataset.dbt_prod.dataset_id
}

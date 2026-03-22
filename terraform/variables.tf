variable "credentials" {
  description = "Path to GCP service account JSON key file"
  default     = "./keys/my-creds.json"
}

variable "project" {
  description = "GCP Project ID"
  default     = "industrial-twin-pipeline"
}

variable "region" {
  description = "GCP Region"
  default     = "europe-west1"
}

variable "location" {
  description = "GCP multi-region location for GCS and BigQuery"
  default     = "EU"
}

variable "storage_class" {
  description = "GCS bucket storage class"
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "Base name of the GCS data lake bucket (project ID is appended)"
  default     = "it_datalake"
}

variable "bq_dataset_raw" {
  description = "BigQuery dataset for raw sensor data (loaded directly from GCS)"
  default     = "industrial_twin_raw"
}

variable "bq_dataset_dbt" {
  description = "BigQuery dataset for dbt production models"
  default     = "industrial_twin_prod"
}

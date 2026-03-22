terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

# -------------------------------------------------------
# Data Lake: GCS Bucket
# -------------------------------------------------------
resource "google_storage_bucket" "data_lake" {
  name                        = "${var.gcs_bucket_name}_${var.project}"
  location                    = var.location
  storage_class               = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 90 # days — raw files older than 90 days are deleted
    }
  }

  force_destroy = true
}

# -------------------------------------------------------
# Data Warehouse: BigQuery Dataset (raw layer)
# -------------------------------------------------------
resource "google_bigquery_dataset" "raw" {
  dataset_id  = var.bq_dataset_raw
  project     = var.project
  location    = var.location
  description = "Raw sensor data loaded from GCS"

  delete_contents_on_destroy = true
}

# -------------------------------------------------------
# Data Warehouse: BigQuery Dataset (dbt production layer)
# -------------------------------------------------------
resource "google_bigquery_dataset" "dbt_prod" {
  dataset_id  = var.bq_dataset_dbt
  project     = var.project
  location    = var.location
  description = "dbt-transformed marts for dashboarding"

  delete_contents_on_destroy = true
}

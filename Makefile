# Makefile — shortcuts for the Industrial Twin pipeline
# Usage: make <target>

PROJECT_ID  ?= industrial-twin-pipeline
BUCKET      ?= it_datalake_$(PROJECT_ID)
REGION      ?= europe-west1
DBT_DIR     := dbt
YEAR        ?= 2025
PYTHON      ?= python

.PHONY: help tf-init tf-plan tf-apply tf-destroy \
        ingest spark-upload spark-batch \
        dbt-deps dbt-run dbt-test dbt-docs \
        all clean

help:          ## Show this help
	@echo "Available targets:"
	@echo "  tf-init       Initialise Terraform"
	@echo "  tf-plan       Show Terraform plan"
	@echo "  tf-apply      Apply Terraform (GCS + BigQuery)"
	@echo "  tf-destroy    Destroy all Terraform-managed resources"
	@echo "  ingest        Upload local parquet files and Spark script to GCS"
	@echo "  spark-upload  Upload only spark/transform.py to GCS"
	@echo "  spark-batch   Submit Dataproc Serverless PySpark batch"
	@echo "  dbt-deps      Install dbt packages"
	@echo "  dbt-run       Run dbt models (dev)"
	@echo "  dbt-test      Run dbt tests (dev)"
	@echo "  dbt-docs      Generate and serve dbt docs"
	@echo "  dbt-prod      Run dbt + tests against prod target"
	@echo "  all           End-to-end local run (terraform + ingest + spark + dbt)"
	@echo "  clean         Remove dbt artefacts"

# ── Terraform ──────────────────────────────────────────
tf-init:       ## Initialise Terraform
	cd terraform && terraform init

tf-plan:       ## Show Terraform plan
	cd terraform && terraform plan

tf-apply:      ## Apply Terraform (provision GCS and BigQuery datasets)
	cd terraform && terraform apply -auto-approve

tf-destroy:    ## Destroy all Terraform-managed resources
	cd terraform && terraform destroy -auto-approve

# ── Data ingestion ─────────────────────────────────────
ingest:        ## Upload local data/*.parquet and Spark script to GCS
	$(PYTHON) ingest.py --bucket $(BUCKET)

spark-upload:  ## Upload only the Spark script to GCS
	gsutil cp spark/transform.py gs://$(BUCKET)/code/transform.py

spark-batch:   ## Submit Dataproc Serverless Spark batch
	gcloud dataproc batches submit pyspark \
		--region $(REGION) \
		--project $(PROJECT_ID) \
		gs://$(BUCKET)/code/transform.py \
		-- \
		--input_prefix=gs://$(BUCKET)/raw/year=$(YEAR) \
		--project=$(PROJECT_ID) \
		--dataset=industrial_twin_raw \
		--temp_bucket=$(BUCKET) \
		--year=$(YEAR)

# ── dbt ────────────────────────────────────────────────
dbt-deps:      ## Install dbt packages
	cd $(DBT_DIR) && dbt deps

dbt-run:       ## Run all dbt models (dev target)
	cd $(DBT_DIR) && dbt run

dbt-test:      ## Run dbt tests
	cd $(DBT_DIR) && dbt test

dbt-docs:      ## Generate and serve dbt docs locally
	cd $(DBT_DIR) && dbt docs generate && dbt docs serve

dbt-prod:      ## Run dbt against production BigQuery dataset
	cd $(DBT_DIR) && dbt run --target prod && dbt test --target prod

# ── Full pipeline (local dev) ──────────────────────────
all: tf-apply ingest spark-batch dbt-deps dbt-prod  ## Provision + ingest + transform (end-to-end)

clean:         ## Remove dbt artefacts
	$(PYTHON) -c "import shutil, pathlib; [shutil.rmtree(pathlib.Path('$(DBT_DIR)') / d, ignore_errors=True) for d in ('target','dbt_packages','logs')]"

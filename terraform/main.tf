provider "google" {
  project     = "ny-taxi-ingest-airflow"
  region      = "us-central1"
  credentials = file("~/.google/credentials/google_credentials.json")
}

resource "google_storage_bucket" "ny_taxi_bucket" {
  name          = "ny-taxi-ingest-airflow-bucket"
  location      = "US"
  force_destroy = true

  versioning {
    enabled = true
  }

  lifecycle {
    prevent_destroy = false
  }
}

# Create a BigQuery dataset
resource "google_bigquery_dataset" "ny_taxi_ingest_airflow_dataset" {
  dataset_id    = "ny_taxi_ingest_airflow_dataset"                         # Dataset ID
  friendly_name = "NY Taxi Ingest Airflow Dataset"                         # Friendly name for the dataset
  description   = "Dataset for storing NY taxi data ingested via Airflow." # Description of the dataset
  location      = "US"                                                     # Location of the dataset (US, EU, etc.)

  # Set default table expiration to 1 days (optional)
  default_table_expiration_ms = 1 * 24 * 60 * 60 * 1000
}
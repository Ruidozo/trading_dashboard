terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    bucket  = "terraform-state-bucket-odiur"
    prefix  = "terraform/state"
  }
}

provider "google" {
  project = "trading-dashboard-449211"
  region  = "us-central1"
}

resource "google_storage_bucket" "stock_data" {
  name          = "trading_dashboard_stock_data"
  location      = "US"
  storage_class = "STANDARD"  # Free Tier eligible

  versioning {
    enabled = false  # Disable to reduce storage costs
  }

  lifecycle_rule {
    condition {
      age = 365  # Auto-delete files older than 365 days
    }
    action {
      type = "Delete"
    }
  }

  force_destroy = true  # Allow bucket deletion if needed
}

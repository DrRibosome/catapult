provider "google" {
  project = "myproject"
  region = "us-east1"
  zone = "us-east1-d"
}

resource "google_container_node_pool" "catapult_worker" {
  # NOTE: cluster manually created now
  cluster = "cluster-4"

  name_prefix = "tf-catapult-"
  location = "us-east1-d"

  autoscaling {
    min_node_count = 0
    max_node_count = 2
  }

  # For structure, see:
  # https://www.terraform.io/docs/providers/google/r/container_cluster.html
  node_config {
    preemptible = true
    machine_type = "n1-highcpu-16"
    disk_type = "pd-ssd"
    disk_size_gb = 100
    local_ssd_count = 1

    # scopes filled in by comparing to default scopes on a manually created node group
    # https://developers.google.com/identity/protocols/oauth2/scopes
    oauth_scopes = [
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring",
      "https://www.googleapis.com/auth/devstorage.read_only",
      "https://www.googleapis.com/auth/service.management.readonly"
    ]

    labels = {
      dedicated = "catapult"
    }

    taint {
      key = "dedicated"
      value = "catapult"
      effect = "NO_SCHEDULE"
    }
  }
}

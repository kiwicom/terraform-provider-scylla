terraform {
  required_providers {
    scylla = {
      source  = "martin-sucha/scylla"
      version = "~> 1.0"
    }
  }
}

provider "scylla" {
  # example configuration here
}

terraform {
  required_providers {
    scylla = {
      source  = "kiwicom/scylla"
      version = "~> 1.0"
    }
  }
}

provider "scylla" {
  hosts    = "localhost"
  username = "cassandra"
  password = "cassandra"
}

resource "scylla_service_level" "example" {
  name                 = "sl-example"
  shares               = 900
  workload_type        = "interactive"
  timeout_milliseconds = "300"
}

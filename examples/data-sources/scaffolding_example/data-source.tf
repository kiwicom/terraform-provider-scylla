terraform {
  required_providers {
    scylla = {
      source  = "kiwicom/scylla"
      version = "~> 1.0"
    }
  }
}

data "scylla_example" "example" {
  configurable_attribute = "some-value"
}

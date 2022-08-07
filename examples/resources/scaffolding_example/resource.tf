terraform {
  required_providers {
    scylla = {
      source  = "martin-sucha/scylla"
      version = "~> 1.0"
    }
  }
}

provider "scylla" {
  hosts = "localhost"
  username = "cassandra"
  password = "cassandra"
}

resource "scylla_example" "example" {
  configurable_attribute = "some-value"
}

resource "scylla_role" "test" {
  name = "myrole2"
}

resource "scylla_role" "test2" {
  name = "myrole3"
}

resource "scylla_role" "test3" {
  name = "myrole4"
  login = true
  password = "hello"
  superuser = true
}
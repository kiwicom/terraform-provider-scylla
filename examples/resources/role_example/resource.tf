terraform {
  required_providers {
    scylla = {
      source  = "kiwicom/scylla"
      version = "~> 1.0"
    }
  }
}

provider "scylla" {
  hosts = "localhost"
  username = "cassandra"
  password = "cassandra"
}

resource "scylla_role" "example" {
  name = "role-example"
  login = false
  superuser = false
}

resource "scylla_service_level" "sl1" {
  name = "sl1"
}

resource "scylla_service_level" "sl2" {
  name = "sl2"
}

resource "scylla_role" "with_password_and_sl" {
  name = "role-pwd"
  login = true
  superuser = false
  password = "hello world"
  service_level = scylla_service_level.sl2.name
}

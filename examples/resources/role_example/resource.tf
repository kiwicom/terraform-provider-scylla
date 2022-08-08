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

resource "scylla_role" "example" {
  name = "role-example"
  login = false
  superuser = false
}

resource "scylla_role" "with_password" {
  name = "role-pwd"
  login = true
  superuser = false
  password = "hello world"
}

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

resource "scylla_role" "role3" {
  name = "role3"
  login = false
  superuser = false
}

resource "scylla_role" "role4" {
  name = "role4"
  login = false
  superuser = false
}

resource "scylla_keyspace_grant" "example" {
  keyspace = "system_traces"
  grantee = scylla_role.role3.name
  permission = "SELECT"
}

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

resource "scylla_role" "role1" {
  name      = "role1"
  login     = false
  superuser = false
}

resource "scylla_role" "role2" {
  name      = "role2"
  login     = false
  superuser = false
}

resource "scylla_table_grant" "example" {
  keyspace   = "system_traces"
  table      = "sessions"
  grantee    = scylla_role.role1.name
  permission = "SELECT"
}

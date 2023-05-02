---
# generated by https://github.com/hashicorp/terraform-plugin-docs
page_title: "scylla_keyspace_grant Resource - terraform-provider-scylla"
subcategory: ""
description: |-
  Manages grant to a single table for a single role
---

# scylla_keyspace_grant (Resource)

Manages grant to a single table for a single role



<!-- schema generated by tfplugindocs -->
## Schema

### Required

- `grantee` (String) The name of the role that will be granted privileges to the resource.
- `keyspace` (String) Name of the keyspace where the table resides
- `permission` (String) The permission that is granted.
One of:

* ALTER
* AUTHORIZE
* CREATE
* DROP
* MODIFY
* SELECT


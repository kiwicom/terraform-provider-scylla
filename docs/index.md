---
# generated by https://github.com/hashicorp/terraform-plugin-docs
page_title: "scylla Provider"
subcategory: ""
description: |-
  
---

# scylla Provider



## Example Usage

```terraform
terraform {
  required_providers {
    scylla = {
      source  = "kiwicom/scylla"
      version = "~> 1.0"
    }
  }
}

provider "scylla" {
  # example configuration here
}
```

<!-- schema generated by tfplugindocs -->
## Schema

### Optional

- `hosts` (String) Host or hosts to connect to
- `password` (String, Sensitive) Password for authentication
- `username` (String) Username for authentication

package provider

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccRoleResource(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: testAccRoleResourceConfig("one"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("scylla_role.test", "name", "one"),
				),
			},
			// ImportState testing
			{
				ResourceName:      "scylla_role.test",
				ImportState:       true,
				ImportStateVerify: true,
				// This is not normally necessary, but is here because this
				// example code does not have an actual upstream service.
				// Once the Read method is able to refresh information from
				// the upstream service, this can be removed.
				ImportStateVerifyIgnore: []string{"name"},
			},
			// Update and Read testing
			{
				Config: testAccRoleResourceConfig("two"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("scylla_role.test", "name", "two"),
				),
			},
			// Delete testing automatically occurs in TestCase
		},
	})
}

func testAccRoleResourceConfig(name string) string {
	return fmt.Sprintf(`
resource "scylla_role" "test" {
  name = %[1]q
}
`, name)
}

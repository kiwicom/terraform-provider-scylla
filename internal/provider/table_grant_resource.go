package provider

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"

	"github.com/kiwicom/terraform-provider-scylla/internal/qb"
)

// Ensure provider defined types fully satisfy framework interfaces
var _ tfsdk.ResourceType = tableGrantResourceType{}
var _ tfsdk.Resource = tableGrantResource{}
var _ tfsdk.ResourceWithImportState = tableGrantResource{}
var _ grantResourceData = &tableGrantResourceData{}

type tableGrantResourceType struct{}

func (t tableGrantResourceType) GetSchema(ctx context.Context) (tfsdk.Schema, diag.Diagnostics) {
	return tfsdk.Schema{
		// This description is used by the documentation generator and the language server.
		MarkdownDescription: "Manages grant to a single table for a single role",

		Attributes: map[string]tfsdk.Attribute{
			"keyspace": {
				MarkdownDescription: "Name of the keyspace where the table resides",
				Required:            true,
				Type:                types.StringType,
				PlanModifiers: []tfsdk.AttributePlanModifier{
					tfsdk.RequiresReplace(),
				},
			},
			"table": {
				MarkdownDescription: "Name of the table",
				Required:            true,
				Type:                types.StringType,
				PlanModifiers: []tfsdk.AttributePlanModifier{
					tfsdk.RequiresReplace(),
				},
			},
			"grantee": {
				Required:            true,
				MarkdownDescription: "The name of the role that will be granted privileges to the resource.",
				Type:                types.StringType,
				PlanModifiers: []tfsdk.AttributePlanModifier{
					tfsdk.RequiresReplace(),
				},
			},
			"permission": {
				Required: true,
				MarkdownDescription: `The permission that is granted.
One of:

* ALTER
* AUTHORIZE
* DROP
* MODIFY
* SELECT
`,
				Type: types.StringType,
				PlanModifiers: []tfsdk.AttributePlanModifier{
					tfsdk.RequiresReplace(),
				},
			},
		},
	}, nil
}

func (t tableGrantResourceType) NewResource(ctx context.Context, in tfsdk.Provider) (tfsdk.Resource, diag.Diagnostics) {
	provider, diags := convertProviderType(in)

	return tableGrantResource{
		provider: provider,
	}, diags
}

type tableGrantResourceData struct {
	Keyspace   types.String `tfsdk:"keyspace"`
	Table      types.String `tfsdk:"table"`
	Grantee    types.String `tfsdk:"grantee"`
	Permission types.String `tfsdk:"permission"`
}

func (t *tableGrantResourceData) resource() qb.CQL {
	return qb.CQL(fmt.Sprintf("%s.%s", qb.QName(t.Keyspace.Value), qb.QName(t.Table.Value)))
}

func (t *tableGrantResourceData) listResource() string {
	return fmt.Sprintf("<table %s.%s>", strings.ToLower(t.Keyspace.Value),
		strings.ToLower(t.Table.Value))
}

func (t *tableGrantResourceData) permission() qb.CQL {
	return qb.CQL(t.Permission.Value)
}

func (t *tableGrantResourceData) grantee() string {
	return t.Grantee.Value
}

func (t *tableGrantResourceData) validate() (diags diag.Diagnostics) {
	if t.Keyspace.IsNull() || t.Keyspace.IsUnknown() || t.Keyspace.Value == "" {
		diags.AddAttributeError(path.Root("keyspace"), "Keyspace missing",
			"Keyspace of the table must be specified.")
	}
	if t.Table.IsNull() || t.Table.IsUnknown() || t.Table.Value == "" {
		diags.AddAttributeError(path.Root("table"), "Table missing",
			"Table name must be specified.")
	}
	if t.Grantee.IsNull() || t.Grantee.IsUnknown() || t.Grantee.Value == "" {
		diags.AddAttributeError(path.Root("grantee"), "Grantee missing",
			"Grantee must be specified.")
	}
	if t.Permission.IsNull() || t.Permission.IsUnknown() || t.Permission.Value == "" {
		diags.AddAttributeError(path.Root("permission"), "Permission missing",
			"Permission must be specified.")
	} else {
		perm := strings.ToUpper(t.Permission.Value)
		permNames := make([]string, 0, len(tablePermissions))
		for k := range tablePermissions {
			permNames = append(permNames, k)
		}
		sort.Strings(permNames)
		if _, ok := tablePermissions[perm]; !ok {
			diags.AddAttributeError(path.Root("permission"), "Unsupported permission",
				fmt.Sprintf("Permission must be one of %s", permNames))
		}
	}

	return
}

type tableGrantResource struct {
	provider provider
}

var tablePermissions = map[string]struct{}{
	"CREATE":    {},
	"ALTER":     {},
	"DROP":      {},
	"SELECT":    {},
	"MODIFY":    {},
	"AUTHORIZE": {},
	"DESCRIBE":  {},
}

func (r tableGrantResource) Create(ctx context.Context, req tfsdk.CreateResourceRequest, resp *tfsdk.CreateResourceResponse) {
	var data tableGrantResourceData
	r.provider.createGrant(ctx, req, resp, &data)
}

func (r tableGrantResource) Read(ctx context.Context, req tfsdk.ReadResourceRequest, resp *tfsdk.ReadResourceResponse) {
	var data tableGrantResourceData
	r.provider.readGrant(ctx, req, resp, &data)
}

func (r tableGrantResource) Update(ctx context.Context, req tfsdk.UpdateResourceRequest, resp *tfsdk.UpdateResourceResponse) {
	resp.Diagnostics.AddError("Update not supported", "Grant resource does not support update, only recreate")
}

func (r tableGrantResource) Delete(ctx context.Context, req tfsdk.DeleteResourceRequest, resp *tfsdk.DeleteResourceResponse) {
	var data tableGrantResourceData
	r.provider.deleteGrant(ctx, req, resp, &data)
}

func (r tableGrantResource) ImportState(ctx context.Context, req tfsdk.ImportResourceStateRequest, resp *tfsdk.ImportResourceStateResponse) {
	tfsdk.ResourceImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

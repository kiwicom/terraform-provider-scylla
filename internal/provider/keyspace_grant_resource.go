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
var _ tfsdk.ResourceType = keyspaceGrantResourceType{}
var _ tfsdk.Resource = keyspaceGrantResource{}
var _ tfsdk.ResourceWithImportState = keyspaceGrantResource{}
var _ grantResourceData = &keyspaceGrantResourceData{}

type keyspaceGrantResourceType struct{}

func (t keyspaceGrantResourceType) GetSchema(ctx context.Context) (tfsdk.Schema, diag.Diagnostics) {
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
* CREATE
* DROP
* MODIFY
* SELECT`,
				Type: types.StringType,
				PlanModifiers: []tfsdk.AttributePlanModifier{
					tfsdk.RequiresReplace(),
				},
			},
		},
	}, nil
}

func (t keyspaceGrantResourceType) NewResource(ctx context.Context, in tfsdk.Provider) (tfsdk.Resource, diag.Diagnostics) {
	provider, diags := convertProviderType(in)

	return keyspaceGrantResource{
		provider: provider,
	}, diags
}

type keyspaceGrantResourceData struct {
	Keyspace   types.String `tfsdk:"keyspace"`
	Grantee    types.String `tfsdk:"grantee"`
	Permission types.String `tfsdk:"permission"`
}

func (t *keyspaceGrantResourceData) resource() qb.CQL {
	return qb.CQL(fmt.Sprintf("KEYSPACE %s", qb.QName(t.Keyspace.Value)))
}

func (t *keyspaceGrantResourceData) listResource() string {
	return fmt.Sprintf("<keyspace %s>", strings.ToLower(t.Keyspace.Value))
}

func (t *keyspaceGrantResourceData) permission() qb.CQL {
	return qb.CQL(t.Permission.Value)
}

func (t *keyspaceGrantResourceData) grantee() string {
	return t.Grantee.Value
}

func (t *keyspaceGrantResourceData) validate() (diags diag.Diagnostics) {
	if t.Keyspace.IsNull() || t.Keyspace.IsUnknown() || t.Keyspace.Value == "" {
		diags.AddAttributeError(path.Root("keyspace"), "Keyspace missing",
			"Keyspace of the table must be specified.")
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
		permNames := make([]string, 0, len(keyspacePermissions))
		for k := range keyspacePermissions {
			permNames = append(permNames, k)
		}
		sort.Strings(permNames)
		if _, ok := keyspacePermissions[perm]; !ok {
			diags.AddAttributeError(path.Root("permission"), "Unsupported permission",
				fmt.Sprintf("Permission must be one of %s", permNames))
		}
	}

	return
}

type keyspaceGrantResource struct {
	provider provider
}

var keyspacePermissions = map[string]struct{}{
	"CREATE":    {},
	"ALTER":     {},
	"DROP":      {},
	"SELECT":    {},
	"MODIFY":    {},
	"AUTHORIZE": {},
	"DESCRIBE":  {},
}

func (r keyspaceGrantResource) Create(ctx context.Context, req tfsdk.CreateResourceRequest, resp *tfsdk.CreateResourceResponse) {
	var data keyspaceGrantResourceData
	r.provider.createGrant(ctx, req, resp, &data)
}

func (r keyspaceGrantResource) Read(ctx context.Context, req tfsdk.ReadResourceRequest, resp *tfsdk.ReadResourceResponse) {
	var data keyspaceGrantResourceData
	r.provider.readGrant(ctx, req, resp, &data)
}

func (r keyspaceGrantResource) Update(ctx context.Context, req tfsdk.UpdateResourceRequest, resp *tfsdk.UpdateResourceResponse) {
	resp.Diagnostics.AddError("Update not supported", "Grant resource does not support update, only recreate")
}

func (r keyspaceGrantResource) Delete(ctx context.Context, req tfsdk.DeleteResourceRequest, resp *tfsdk.DeleteResourceResponse) {
	var data keyspaceGrantResourceData
	r.provider.deleteGrant(ctx, req, resp, &data)
}

func (r keyspaceGrantResource) ImportState(ctx context.Context, req tfsdk.ImportResourceStateRequest, resp *tfsdk.ImportResourceStateResponse) {
	tfsdk.ResourceImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

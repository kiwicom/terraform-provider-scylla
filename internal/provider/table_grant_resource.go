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
	"github.com/hashicorp/terraform-plugin-log/tflog"

	"github.com/kiwicom/terraform-provider-scylla/internal/qb"
)

// Ensure provider defined types fully satisfy framework interfaces
var _ tfsdk.ResourceType = tableGrantResourceType{}
var _ tfsdk.Resource = tableGrantResource{}
var _ tfsdk.ResourceWithImportState = tableGrantResource{}

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

* ALL
* CREATE
* ALTER
* DROP
* SELECT
* MODIFY
* AUTHORIZE
* DESCRIBE`,
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
		permNames := make([]string, 0, len(permissions))
		for k := range permissions {
			permNames = append(permNames, k)
		}
		sort.Strings(permNames)
		if _, ok := permissions[perm]; !ok {
			diags.AddAttributeError(path.Root("permission"), "Unsupported permission",
				fmt.Sprintf("Permission must be one of %s", permNames))
		}
	}

	return
}

type tableGrantResource struct {
	provider provider
}

var permissions = map[string]struct{}{
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

	diags := req.Config.Get(ctx, &data)
	diags = append(diags, data.validate()...)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	perm := strings.ToUpper(data.Permission.Value)

	var stmt qb.Builder
	stmt.Appendf("GRANT %s ON %s TO %s", qb.CQL(perm), data.resource(), qb.QName(data.Grantee.Value))

	_, err := r.provider.execute(stmt.String(), nil)
	if err != nil {
		resp.Diagnostics.AddError("error granting", fmt.Sprintf("%s\n\n%s", stmt.String(), err.Error()))
		return
	}

	tflog.Trace(ctx, "created grant")

	diags = resp.State.Set(ctx, &data)
	resp.Diagnostics.Append(diags...)
}

func (r tableGrantResource) Read(ctx context.Context, req tfsdk.ReadResourceRequest, resp *tfsdk.ReadResourceResponse) {
	var data tableGrantResourceData

	diags := req.State.Get(ctx, &data)
	diags = append(diags, data.validate()...)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	upperPermission := strings.ToUpper(data.Permission.Value)

	var stmt qb.Builder
	stmt.Appendf("LIST %s PERMISSION ON %s OF %s", qb.CQL(upperPermission),
		data.resource(), qb.QName(data.Grantee.Value))

	result, err := r.provider.execute(stmt.String(), nil)
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") {
			// role or table does not exist, so the grant does not exist either.
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError("Query error", fmt.Sprintf("Unable to read grant:\n%s\n%s",
			stmt.String(), err))
		return
	}

	colRole, err := findColumn("role", result.ColSpec)
	if err != nil {
		resp.Diagnostics.AddError("Query error", err.Error())
		return
	}

	colResource, err := findColumn("resource", result.ColSpec)
	if err != nil {
		resp.Diagnostics.AddError("Query error", err.Error())
		return
	}

	colPermission, err := findColumn("permission", result.ColSpec)
	if err != nil {
		resp.Diagnostics.AddError("Query error", err.Error())
		return
	}

	found := false

	expectedResource := fmt.Sprintf("<table %s.%s>", strings.ToLower(data.Keyspace.Value),
		strings.ToLower(data.Table.Value))
	for i := range result.Rows {
		role, err := result.Rows[i][colRole].AsText()
		if err != nil {
			resp.Diagnostics.AddError("Query error", err.Error())
			return
		}
		resource, err := result.Rows[i][colResource].AsText()
		if err != nil {
			resp.Diagnostics.AddError("Query error", err.Error())
			return
		}
		permission, err := result.Rows[i][colPermission].AsText()
		if err != nil {
			resp.Diagnostics.AddError("Query error", err.Error())
			return
		}
		if role == data.Grantee.Value && resource == expectedResource && permission == data.Permission.Value {
			found = true
			break
		}
	}

	if !found {
		resp.State.RemoveResource(ctx)
		return
	}

	diags = resp.State.Set(ctx, &data)
	resp.Diagnostics.Append(diags...)
}

func (r tableGrantResource) Update(ctx context.Context, req tfsdk.UpdateResourceRequest, resp *tfsdk.UpdateResourceResponse) {
	resp.Diagnostics.AddError("Update not supported", "Grant resource does not support update, only recreate")
}

func (r tableGrantResource) Delete(ctx context.Context, req tfsdk.DeleteResourceRequest, resp *tfsdk.DeleteResourceResponse) {
	var data tableGrantResourceData

	diags := req.State.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	perm := strings.ToUpper(data.Permission.Value)
	if _, ok := permissions[perm]; !ok {
		resp.Diagnostics.AddAttributeError(path.Root("permission"), "Unsupported value",
			"Permission is not one of the supported values.")
		return
	}

	var stmt qb.Builder
	stmt.Appendf("REVOKE %s ON %s FROM %s", qb.CQL(perm), data.resource(), qb.QName(data.Grantee.Value))

	_, err := r.provider.execute(stmt.String(), nil)
	if err != nil {
		resp.Diagnostics.AddError("Error revoking", fmt.Sprintf("%s\n\n%s", stmt.String(), err.Error()))
		return
	}
}

func (r tableGrantResource) ImportState(ctx context.Context, req tfsdk.ImportResourceStateRequest, resp *tfsdk.ImportResourceStateResponse) {
	tfsdk.ResourceImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

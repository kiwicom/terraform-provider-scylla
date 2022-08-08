package provider

import (
	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/martin-sucha/terraform-provider-scylla/internal/qb"
	"github.com/scylladb/scylla-go-driver/frame"
	"golang.org/x/crypto/bcrypt"
	"strings"
)

// Ensure provider defined types fully satisfy framework interfaces
var _ tfsdk.ResourceType = roleResourceType{}
var _ tfsdk.Resource = roleResource{}
var _ tfsdk.ResourceWithImportState = roleResource{}

type roleResourceType struct{}

func (t roleResourceType) GetSchema(ctx context.Context) (tfsdk.Schema, diag.Diagnostics) {
	return tfsdk.Schema{
		// This description is used by the documentation generator and the language server.
		MarkdownDescription: "Scylla role",

		Attributes: map[string]tfsdk.Attribute{
			"name": {
				MarkdownDescription: "Name of the role",
				Required:            true,
				Type:                types.StringType,
				PlanModifiers: []tfsdk.AttributePlanModifier{
					tfsdk.RequiresReplace(),
				},
			},
			"id": {
				Computed:            true,
				MarkdownDescription: "ID of the role",
				PlanModifiers: tfsdk.AttributePlanModifiers{
					tfsdk.UseStateForUnknown(),
				},
				Type: types.StringType,
			},
			"login": {
				MarkdownDescription: "Indicates whether the role is allowed to login. Defaults to false.",
				Required:            true,
				Type:                types.BoolType,
			},
			"superuser": {
				MarkdownDescription: "Indicates whether the user has all permissions. Defaults to false.",
				Required:            true,
				Type:                types.BoolType,
			},
			"password": {
				MarkdownDescription: "Password of the user.",
				Optional:            true,
				Type:                types.StringType,
				Sensitive:           true,
				PlanModifiers: tfsdk.AttributePlanModifiers{
					tfsdk.UseStateForUnknown(),
				},
				Computed: true,
			},
		},
	}, nil
}

func (t roleResourceType) NewResource(ctx context.Context, in tfsdk.Provider) (tfsdk.Resource, diag.Diagnostics) {
	provider, diags := convertProviderType(in)

	return roleResource{
		provider: provider,
	}, diags
}

type roleResourceData struct {
	Name      types.String `tfsdk:"name"`
	Id        types.String `tfsdk:"id"`
	Login     types.Bool   `tfsdk:"login"`
	Superuser types.Bool   `tfsdk:"superuser"`
	Password  types.String `tfsdk:"password"`
}

type roleResource struct {
	provider provider
}

func (r roleResource) Create(ctx context.Context, req tfsdk.CreateResourceRequest, resp *tfsdk.CreateResourceResponse) {
	var data roleResourceData

	diags := req.Config.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	data.Id = data.Name

	var stmt qb.Builder
	stmt.Appendf("CREATE ROLE %s", qb.QName(data.Name.Value))
	stmt.Appendf(" WITH LOGIN = %s", qb.Bool(data.Login.Value))
	stmt.Appendf(" AND SUPERUSER = %s", qb.Bool(data.Superuser.Value))
	if !data.Password.IsNull() {
		stmt.Appendf(" AND PASSWORD = %s", qb.String(data.Password.Value))
	}

	_, err := r.provider.execute(stmt.String(), nil)
	if err != nil {
		resp.Diagnostics.AddError("error creating role", err.Error())
		return
	}

	// write logs using the tflog package
	// see https://pkg.go.dev/github.com/hashicorp/terraform-plugin-log/tflog
	// for more information
	tflog.Trace(ctx, "created role")

	diags = resp.State.Set(ctx, &data)
	resp.Diagnostics.Append(diags...)
}

func (r roleResource) Read(ctx context.Context, req tfsdk.ReadResourceRequest, resp *tfsdk.ReadResourceResponse) {
	var data roleResourceData

	diags := req.State.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	cqlName, err := frame.CqlFromASCII(data.Id.Value)
	if err != nil {
		resp.Diagnostics.AddError("Cannot convert role name", err.Error())
		return
	}

	result, err := r.provider.execute("SELECT can_login, is_superuser, salted_hash FROM system_auth.roles WHERE role = ?",
		[]frame.CqlValue{cqlName})
	if err != nil {
		resp.Diagnostics.AddError("Query error", fmt.Sprintf("Unable to read role info: %s", err))
		return
	}

	if len(result.Rows) == 0 {
		resp.State.RemoveResource(ctx)
		return
	}

	canLogin, err := result.Rows[0][0].AsBoolean()
	if err != nil {
		resp.Diagnostics.AddError("Query result error",
			fmt.Sprintf("Unable to read role can_login: %s", err))
		return
	}
	isSuperuser, err := result.Rows[0][1].AsBoolean()
	if err != nil {
		resp.Diagnostics.AddError("Query result error",
			fmt.Sprintf("Unable to read role is_superuser: %s", err))
		return
	}
	saltedHash, err := result.Rows[0][2].AsText()
	if err != nil {
		resp.Diagnostics.AddError("Query result error",
			fmt.Sprintf("Unable to read role salted_hash: %s", err))
		return
	}

	data.Login.Null = false
	data.Login.Unknown = false
	data.Login.Value = canLogin
	data.Superuser.Null = false
	data.Superuser.Unknown = false
	data.Superuser.Value = isSuperuser

	if !data.Password.IsNull() {
		// https://github.com/scylladb/scylladb/blob/c51a41a8850ac6f595b920b65860c170b5f215b5/auth/passwords.cc
		switch {
		case strings.HasPrefix(saltedHash, "$2a$"), strings.HasPrefix(saltedHash, "$2y$"):
			err := bcrypt.CompareHashAndPassword([]byte(saltedHash), []byte(data.Password.Value))
			if err != nil {
				data.Password.Null = false
				data.Password.Unknown = true
				data.Password.Value = ""
				tflog.Warn(ctx, "Server-side password is different than what is stored in the state")
			}
		default:
			// Scheme not supported.
			// Use password from state.
		}

	}

	diags = resp.State.Set(ctx, &data)
	resp.Diagnostics.Append(diags...)
}

func (r roleResource) Update(ctx context.Context, req tfsdk.UpdateResourceRequest, resp *tfsdk.UpdateResourceResponse) {
	var plan, state roleResourceData

	diags := req.Plan.Get(ctx, &plan)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	diags = req.State.Get(ctx, &state)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	var stmt qb.Builder
	stmt.Appendf("ALTER ROLE %s", qb.QName(plan.Id.Value))
	if !plan.Login.Equal(state.Login) {
		stmt.Once("with", " WITH ", " AND ")
		stmt.Appendf("LOGIN = %s", qb.Bool(plan.Login.Value))
	}
	if !plan.Superuser.Equal(state.Superuser) {
		stmt.Once("with", " WITH ", " AND ")
		stmt.Appendf("SUPERUSER = %s", qb.Bool(plan.Superuser.Value))
	}
	if !plan.Password.Equal(state.Password) && !plan.Password.IsNull() {
		stmt.Once("with", " WITH ", " AND ")
		stmt.Appendf("PASSWORD = %s", qb.String(plan.Password.Value))
	}

	_, err := r.provider.execute(stmt.String(), nil)
	if err != nil {
		resp.Diagnostics.AddError("error altering role", err.Error())
		return
	}

	diags = resp.State.Set(ctx, &plan)
	resp.Diagnostics.Append(diags...)
}

func (r roleResource) Delete(ctx context.Context, req tfsdk.DeleteResourceRequest, resp *tfsdk.DeleteResourceResponse) {
	var data roleResourceData

	diags := req.State.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	var stmt qb.Builder
	stmt.Appendf("DROP ROLE %s", qb.QName(data.Id.Value))

	_, err := r.provider.execute(stmt.String(), nil)
	if err != nil {
		resp.Diagnostics.AddError("error dropping role", err.Error())
		return
	}
}

func (r roleResource) ImportState(ctx context.Context, req tfsdk.ImportResourceStateRequest, resp *tfsdk.ImportResourceStateResponse) {
	tfsdk.ResourceImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

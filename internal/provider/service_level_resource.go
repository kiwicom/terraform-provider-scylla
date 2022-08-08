package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"

	"github.com/kiwicom/terraform-provider-scylla/internal/qb"
)

// Ensure provider defined types fully satisfy framework interfaces
var _ tfsdk.ResourceType = serviceLevelResourceType{}
var _ tfsdk.Resource = serviceLevelResource{}
var _ tfsdk.ResourceWithImportState = serviceLevelResource{}

type serviceLevelResourceType struct{}

func (t serviceLevelResourceType) GetSchema(ctx context.Context) (tfsdk.Schema, diag.Diagnostics) {
	return tfsdk.Schema{
		// This description is used by the documentation generator and the language server.
		MarkdownDescription: "Scylla role",

		Attributes: map[string]tfsdk.Attribute{
			"name": {
				MarkdownDescription: "Name of the service level",
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
			"shares": {
				MarkdownDescription: "Number of shares granted to the service level. Values are in range 1 to 1000.",
				Optional:            true,
				Type:                types.Int64Type,
				Computed:            true,
				PlanModifiers: tfsdk.AttributePlanModifiers{
					tfsdk.UseStateForUnknown(),
				},
			},
			"workload_type": {
				MarkdownDescription: "Type of the workload. One of `unspecified`, `interactive` or `batch`.",
				Optional:            true,
				Type:                types.StringType,
				Computed:            true,
				PlanModifiers: tfsdk.AttributePlanModifiers{
					tfsdk.UseStateForUnknown(),
				},
			},
			"timeout_milliseconds": {
				MarkdownDescription: "Timeout in milliseconds.",
				Optional:            true,
				Type:                types.Int64Type,
				Computed:            true,
				PlanModifiers: tfsdk.AttributePlanModifiers{
					tfsdk.UseStateForUnknown(),
				},
			},
		},
	}, nil
}

func (t serviceLevelResourceType) NewResource(ctx context.Context, in tfsdk.Provider) (tfsdk.Resource, diag.Diagnostics) {
	provider, diags := convertProviderType(in)

	return serviceLevelResource{
		provider: provider,
	}, diags
}

type serviceLevelResourceData struct {
	Name                types.String `tfsdk:"name"`
	Id                  types.String `tfsdk:"id"`
	Shares              types.Int64  `tfsdk:"shares"`
	WorkloadType        types.String `tfsdk:"workload_type"`
	TimeoutMilliseconds types.Int64  `tfsdk:"timeout_milliseconds"`
}

func (s *serviceLevelResourceData) validate() diag.Diagnostics {
	var diags diag.Diagnostics
	if !s.Shares.IsNull() && (s.Shares.Value < 1 || s.Shares.Value > 1000) {
		diags = append(diags, diag.NewAttributeErrorDiagnostic(path.Root("shares"),
			"Out of range", "shares must be between 1 and 1000 (inclusive)."))
	}
	if !s.WorkloadType.IsNull() {
		switch s.WorkloadType.Value {
		case "unspecified", "interactive", "batch":
			// ok
		default:
			diags = append(diags, diag.NewAttributeErrorDiagnostic(path.Root("workload_type"),
				"Unsupported value",
				"workload_type must be either \"unspecified\", \"interactive\" or \"batch\""))
		}
	}
	return diags
}

type serviceLevelResource struct {
	provider provider
}

func (r serviceLevelResource) Create(ctx context.Context, req tfsdk.CreateResourceRequest, resp *tfsdk.CreateResourceResponse) {
	var data serviceLevelResourceData

	diags := req.Config.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)
	resp.Diagnostics.Append(data.validate()...)

	if resp.Diagnostics.HasError() {
		return
	}

	data.Id = data.Name

	var stmt qb.Builder
	stmt.Appendf("CREATE SERVICE LEVEL %s", qb.QName(data.Name.Value))
	if !data.Shares.IsNull() && !data.Shares.IsUnknown() {
		stmt.Once("with", " WITH ", " AND ")
		stmt.Appendf("SHARES = %s", qb.Int(int(data.Shares.Value)))
	}
	if !data.WorkloadType.IsNull() && !data.WorkloadType.IsUnknown() {
		stmt.Once("with", " WITH ", " AND ")
		stmt.Appendf("WORKLOAD_TYPE = %s", qb.String(data.WorkloadType.Value))
	}
	if !data.TimeoutMilliseconds.IsNull() && !data.TimeoutMilliseconds.IsUnknown() {
		stmt.Once("with", " WITH ", " AND ")
		stmt.Appendf("TIMEOUT = %s", qb.String(fmt.Sprintf("%dms", data.TimeoutMilliseconds.Value)))
	}

	_, err := r.provider.execute(stmt.String(), nil)
	if err != nil {
		resp.Diagnostics.AddError("error creating service level", err.Error())
		return
	}

	tflog.Trace(ctx, "created service level")

	diags = resp.State.Set(ctx, &data)
	resp.Diagnostics.Append(diags...)
}

func (r serviceLevelResource) Read(ctx context.Context, req tfsdk.ReadResourceRequest, resp *tfsdk.ReadResourceResponse) {
	var data serviceLevelResourceData

	diags := req.State.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	exists, diags := r.readData(ctx, &data)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	if !exists {
		resp.State.RemoveResource(ctx)
		return
	}

	diags = resp.State.Set(ctx, &data)
	resp.Diagnostics.Append(diags...)
}

func (r serviceLevelResource) readData(ctx context.Context, data *serviceLevelResourceData) (bool, diag.Diagnostics) {
	var stmt qb.Builder
	stmt.Appendf("LIST SERVICE LEVEL %s", qb.QName(data.Id.Value))

	result, err := r.provider.execute(stmt.String(), nil)
	if err != nil {
		return false, diag.Diagnostics{
			diag.NewErrorDiagnostic("Query error", fmt.Sprintf("Unable to read service level info: %s", err)),
		}
	}

	if len(result.Rows) == 0 {
		return false, nil
	}

	colTimeout, err := findColumn("timeout", result.ColSpec)
	if err != nil {
		return false, diag.Diagnostics{
			diag.NewErrorDiagnostic("Query error", err.Error()),
		}
	}
	valTimeout := result.Rows[0][colTimeout]

	colWorkloadType, err := findColumn("workload_type", result.ColSpec)
	if err != nil {
		return false, diag.Diagnostics{
			diag.NewErrorDiagnostic("Query error", err.Error()),
		}
	}
	valWorkloadType := result.Rows[0][colWorkloadType]

	if valTimeout.Value == nil {
		data.TimeoutMilliseconds = types.Int64{
			Null: true,
		}
	} else {
		timeout, err := valTimeout.AsDuration()
		if err != nil {
			return false, diag.Diagnostics{
				diag.NewErrorDiagnostic("Query error", fmt.Sprintf("read timeout: %s", err.Error())),
			}
		}
		// Ignore months and days from duration, timeout won't be that long.
		data.TimeoutMilliseconds = types.Int64{
			Value: timeout.Nanoseconds / 1e6,
		}
	}

	if valWorkloadType.Value == nil {
		data.WorkloadType = types.String{
			Null: true,
		}
	} else {
		workloadType, err := valWorkloadType.AsText()
		if err != nil {
			return false, diag.Diagnostics{
				diag.NewErrorDiagnostic("Query error", fmt.Sprintf("read workload_type: %s", err.Error())),
			}
		}
		data.WorkloadType = types.String{
			Value: workloadType,
		}
	}

	data.Shares = types.Int64{
		Null: true,
	}
	colShares, err := findColumn("shares", result.ColSpec)
	if err == nil {
		// shares is only available in Scylla Enterprise.
		valShares := result.Rows[0][colShares]
		if valShares.Value != nil {
			shares, err := valShares.AsInt32()
			if err != nil {
				return false, diag.Diagnostics{
					diag.NewErrorDiagnostic("Query error", fmt.Sprintf("read shares: %s", err.Error())),
				}
			}
			data.Shares = types.Int64{
				Value: int64(shares),
			}
		}
	}
	return true, nil
}

func (r serviceLevelResource) Update(ctx context.Context, req tfsdk.UpdateResourceRequest, resp *tfsdk.UpdateResourceResponse) {
	var plan, state serviceLevelResourceData

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
	stmt.Appendf("ALTER SERVICE LEVEL %s", qb.QName(plan.Id.Value))
	if !plan.Shares.Equal(state.Shares) && !plan.Shares.IsNull() && !plan.Shares.IsUnknown() {
		stmt.Once("with", " WITH ", " AND ")
		stmt.Appendf("SHARES = %s", qb.Int(int(plan.Shares.Value)))
	}
	if !plan.WorkloadType.Equal(state.WorkloadType) && !plan.WorkloadType.IsNull() && !plan.WorkloadType.IsUnknown() {
		stmt.Once("with", " WITH ", " AND ")
		stmt.Appendf("WORKLOAD_TYPE = %s", qb.String(plan.WorkloadType.Value))
	}
	if !plan.TimeoutMilliseconds.Equal(state.TimeoutMilliseconds) && !plan.TimeoutMilliseconds.IsNull() && !plan.TimeoutMilliseconds.IsUnknown() {
		stmt.Once("with", " WITH ", " AND ")
		stmt.Appendf("TIMEOUT = %s", qb.String(fmt.Sprintf("%dms", plan.TimeoutMilliseconds.Value)))
	}

	_, err := r.provider.execute(stmt.String(), nil)
	if err != nil {
		resp.Diagnostics.AddError("Error altering role", err.Error())
		return
	}

	exists, diags := r.readData(ctx, &plan)
	resp.Diagnostics.Append(diags...)

	if !exists {
		resp.State.RemoveResource(ctx)
		return
	}

	diags = resp.State.Set(ctx, &plan)
	resp.Diagnostics.Append(diags...)
}

func (r serviceLevelResource) Delete(ctx context.Context, req tfsdk.DeleteResourceRequest, resp *tfsdk.DeleteResourceResponse) {
	var data serviceLevelResourceData

	diags := req.State.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	var stmt qb.Builder
	stmt.Appendf("DROP SERVICE LEVEL %s", qb.QName(data.Id.Value))

	_, err := r.provider.execute(stmt.String(), nil)
	if err != nil {
		resp.Diagnostics.AddError("Error dropping service level", err.Error())
		return
	}
}

func (r serviceLevelResource) ImportState(ctx context.Context, req tfsdk.ImportResourceStateRequest, resp *tfsdk.ImportResourceStateResponse) {
	tfsdk.ResourceImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

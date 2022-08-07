package provider

import (
	"context"
	"fmt"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/scylladb/scylla-go-driver/frame"
	"github.com/scylladb/scylla-go-driver/transport"
	"net"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/tfsdk"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// Ensure provider defined types fully satisfy framework interfaces
var _ tfsdk.Provider = &provider{}

// provider satisfies the tfsdk.Provider interface and usually is included
// with all Resource and DataSource implementations.
type provider struct {
	// conn is used to execute the queries.
	conn *transport.Conn

	// hosts is used to establish connection.
	hosts []string

	// connConnfig holds settings for creating connection.
	connConfig transport.ConnConfig

	// configured is set to true at the end of the Configure method.
	// This can be used in Resource and DataSource implementations to verify
	// that the provider was previously configured.
	configured bool

	// version is set to the provider version on release, "dev" when the
	// provider is built and ran locally, and "test" when running acceptance
	// testing.
	version string
}

// providerData can be used to store data from the Terraform configuration.
type providerData struct {
	Hosts    types.String `tfsdk:"hosts"`
	Username types.String `tfsdk:"username"`
	Password types.String `tfsdk:"password"`
}

func (p *provider) Configure(ctx context.Context, req tfsdk.ConfigureProviderRequest, resp *tfsdk.ConfigureProviderResponse) {
	var data providerData
	diags := req.Config.Get(ctx, &data)
	resp.Diagnostics.Append(diags...)

	if resp.Diagnostics.HasError() {
		return
	}

	if data.Hosts.Value == "" {
		resp.Diagnostics.AddAttributeError(path.Root("hosts"), "No hosts configured",
			"The hosts field must contain at least one host to connect to")
	} else {
		for _, hostport := range strings.Split(data.Hosts.Value, ",") {
			p.hosts = append(p.hosts, addDefaultPort(hostport))
		}
	}

	if !data.Username.IsNull() {
		p.connConfig.Username = data.Username.Value
	}

	if !data.Password.IsNull() {
		p.connConfig.Password = data.Password.Value
	}

	// If the upstream provider SDK or HTTP client requires configuration, such
	// as authentication or logging, this is a great opportunity to do so.

	p.configured = true
}

func addDefaultPort(hostport string) string {
	_, _, err := net.SplitHostPort(hostport)
	if err == nil {
		// There already is host and port.
		return hostport
	}
	return net.JoinHostPort(hostport, "9042")
}

func (p *provider) GetResources(ctx context.Context) (map[string]tfsdk.ResourceType, diag.Diagnostics) {
	return map[string]tfsdk.ResourceType{
		"scylla_example": exampleResourceType{},
		"scylla_role":    roleResourceType{},
	}, nil
}

func (p *provider) GetDataSources(ctx context.Context) (map[string]tfsdk.DataSourceType, diag.Diagnostics) {
	return map[string]tfsdk.DataSourceType{
		"scylla_example": exampleDataSourceType{},
	}, nil
}

func (p *provider) GetSchema(ctx context.Context) (tfsdk.Schema, diag.Diagnostics) {
	return tfsdk.Schema{
		Attributes: map[string]tfsdk.Attribute{
			"hosts": {
				MarkdownDescription: "Host or hosts to connect to",
				Optional:            true,
				Type:                types.StringType,
			},
			"username": {
				MarkdownDescription: "Username for authentication",
				Optional:            true,
				Type:                types.StringType,
			},
			"password": {
				MarkdownDescription: "Password for authentication",
				Optional:            true,
				Type:                types.StringType,
				Sensitive:           true,
			},
		},
	}, nil
}

func (p *provider) initConn() error {
	if p.conn != nil {
		return nil
	}
	var lastErr error
	for _, hostport := range p.hosts {
		conn, err := transport.OpenConn(hostport, nil, p.connConfig)
		if err != nil {
			lastErr = err
			continue
		}
		p.conn = conn
		return nil
	}
	return lastErr
}

func (p *provider) execute(query string, values []frame.CqlValue) (transport.QueryResult, error) {
	err := p.initConn()
	if err != nil {
		return transport.QueryResult{}, err
	}
	frameValues := make([]frame.Value, len(values))
	for i := range values {
		frameValues[i].N = frame.Int(len(values[i].Value))
		frameValues[i].Bytes = values[i].Value
	}
	stmt := transport.Statement{
		Content:     query,
		Values:      frameValues,
		PageSize:    0,
		Consistency: frame.ONE,
	}

	return p.conn.Query(stmt, nil)
}

func New(version string) func() tfsdk.Provider {
	return func() tfsdk.Provider {
		return &provider{
			version: version,
		}
	}
}

// convertProviderType is a helper function for NewResource and NewDataSource
// implementations to associate the concrete provider type. Alternatively,
// this helper can be skipped and the provider type can be directly type
// asserted (e.g. provider: in.(*provider)), however using this can prevent
// potential panics.
func convertProviderType(in tfsdk.Provider) (provider, diag.Diagnostics) {
	var diags diag.Diagnostics

	p, ok := in.(*provider)

	if !ok {
		diags.AddError(
			"Unexpected Provider Instance Type",
			fmt.Sprintf("While creating the data source or resource, an unexpected provider type (%T) was received. This is always a bug in the provider code and should be reported to the provider developers.", p),
		)
		return provider{}, diags
	}

	if p == nil {
		diags.AddError(
			"Unexpected Provider Instance Type",
			"While creating the data source or resource, an unexpected empty provider instance was received. This is always a bug in the provider code and should be reported to the provider developers.",
		)
		return provider{}, diags
	}

	return *p, diags
}

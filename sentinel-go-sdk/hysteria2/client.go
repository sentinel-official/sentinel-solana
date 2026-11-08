package hysteria2

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"runtime"
	"strconv"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

const clientTemplate = "templates/client.yaml.tmpl"

var _ service.ClientCleaner = (*Client)(nil)

type Client struct {
	conf      string
	iface     string
	live      string
	log       zerolog.Logger
	noIPv6    bool
	placement service.Placement
	process   *service.ProcessManager
	uuid      string
}

// SetTunnelIPv6 sets whether IPv6 is routed into the tunnel, which is the default. It applies from
// the next Start. With it off, only the tunnel's own IPv6 prefix is routed, so the rest of IPv6 is
// left to the host and its firewall.
func (c *Client) SetTunnelIPv6(on bool) {
	c.noIPv6 = !on
}

func NewClient(_ ClientConfig, p service.Placement, log zerolog.Logger) (service.Client, error) {
	if err := service.ValidateInterfaceName(p.Interface); err != nil {
		return nil, err
	}

	return newClient(p, log), nil
}

func newClient(p service.Placement, log zerolog.Logger, opts ...service.ProcessManagerOption) *Client {
	log = service.ScopeLogger(log, Name)

	return &Client{
		conf:      p.ToolPath(".yaml"),
		iface:     clientInterface(p.Interface, runtime.GOOS),
		log:       log,
		placement: p,
		process:   service.NewProcessManager(binary, p.Dir, log, opts...),
	}
}

func (c *Client) Cleanup(ctx context.Context) error {
	if err := c.process.KillStale(ctx, configFlag, c.conf); err != nil {
		return err
	}

	return service.RemoveOwned(c.placement.ToolDir, c.placement.Interface)
}

func (c *Client) Interface() string {
	if c.live != "" {
		return c.live
	}

	return c.iface
}

func (c *Client) Name() service.Name {
	return Name
}

func (c *Client) PeerRequest(_ context.Context) (service.PeerRequest, error) {
	req := PeerRequest{UUID: uuid.NewString()}

	packed, err := service.PackRequest(Name, req)
	if err != nil {
		return service.PeerRequest{}, err
	}

	c.uuid = req.UUID

	return packed, nil
}

func (c *Client) ProxyEndpoint() string {
	return ""
}

func (c *Client) Start(ctx context.Context, host string, resp service.PeerResponse) error {
	if resp.Name != c.Name() {
		return fmt.Errorf("%w: service %q", service.ErrInvalidPeer, resp.Name)
	}

	p, err := service.UnpackResponse[PeerResponse](resp)
	if err != nil {
		return err
	}

	if string(resp.ID) != c.uuid || p.Auth != c.uuid {
		return fmt.Errorf("%w: peer %q", service.ErrInvalidPeer, resp.ID)
	}

	addr, err := resolveHost(ctx, host)
	if err != nil {
		return err
	}

	var before map[string]bool
	if c.iface == "" {
		before, err = service.InterfaceNames()
		if err != nil {
			return err
		}
	}

	if err := writeConf(c.conf, addr, c.iface, p, c.noIPv6); err != nil {
		return err
	}

	succeeded := false

	defer func() {
		if !succeeded {
			_ = service.RemoveOwned(c.placement.ToolDir, c.placement.Interface)
		}
	}()

	cmd := service.Command{
		Args: []string{"client", configFlag, c.conf},
		Path: binary,
	}

	probe := func(ctx context.Context) error {
		iface := c.iface
		if iface == "" {
			live, err := service.DetectNewInterface(before)
			if err != nil {
				return err
			}

			iface = live
		}

		_, err := service.LinkStats(iface) //nolint:contextcheck,nolintlint // darwin LinkStats owns its context
		if err != nil {
			return fmt.Errorf("querying stats: %w", err)
		}

		c.live = iface

		return nil
	}

	if err := c.process.Launch(ctx, cmd, probe); err != nil {
		return err
	}

	succeeded = true

	c.log.Info().Str("iface", c.Interface()).Str("process", binary).Msg("service started")

	return nil
}

func (c *Client) Stats(_ context.Context) (service.PeerStats, error) {
	if !c.process.Started() {
		return service.PeerStats{}, fmt.Errorf("querying stats: %w", service.ErrServiceNotStarted)
	}

	stats, err := service.LinkStats(c.Interface()) //nolint:contextcheck,nolintlint // darwin LinkStats owns its context
	if err != nil {
		return service.PeerStats{}, fmt.Errorf("querying stats: %w", err)
	}

	return stats, nil
}

func (c *Client) Stop(ctx context.Context) error {
	return c.process.Do(func() error {
		if !c.process.Started() {
			return nil
		}

		if err := c.process.Terminate(ctx); err != nil {
			return err
		}

		if err := service.RemoveOwned(c.placement.ToolDir, c.placement.Interface); err != nil {
			return err
		}

		c.log.Info().Str("iface", c.Interface()).Str("process", binary).Msg("service stopped")

		return nil
	})
}

func (c *Client) Wait(ctx context.Context) error {
	return c.process.Wait(ctx)
}

type clientTemplateData struct {
	Auth         string
	Endpoint     string
	ExcludeV4    string
	ExcludeV6    string
	Interface    string
	ObfsPassword string
	RouteV6      string
	TLSPin       string
}

// tunPrefixV6 is the tunnel's own IPv6 prefix (templates/client.yaml.tmpl). Routing only it keeps
// IPv6 out of the tunnel: with an empty list sing-tun would route all of IPv6.
const tunPrefixV6 = "2001::ffff:ffff:ffff:fff0/126"

func resolveHost(ctx context.Context, host string) (netip.Addr, error) {
	if addr, err := netip.ParseAddr(host); err == nil {
		return addr, nil
	}

	ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("resolving %s: %w", host, err)
	}

	if len(ips) == 0 {
		return netip.Addr{}, fmt.Errorf("resolving %s: no addresses found", host)
	}

	addr, ok := netip.AddrFromSlice(ips[0])
	if !ok {
		return netip.Addr{}, fmt.Errorf("resolving %s: invalid address", host)
	}

	return addr.Unmap(), nil
}

func writeConf(path string, addr netip.Addr, iface string, resp PeerResponse, noIPv6 bool) error {
	data := clientTemplateData{
		Auth:         resp.Auth,
		Endpoint:     net.JoinHostPort(addr.String(), strconv.Itoa(int(resp.Port))),
		Interface:    iface,
		ObfsPassword: resp.ObfsPassword,
		RouteV6:      "::/0",
		TLSPin:       resp.TLSPin,
	}

	if noIPv6 {
		data.RouteV6 = tunPrefixV6
	}

	exclude := netip.PrefixFrom(addr, addr.BitLen()).String()
	if addr.Is4() {
		data.ExcludeV4 = exclude
	} else {
		data.ExcludeV6 = exclude
	}

	return service.WriteTemplate(templates, clientTemplate, path, data, "#")
}

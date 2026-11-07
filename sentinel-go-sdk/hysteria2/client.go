package hysteria2

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"runtime"
	"strconv"
	"sync"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

const clientTemplate = "templates/client.yaml.tmpl"

var _ service.ClientCleaner = (*Client)(nil)

type Client struct {
	meter     tunMeter
	statsMu   sync.Mutex
	conf      string
	iface     string
	live      string
	log       zerolog.Logger
	placement service.Placement
	process   *service.ProcessManager
	uuid      string
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

	if err := writeConf(c.conf, addr, c.iface, p); err != nil {
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

	c.statsMu.Lock()
	defer c.statsMu.Unlock()

	return c.meter.add(stats), nil
}

// tunMeter removes the double count of the Hysteria2 tun. Its "system" TCP
// stack NATs every TCP flow through a listener on the tun's own address, so
// each TCP byte crosses the tun once in each direction (rx and tx), while UDP
// crosses once. Per sample, max(Δrx, Δtx) is exact for TCP, one-way UDP and
// any mix of the two; only bulk UDP in both directions within one sample
// undercounts (by the smaller direction). The total is reported as rx so both
// fields stay monotonic.
//
// The tun counts whole IP packets while the node meters proxied payload, so
// headers would make the client count about 5 % high. tunHeaderBytes is taken
// off per packet in the counted direction: the smallest IPv4+UDP header, so
// the client never drops below the payload it really moved.
type tunMeter struct {
	last  service.PeerStats
	total uint64
}

const tunHeaderBytes = 28

func (m *tunMeter) add(raw service.PeerStats) service.PeerStats {
	delta := func(now, prev uint64) uint64 {
		if now < prev { // interface recreated: counters restarted
			return now
		}

		return now - prev
	}

	rx, tx := delta(raw.RxBytes, m.last.RxBytes), delta(raw.TxBytes, m.last.TxBytes)
	bytes, packets := rx, delta(raw.RxPackets, m.last.RxPackets)

	if tx > rx {
		bytes, packets = tx, delta(raw.TxPackets, m.last.TxPackets)
	}

	m.last = raw
	m.total += bytes - min(bytes, packets*tunHeaderBytes)

	return service.PeerStats{RxBytes: m.total}
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
	TLSPin       string
}

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

func writeConf(path string, addr netip.Addr, iface string, resp PeerResponse) error {
	data := clientTemplateData{
		Auth:         resp.Auth,
		Endpoint:     net.JoinHostPort(addr.String(), strconv.Itoa(int(resp.Port))),
		Interface:    iface,
		ObfsPassword: resp.ObfsPassword,
		TLSPin:       resp.TLSPin,
	}

	exclude := netip.PrefixFrom(addr, addr.BitLen()).String()
	if addr.Is4() {
		data.ExcludeV4 = exclude
	} else {
		data.ExcludeV6 = exclude
	}

	return service.WriteTemplate(templates, clientTemplate, path, data, "#")
}

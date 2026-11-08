package wgcore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

const (
	clientTemplate             = "templates/client.conf.tmpl"
	livenessPollInterval       = 5 * time.Second
	persistentKeepaliveSeconds = 25
)

var errNameFileEmpty = errors.New("interface name file is empty")

var _ service.ClientCleaner = (*Client[service.Validator])(nil)

type Client[E service.Validator] struct {
	conf            string
	live            string
	log             zerolog.Logger
	noIPv6          bool
	placement       service.Placement
	privateKey      string
	publicKey       string
	runner          runner
	serverPublicKey string
	started         bool
	stop            service.StopOnce
	stopped         chan struct{}
	variant         Variant
}

func NewClient[E service.Validator](v Variant, p service.Placement, log zerolog.Logger) (service.Client, error) {
	if err := service.ValidateInterfaceName(p.Interface); err != nil {
		return nil, err
	}

	return newClient[E](v, p, log, newRunner(v)), nil
}

func newClient[E service.Validator](v Variant, p service.Placement, log zerolog.Logger, r runner) *Client[E] {
	return &Client[E]{
		conf:      p.ToolPath(".conf"),
		log:       service.ScopeLogger(log, v.Name),
		placement: p,
		runner:    r,
		stopped:   make(chan struct{}),
		variant:   v,
	}
}

func (c *Client[E]) Cleanup(ctx context.Context) error {
	owned, err := service.OwnsFile(c.conf)
	if err != nil {
		return err
	}

	if err := c.runner.Cleanup(ctx, c.conf, owned); err != nil {
		return fmt.Errorf("stopping %s: %w", c.placement.Interface, err)
	}

	return service.RemoveOwned(c.placement.ToolDir, c.placement.Interface)
}

func (c *Client[E]) Interface() string {
	if c.live != "" {
		return c.live
	}

	return c.placement.Interface
}

func (c *Client[E]) Name() service.Name {
	return c.variant.Name
}

func (c *Client[E]) PeerRequest(_ context.Context) (service.PeerRequest, error) {
	key, err := NewPrivateKey()
	if err != nil {
		return service.PeerRequest{}, err
	}

	pub, err := DerivePublicKey(key)
	if err != nil {
		return service.PeerRequest{}, err
	}

	c.privateKey = key
	c.publicKey = pub
	req := PeerRequest{PublicKey: pub}

	packed, err := service.PackRequest(c.variant.Name, req)
	if err != nil {
		return service.PeerRequest{}, err
	}

	return packed, nil
}

// SetTunnelIPv6 sets whether IPv6 is routed into the tunnel, which is the default. It applies from the
// next Start. With it off, AllowedIPs holds IPv4 only, so IPv6 is left to the host and its firewall.
func (c *Client[E]) SetTunnelIPv6(on bool) {
	c.noIPv6 = !on
}

func (c *Client[E]) ProxyEndpoint() string {
	return ""
}

func (c *Client[E]) Start(ctx context.Context, host string, resp service.PeerResponse) error {
	p, err := c.verifyResponse(resp)
	if err != nil {
		return err
	}

	data := clientTemplateData[E]{
		Addresses:        p.Addresses,
		AllowedIPs:       allowedIPs(runtime.GOOS, c.noIPv6),
		Endpoint:         net.JoinHostPort(host, strconv.Itoa(int(p.Port))),
		Extensions:       p.Extensions,
		KeepaliveSeconds: persistentKeepaliveSeconds,
		MTU:              p.MTU,
		PrivateKey:       c.privateKey,
		PublicKey:        p.PublicKey,
	}

	if err := service.WriteTemplate(c.variant.Templates, clientTemplate, c.conf, data, "#"); err != nil {
		return err
	}

	succeeded := false

	defer func() {
		if !succeeded {
			_ = service.RemoveOwned(c.placement.ToolDir, c.placement.Interface)
		}
	}()

	_ = c.runner.Down(ctx, c.conf)

	live, err := c.runner.Up(ctx, c.conf)
	if err != nil {
		downAfterFailedStart(ctx, c.runner, c.conf)

		return fmt.Errorf("starting %s: %w", c.placement.Interface, err)
	}

	probe := func(ctx context.Context) error {
		if _, err := c.runner.Transfer(ctx, live); err != nil {
			return fmt.Errorf("querying stats: %w", err)
		}

		return nil
	}

	if err := service.PollReady(ctx, probe); err != nil {
		downAfterFailedStart(ctx, c.runner, c.conf)

		return fmt.Errorf("starting %s: %w", c.placement.Interface, err)
	}

	c.live = live
	c.serverPublicKey = p.PublicKey
	c.started = true
	succeeded = true

	c.log.Info().Str("iface", c.placement.Interface).Msg("service started")

	return nil
}

func (c *Client[E]) Stats(ctx context.Context) (service.PeerStats, error) {
	if !c.started || isClosed(c.stopped) {
		return service.PeerStats{}, fmt.Errorf("querying stats: %w", service.ErrServiceNotStarted)
	}

	transfer, err := c.runner.Transfer(ctx, c.Interface())
	if err != nil {
		return service.PeerStats{}, fmt.Errorf("querying stats: %w", err)
	}

	return transfer[c.serverPublicKey], nil
}

func (c *Client[E]) Stop(ctx context.Context) error {
	return c.stop.Do(func() error {
		close(c.stopped)

		if !c.started {
			return nil
		}

		if err := c.runner.Down(ctx, c.conf); err != nil {
			return fmt.Errorf("stopping %s: %w", c.placement.Interface, err)
		}

		if err := service.RemoveOwned(c.placement.ToolDir, c.placement.Interface); err != nil {
			return err
		}

		c.log.Info().Str("iface", c.placement.Interface).Msg("service stopped")

		return nil
	})
}

func (c *Client[E]) Wait(ctx context.Context) error {
	if !c.started {
		return fmt.Errorf("waiting for %s: %w", c.placement.Interface, service.ErrServiceNotStarted)
	}

	return watchInterface(ctx, c.stopped, c.runner, c.Interface())
}

func (c *Client[E]) verifyResponse(resp service.PeerResponse) (PeerResponse[E], error) {
	if resp.Name != c.Name() {
		return PeerResponse[E]{}, fmt.Errorf("%w: service %q, want %q", service.ErrInvalidPeer, resp.Name, c.Name())
	}

	p, err := service.UnpackResponse[PeerResponse[E]](resp)
	if err != nil {
		return PeerResponse[E]{}, err
	}

	if c.publicKey == "" || string(resp.ID) != c.publicKey {
		return PeerResponse[E]{}, fmt.Errorf("%w: response peer ID %q", service.ErrInvalidPeer, resp.ID)
	}

	if p.PublicKey == c.publicKey {
		return PeerResponse[E]{}, fmt.Errorf("%w: server public key equals the client key", service.ErrInvalidPeer)
	}

	return p, nil
}

type clientTemplateData[E service.Validator] struct {
	Addresses        []string
	AllowedIPs       string
	Endpoint         string
	Extensions       E
	KeepaliveSeconds int
	MTU              uint16
	PrivateKey       string
	PublicKey        string
}

func allowedIPs(goos string, noIPv6 bool) string {
	v4, v6 := "0.0.0.0/0", "::/0"
	if goos == "windows" {
		v4, v6 = "0.0.0.0/1, 128.0.0.0/1", "::/1, 8000::/1"
	}

	if noIPv6 {
		return v4
	}

	return v4 + ", " + v6
}

func downAfterFailedStart(ctx context.Context, r runner, conf string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), service.FailedStartStopTimeout)
	defer cancel()

	_ = r.Down(ctx, conf)
}

func interfaceFromNameFile(dir, iface string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(dir, iface+".name")) //#nosec G304 -- validated interface name
	if err != nil {
		return "", fmt.Errorf("reading interface name for %s: %w", iface, err)
	}

	live := strings.TrimSpace(string(raw))
	if live == "" {
		return "", fmt.Errorf("reading interface name for %s: %w", iface, errNameFileEmpty)
	}

	return live, nil
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func liveInterface(iface, runDir string) (string, error) {
	if runtime.GOOS == "linux" {
		return iface, nil
	}

	return interfaceFromNameFile(runDir, iface)
}

func watchInterface(ctx context.Context, stopped <-chan struct{}, r runner, iface string) error {
	select {
	case <-stopped:
		return nil
	default:
	}

	ticker := time.NewTicker(livenessPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopped:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("waiting for %s: %w", iface, ctx.Err())
		case <-ticker.C:
			if _, err := r.Transfer(ctx, iface); err != nil {
				select {
				case <-stopped:
					return nil
				default:
					return fmt.Errorf("%w: %s: %w", service.ErrProcessExited, iface, err)
				}
			}
		}
	}
}

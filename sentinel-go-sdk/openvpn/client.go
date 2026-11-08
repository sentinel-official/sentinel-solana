package openvpn

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

const (
	clientTemplate = "templates/client.conf.tmpl"
	configFlag     = "--config"
	stopGrace      = 5 * time.Second
)

var _ service.ClientCleaner = (*Client)(nil)

type Client struct {
	adapter            *tapctl
	cfg                ClientConfig
	conf               string
	detectNewInterface func(map[string]bool) (string, error)
	iface              string
	interfaceNames     func() (map[string]bool, error)
	live               string
	log                zerolog.Logger
	noIPv6             bool
	placement          service.Placement
	process            *service.ProcessManager
	uuid               string
}

func NewClient(cfg ClientConfig, p service.Placement, log zerolog.Logger) (service.Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	if err := service.ValidateInterfaceName(p.Interface); err != nil {
		return nil, err
	}

	return newClient(cfg, p, log), nil
}

func newClient(
	cfg ClientConfig, p service.Placement, log zerolog.Logger, opts ...service.ProcessManagerOption,
) *Client {
	log = service.ScopeLogger(log, Name)

	var adapter *tapctl

	if runtime.GOOS == "windows" {
		adapter = newTapctl()
	}

	return &Client{
		adapter:            adapter,
		cfg:                cfg,
		conf:               p.ToolPath(".conf"),
		detectNewInterface: service.DetectNewInterface,
		iface:              clientInterface(p.Interface, runtime.GOOS),
		interfaceNames:     service.InterfaceNames,
		log:                log,
		placement:          p,
		process:            service.NewProcessManager(binary, p.Dir, log, opts...),
	}
}

func (c *Client) Cleanup(ctx context.Context) error {
	if err := c.process.KillStale(ctx, configFlag, c.conf); err != nil {
		return err
	}

	return errors.Join(
		c.deleteLeftoverAdapter(ctx),
		service.RemoveOwned(c.placement.ToolDir, c.placement.Interface),
	)
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

	if resp.ID != service.PeerID(c.uuid) || p.Username != c.uuid {
		return fmt.Errorf("%w: response %q does not match the requested uuid", service.ErrInvalidPeer, resp.ID)
	}

	var before map[string]bool

	if c.iface == "" {
		before, err = c.interfaceNames()
		if err != nil {
			return err
		}
	}

	succeeded := false

	defer func() {
		if !succeeded {
			_ = service.RemoveOwned(c.placement.ToolDir, c.placement.Interface)
		}
	}()

	conf, err := c.writeConf(host, p)
	if err != nil {
		return err
	}

	if c.adapter != nil {
		if err := c.adapter.Create(ctx, c.iface); err != nil {
			return err
		}
	}

	cmd := service.Command{
		Args: []string{configFlag, conf},
		Path: binary,
	}

	probe := func(ctx context.Context) error {
		if err := c.checkConnected(ctx); err != nil {
			return err
		}

		if c.iface != "" {
			c.live = c.iface

			return nil
		}

		live, err := c.detectNewInterface(before)
		if err != nil {
			return err
		}

		c.live = live

		return nil
	}

	if err := c.process.Launch(ctx, cmd, probe); err != nil {
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), service.FailedStartStopTimeout)
		defer cancel()

		return errors.Join(err, c.deleteAdapter(stopCtx))
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

		c.signalStop(ctx)

		if err := c.process.Terminate(ctx); err != nil {
			return err
		}

		err := errors.Join(c.deleteAdapter(ctx), service.RemoveOwned(c.placement.ToolDir, c.placement.Interface))
		if err != nil {
			return err
		}

		c.log.Info().Str("iface", c.Interface()).Str("process", binary).Msg("service stopped")

		return nil
	})
}

func (c *Client) Wait(ctx context.Context) error {
	return c.process.Wait(ctx)
}

func (c *Client) checkConnected(ctx context.Context) error {
	addr := service.LoopbackAddr(c.cfg.ManagementPort)
	dialer := net.Dialer{}

	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("dialing %s: %w", addr, err)
	}

	defer func() { _ = conn.Close() }()

	now := time.Now()

	deadline := now.Add(commandTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}

	if err := conn.SetDeadline(deadline); err != nil {
		return fmt.Errorf("setting deadline on %s: %w", addr, err)
	}

	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(now) })
	defer stop()

	if _, err := io.WriteString(conn, "state\n"); err != nil {
		return fmt.Errorf("writing state to %s: %w", addr, err)
	}

	state, err := readState(bufio.NewReader(conn))
	if err != nil {
		return fmt.Errorf("reading state from %s: %w", addr, err)
	}

	if state != connectedState {
		return fmt.Errorf("%w: %s", errTunnelNotConnected, state)
	}

	return nil
}

func (c *Client) deleteAdapter(ctx context.Context) error {
	if c.adapter == nil {
		return nil
	}

	return c.adapter.Delete(ctx, c.iface)
}

func (c *Client) deleteLeftoverAdapter(ctx context.Context) error {
	if c.adapter == nil {
		return nil
	}

	names, err := c.interfaceNames()
	if err != nil {
		return err
	}

	if !names[c.iface] {
		return nil
	}

	err = c.deleteAdapter(ctx)
	if errors.Is(err, service.ErrBinaryNotFound) {
		c.log.Warn().Err(err).Str("iface", c.iface).Msg("leftover adapter kept")

		return nil
	}

	return err
}

func (c *Client) signalStop(ctx context.Context) {
	addr := service.LoopbackAddr(c.cfg.ManagementPort)
	dialer := net.Dialer{}

	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		c.log.Warn().Err(err).Str("addr", addr).Msg("management shutdown dial failed")

		return
	}
	defer func() {
		if err := conn.Close(); err != nil {
			c.log.Warn().Err(err).Str("addr", addr).Msg("management shutdown close failed")
		}
	}()

	if err := conn.SetDeadline(time.Now().Add(commandTimeout)); err != nil {
		c.log.Warn().Err(err).Str("addr", addr).Msg("management shutdown deadline failed")

		return
	}

	if _, err := io.WriteString(conn, "signal SIGTERM\n"); err != nil {
		c.log.Warn().Err(err).Str("addr", addr).Msg("management shutdown signal failed")

		return
	}

	waitCtx, cancel := context.WithTimeout(ctx, stopGrace)
	defer cancel()

	if err := c.process.Wait(waitCtx); err != nil {
		if errors.Is(err, service.ErrProcessExited) {
			c.log.Debug().Err(err).Msg("management shutdown process exited")
		} else {
			c.log.Warn().Err(err).Msg("management shutdown wait failed")
		}
	}
}

// SetTunnelIPv6 sets whether IPv6 is routed into the tunnel, which is the default. It applies from the
// next Start. With it off, redirect-gateway takes IPv4 only, so IPv6 is left to the host and its firewall.
func (c *Client) SetTunnelIPv6(on bool) {
	c.noIPv6 = !on
}

func (c *Client) writeConf(host string, resp PeerResponse) (string, error) {
	authPath := c.placement.ToolPath(".auth.txt")
	caPath := c.placement.ToolPath(".ca.crt")
	tlsCryptPath := c.placement.ToolPath(".tls-crypt.key")

	owned, err := service.OwnsFile(c.conf)
	if err != nil {
		return "", err
	}

	if _, statErr := os.Lstat(authPath); !owned && !errors.Is(statErr, fs.ErrNotExist) {
		return "", fmt.Errorf("%w: %s", service.ErrConfigNotOwned, authPath)
	}

	dev, devNode := tunDirectives(c.iface, runtime.GOOS)
	data := clientTemplateData{
		AuthFile:       quoteConfigPath(authPath),
		CAFile:         quoteConfigPath(caPath),
		Dev:            dev,
		DevNode:        devNode,
		Host:           host,
		IPv6:           !c.noIPv6,
		ManagementPort: c.cfg.ManagementPort,
		Port:           resp.Port,
		Protocol:       resp.Protocol,
		TLSCryptFile:   quoteConfigPath(tlsCryptPath),
		TmpDir:         quoteConfigPath(c.placement.ToolDir),
	}

	if err := service.WriteTemplate(templates, clientTemplate, c.conf, data, "#"); err != nil {
		return "", err
	}

	if err := service.WriteOwned(caPath, []byte(resp.CA), "#"); err != nil {
		return "", err
	}

	if err := service.WriteOwned(tlsCryptPath, []byte(resp.TLSCrypt), "#"); err != nil {
		return "", err
	}

	auth := resp.Username + "\n" + resp.Password + "\n"
	if err := service.WriteOwned(authPath, []byte(auth), ""); err != nil {
		return "", err
	}

	return c.conf, nil
}

type clientTemplateData struct {
	AuthFile       string
	CAFile         string
	Dev            string
	DevNode        string
	Host           string
	IPv6           bool
	ManagementPort uint16
	Port           uint16
	Protocol       string
	TLSCryptFile   string
	TmpDir         string
}

func quoteConfigPath(path string) string {
	path = strings.ReplaceAll(path, `\`, `\\`)
	path = strings.ReplaceAll(path, `"`, `\"`)

	return `"` + path + `"`
}

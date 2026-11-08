package openvpn

import (
	"bufio"
	"context"
	"io"

	"github.com/rs/zerolog"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

type Management = management

func NewManagement(peers map[string]string) *management { return newManagement(peers) }

func CheckDCO(present bool, err error) []service.PreflightCheck { return checkDCO(present, err) }

func CheckVersion(out []byte) []service.PreflightCheck { return checkVersion(out) }

func ClientWriteConf(c *Client, host string, resp PeerResponse) (string, error) {
	return c.writeConf(host, resp)
}

func ManagementExec(m *management, cmd string) (string, error) { return m.exec(cmd) }

func ManagementHasClient(m *management, user, cid string) bool { return m.hasClient(user, cid) }

func ManagementServe(m *management, r *bufio.Reader, w io.Writer) error {
	return m.serve(r, w)
}

func NewClientForTest(
	cfg ClientConfig, p service.Placement, log zerolog.Logger, opts ...service.ProcessManagerOption,
) *Client {
	return newClient(cfg, p, log, opts...)
}

func NewTapctlForTest(
	exec execFunc,
	interfaceNames func() (map[string]bool, error),
	lookPath func(name string) (string, error),
) *tapctl {
	return &tapctl{exec: exec, interfaceNames: interfaceNames, lookPath: lookPath}
}

func NewTLSCryptKey() ([]byte, error) { return newTLSCryptKey() }

func ParseStatus(out string) map[string]service.PeerStats { return parseStatus(out) }

func QuoteConfigPath(path string) string { return quoteConfigPath(path) }

func SetClientAdapter(c *Client, t *tapctl) { c.adapter = t }

func SetClientInterfaceHooks(
	c *Client,
	names func() (map[string]bool, error),
	detect func(map[string]bool) (string, error),
) {
	c.interfaceNames = names
	c.detectNewInterface = detect
}

func TunDirectives(iface, goos string) (string, string) { return tunDirectives(iface, goos) }

func ValidateTLSCrypt(text string) error { return validateTLSCrypt(text) }

func SetHostRouteRunner(c *Client, goos string, run func(context.Context, service.Command) ([]byte, error)) {
	c.routes.goos = goos
	c.routes.run = run
}

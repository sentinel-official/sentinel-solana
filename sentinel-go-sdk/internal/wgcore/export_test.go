package wgcore

import (
	"github.com/rs/zerolog"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

var ErrNameFileEmpty = errNameFileEmpty

func AllowedIPs(goos string) string { return allowedIPs(goos) }

func ClientPrivateKey[E service.Validator](c *Client[E]) string { return c.privateKey }

func ClientPublicKey[E service.Validator](c *Client[E]) string { return c.publicKey }

func InterfaceFromNameFile(dir, iface string) (string, error) {
	return interfaceFromNameFile(dir, iface)
}

func NewClientForTest[E service.Validator](v Variant, p service.Placement, log zerolog.Logger, r runner) *Client[E] {
	return newClient[E](v, p, log, r)
}

func NewQuickRunnerForTest(v Variant, goos string, exec execFunc) runner {
	cli := wgCLI{binary: v.Binary, exec: exec}

	return quickRunner{
		wgCLI:           cli,
		confinable:      v.Confinable,
		goos:            goos,
		runDir:          v.RunDir,
		tunnelBinary:    v.TunnelBinary,
		userspaceBinary: v.UserspaceBinary,
	}
}

// NewQuickRunnerWithLookPathForTest is NewQuickRunnerForTest with the userspace lookup stubbed.
func NewQuickRunnerWithLookPathForTest(
	v Variant, goos string, exec execFunc, lookPath func(string) (string, error),
) runner {
	r := NewQuickRunnerForTest(v, goos, exec).(quickRunner) //nolint:forcetypeassert // built just above

	r.lookPath = lookPath

	return r
}

func NewWindowsServiceRunnerForTest(
	binary, serviceBinary, tunnelServicePrefix string,
	exec execFunc,
	installed installedFunc,
) runner {
	cli := wgCLI{binary: binary, exec: exec}

	return windowsServiceRunner{
		installed:           installed,
		serviceBinary:       serviceBinary,
		tunnelServicePrefix: tunnelServicePrefix,
		wgCLI:               cli,
	}
}

func ParseTransfer(out string) (map[string]service.PeerStats, error) { return parseTransfer(out) }

package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"

	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

const (
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = 20 * time.Second
)

type dialer struct {
	endpoint  string
	log       zerolog.Logger
	stateRoot string
}

// callError shapes a failed daemon call for the user. The raw gRPC error goes to the debug log
// (--log.level debug), so the message itself can stay in plain words.
func (d *dialer) callError(role string, tag instance.Tag, err error) error {
	endpoint := d.endpointFor(tag)
	d.log.Debug().Err(err).Str("endpoint", endpoint).Str("instance", string(tag)).Msg("daemon call failed")

	return wrapCallError(role, tag, endpoint, err, service.InterfaceNames, func() string { return d.tunnelIface(tag) })
}

func (d *dialer) Dial(ctx context.Context, tag instance.Tag) (*grpc.ClientConn, error) {
	if err := d.checkInstance(tag); err != nil {
		return nil, err
	}

	endpoint := d.endpointFor(tag)

	conn, err := dialEndpoint(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("dialing %s: %w", endpoint, err)
	}

	return conn, nil
}

func (d *dialer) checkInstance(tag instance.Tag) error {
	if d.endpoint != "" {
		return nil
	}

	available, err := instance.List(d.stateRoot)
	if err == nil && !slices.Contains(available, tag) {
		return instance.NewNotFoundError(tag, available)
	}

	return nil
}

func (d *dialer) endpointFor(tag instance.Tag) string {
	if d.endpoint != "" {
		return d.endpoint
	}

	return tag.Endpoint(runtime.GOOS)
}

// tunnelIface reads the live tunnel interface name the daemon recorded for tag in its instance
// state dir while a session is up. It names a leftover tunnel when the daemon is unreachable. An
// empty string means unknown — no record, a read error, or a custom endpoint whose state dir we do
// not resolve — and the caller falls back to the tag's default dvpn-<instance> name.
func (d *dialer) tunnelIface(tag instance.Tag) string {
	if d.endpoint != "" {
		return ""
	}

	root := d.stateRoot
	if root == "" {
		root = instance.StateRoot(runtime.GOOS)
	}

	raw, err := os.ReadFile(filepath.Join(root, string(tag), instance.TunnelIfaceFileName))
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(raw))
}

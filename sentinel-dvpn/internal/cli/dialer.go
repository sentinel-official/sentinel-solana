package cli

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"

	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
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

	return wrapCallError(role, tag, endpoint, err)
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

package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
)

const (
	daemonCallTimeout = 30 * time.Second

	roleClient   = "client"
	roleNode     = "node"
	roleProvider = "provider"
)

var errRoleNotServed = errors.New("role is not served")

// daemonDownError is the plain-words answer for a daemon that isn't running. The gRPC transport error
// stays in the chain (errors.As, status.Code) and in the debug log, not in the text the user reads.
type daemonDownError struct {
	msg string
	err error
}

func (e *daemonDownError) Error() string { return e.msg }

func (e *daemonDownError) Unwrap() error { return e.err }

type callSpec[Resp any] struct {
	AddFlags func(*pflag.FlagSet)
	Args     cobra.PositionalArgs
	Render   func(*Resp) any
	Role     string
	Short    string
	Use      string
}

type streamCall[C, Req, Resp any] func(
	C,
	context.Context,
	*Req,
	...grpc.CallOption,
) (grpc.ServerStreamingClient[Resp], error)

type unaryCall[C, Req, Resp any] func(C, context.Context, *Req, ...grpc.CallOption) (*Resp, error)

func addInstanceFlag(cmd *cobra.Command, tagFlag *string) {
	cmd.Flags().StringVar(tagFlag, "instance", "", "daemon instance `tag`: c<N>, n<N> or p<N>")
	_ = cmd.MarkFlagRequired("instance")
}

func newStreamCallCmd[C, Req, Resp any](
	daemon *dialer,
	printer *Printer,
	spec callSpec[Resp],
	newClient func(grpc.ClientConnInterface) C,
	open streamCall[C, Req, Resp],
	newRequest func(*cobra.Command, []string) (*Req, error),
) *cobra.Command {
	var tagFlag string

	run := func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		req := new(Req)

		if newRequest != nil {
			var err error

			req, err = newRequest(cmd, args)
			if err != nil {
				return err
			}
		}

		tag, err := instance.Parse(tagFlag)
		if err != nil {
			return err
		}

		conn, err := daemon.Dial(ctx, tag)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()

		stream, err := open(newClient(conn), ctx, req)
		if err != nil {
			return daemon.callError(spec.Role, tag, err)
		}

		for {
			resp, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			}

			if err != nil {
				return daemon.callError(spec.Role, tag, err)
			}

			var result any = resp
			if spec.Render != nil {
				result = spec.Render(resp)
			}

			if err := printer.Print(result); err != nil {
				return err
			}
		}
	}

	cmd := &cobra.Command{
		Args:  spec.Args,
		RunE:  run,
		Short: spec.Short,
		Use:   spec.Use,
	}
	addInstanceFlag(cmd, &tagFlag)

	if spec.AddFlags != nil {
		spec.AddFlags(cmd.Flags())
	}

	return cmd
}

func newUnaryCallCmd[C, Req, Resp any](
	daemon *dialer,
	printer *Printer,
	spec callSpec[Resp],
	newClient func(grpc.ClientConnInterface) C,
	call unaryCall[C, Req, Resp],
	newRequest func(*cobra.Command, []string) (*Req, error),
) *cobra.Command {
	var tagFlag string

	run := func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithTimeout(cmd.Context(), daemonCallTimeout)
		defer cancel()

		req := new(Req)

		if newRequest != nil {
			var err error

			req, err = newRequest(cmd, args)
			if err != nil {
				return err
			}
		}

		tag, err := instance.Parse(tagFlag)
		if err != nil {
			return err
		}

		conn, err := daemon.Dial(ctx, tag)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()

		resp, err := call(newClient(conn), ctx, req)
		if err != nil {
			return daemon.callError(spec.Role, tag, err)
		}

		if spec.Render != nil {
			return printer.Print(spec.Render(resp))
		}

		return printer.Print(resp)
	}

	cmd := &cobra.Command{
		Args:  spec.Args,
		RunE:  run,
		Short: spec.Short,
		Use:   spec.Use,
	}
	addInstanceFlag(cmd, &tagFlag)

	if spec.AddFlags != nil {
		spec.AddFlags(cmd.Flags())
	}

	return cmd
}

func wrapCallError(role string, tag instance.Tag, endpoint string, err error) error {
	if role == "" {
		role = "daemon"
	}

	if status.Code(err) == codes.Unimplemented {
		return fmt.Errorf("%s %w: %w", role, errRoleNotServed, err)
	}

	// Unavailable has two very different causes. A transport failure (gRPC could not reach the socket;
	// it wraps these as "connection error: ... transport: ...") means the daemon is not running. A
	// daemon-sent Unavailable is a plain app message (e.g. recovery pending or the daemon stopping): the
	// daemon IS running but not ready, so telling the user to start it would be wrong and could start a
	// second daemon onto the held instance. The transport detail ("dial unix ...: connect: ...") adds
	// nothing the user can act on, so it is kept in the chain and the debug log, not printed.
	if status.Code(err) == codes.Unavailable {
		if msg := status.Convert(err).Message(); strings.Contains(msg, "connection error") || strings.Contains(msg, "transport:") {
			return &daemonDownError{
				msg: fmt.Sprintf("the %s service (instance %s) isn't running: start it with `sentinel-dvpnd service start --instance %s` (or run `sentinel-dvpnd run --instance %s` in a terminal) (%s)",
					role, tag, tag, tag, endpoint),
				err: err,
			}
		}

		return fmt.Errorf("the %s daemon (instance %s) is not ready (starting up or stopping); try again (%s): %w",
			role, tag, endpoint, err)
	}

	return fmt.Errorf("calling daemon: %w", err)
}

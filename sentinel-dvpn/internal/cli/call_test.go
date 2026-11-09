package cli_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentinel-official/sentinel-dvpn/internal/cli"
	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/v1"
)

func TestNewStreamCallCmd_WatchCancellation(t *testing.T) {
	t.Parallel()

	deadlines := make(chan time.Time, 1)
	endpoint := startFakeDaemon(t, func(srv *grpc.Server) {
		daemonv1.RegisterClientServiceServer(srv, &recordingClientService{deadlines: deadlines})
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	root, _ := newRoot(t, endpoint, "client", "watch", "sess-1", "--instance", "c0")

	done := make(chan error, 1)
	go func() { done <- root.ExecuteContext(ctx) }()

	require.True(t, receiveCall(t, deadlines).IsZero(), "watch streams must retain the caller's unbounded context")
	cancel()
	require.Equal(t, codes.Canceled, status.Code(receiveCall(t, done)))
}

func TestNewUnaryCallCmd_BoundsDeadline(t *testing.T) {
	t.Parallel()

	for _, parentTimeout := range []time.Duration{0, 5 * time.Second} {
		name := "default deadline"
		if parentTimeout != 0 {
			name = "parent deadline"
		}

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			deadlines := make(chan time.Time, 1)
			endpoint := startFakeDaemon(t, func(srv *grpc.Server) {
				daemonv1.RegisterDaemonServiceServer(srv, &recordingDaemonService{deadlines: deadlines})
			})
			ctx := t.Context()

			if parentTimeout != 0 {
				var cancel context.CancelFunc

				ctx, cancel = context.WithTimeout(ctx, parentTimeout)
				defer cancel()
			}

			root, _ := newRoot(t, endpoint, "daemon", "version", "--instance", "c0")
			started := time.Now()

			require.NoError(t, root.ExecuteContext(ctx))

			deadline := receiveCall(t, deadlines)
			require.False(t, deadline.IsZero(), "unary daemon calls must have a deadline")

			if parentTimeout != 0 {
				parentDeadline, _ := ctx.Deadline()
				require.WithinDuration(t, parentDeadline, deadline, time.Second)
			} else {
				require.WithinDuration(t, started.Add(30*time.Second), deadline, time.Second)
			}
		})
	}
}

func TestNewUnaryCallCmd_DaemonCommandsThroughRoot(t *testing.T) {
	t.Parallel()

	endpoint := startFullFakeDaemon(t)

	cases := []struct {
		args []string
		want string
	}{
		{
			args: []string{"daemon", "version", "--instance", "c0"},
			want: `{"commit":"test-commit","tag":"",` +
				`"network":"test-network","program_id":"","mint":""}`,
		},
		{args: nodeArgs("close-session", "ns-1"), want: `{}`},
		{args: nodeArgs("heartbeat"), want: `{}`},
		{
			args: nodeArgs("positions"),
			want: `{"positions":[{"address":"pos-1","kind":"channel","owner":"",` +
				`"mode":0,"status":"","escrow":0,"redeemed":0,"cumulative":0,"opened_at":0}]}`,
		},
		{args: nodeArgs("redeem"), want: `{"signatures":["sig-1"]}`},
		{
			args: nodeArgs("redemptions"),
			want: `{"redemptions":[{"signature":"sig-1","kind":"","subjects":[],` +
				`"status":"confirmed","sent_at":0,"confirmed_at":0,"error":""}]}`,
		},
		{
			args: nodeArgs("session", "ns-1"),
			want: `{"session":{"id":"ns-1","credential":"","subject":"",` +
				`"peer_id":"","usage":null,"status":"active","created_at":0,"updated_at":0}}`,
		},
		{
			args: nodeArgs("sessions"),
			want: `{"sessions":[{"id":"ns-1","credential":"","subject":"",` +
				`"peer_id":"","usage":null,"status":"active","created_at":0,"updated_at":0}]}`,
		},
		{
			args: nodeArgs("status"),
			want: `{"info":{"address":"node-addr","id":"","owner":"",` +
				`"operator":"","status":"active","listed_at":0,"live":true,"service":"","peers":0,"prices":null}}`,
		},
		{args: nodeArgs("sync"), want: `{}`},
		{args: providerArgs("keep"), want: `{}`},
		{
			args: providerArgs("leases"),
			want: `{"leases":[{"address":"lease-1","node":"","mode":0,` +
				`"escrow":0,"cumulative":0,"opened_at":0,"status":"open","healthy":true}]}`,
		},
		{
			args: providerArgs("nodes"),
			want: `{"nodes":[{"node":"node-addr","plan":"plan-addr",` +
				`"live":true,"probed_at":0,"lease":""}]}`,
		},
		{args: providerArgs("probe"), want: `{}`},
		{
			args: providerArgs("status"),
			want: `{"info":{"address":"prov-addr","id":"","owner":"",` +
				`"operator":"","status":"listed","nodes":0,"leases":0}}`,
		},
		{
			args: providerArgs("tickets"),
			want: `{"tickets":[{"hash":"hash-1","subscription":"",` +
				`"user":"user-addr","node":"","issued_at":0,"expires_at":0,"data_cap_bytes":0}]}`,
		},
		{
			args: clientArgs("channels"),
			want: `{"channels":[{"address":"chan-1","node":"node-addr",` +
				`"mode":0,"escrow":0,"cumulative":0,"opened_at":0,"status":"open"}]}`,
		},
		{
			args: clientArgs("sessions"),
			want: `{"sessions":[{"id":"sess-1","node":"node-addr",` +
				`"status":"active","service":"","usage":null,"spent":0,"started_at":null,"ended_at":null,` +
				`"error":"","channel":"","ticket_hash":"","proxy_endpoint":""}]}`,
		},
		{
			args: clientArgs("status", "sess-1"),
			want: `{"session":{"id":"sess-1","node":"node-addr","status":"active","service":"",` +
				`"usage":null,"spent":0,"started_at":null,"ended_at":null,"error":"","channel":"",` +
				`"ticket_hash":"","proxy_endpoint":"127.0.0.1:1080"}}`,
		},
		{args: clientArgs("stop", "sess-1"), want: `{"session":null,"kill_switch_released":false,"dns_revert_failed":false}`},
		{
			args: clientArgs("tickets"),
			want: `{"tickets":[{"hash":"hash-1","provider":"prov-addr",` +
				`"node":"","subscription":"","expires_at":0,"data_cap_bytes":0}]}`,
		},
	}

	for _, tc := range cases {
		root, out := newRoot(t, endpoint, tc.args...)
		require.NoError(t, root.ExecuteContext(t.Context()), tc.args)
		require.JSONEq(t, tc.want, out.String(), tc.args)
	}
}

func TestNewUnaryCallCmd_ParentCancellation(t *testing.T) {
	t.Parallel()

	deadlines := make(chan time.Time, 1)
	endpoint := startFakeDaemon(t, func(srv *grpc.Server) {
		daemonv1.RegisterDaemonServiceServer(srv, &recordingDaemonService{
			deadlines:        deadlines,
			waitCancellation: true,
		})
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	root, _ := newRoot(t, endpoint, "daemon", "version", "--instance", "c0")

	done := make(chan error, 1)
	go func() { done <- root.ExecuteContext(ctx) }()

	_ = receiveCall(t, deadlines)

	cancel()
	require.Equal(t, codes.Canceled, status.Code(receiveCall(t, done)))
}

func TestNewUnaryCallCmd_RejectsBadArgs(t *testing.T) {
	t.Parallel()

	endpoint := startFullFakeDaemon(t)

	cases := []struct {
		args []string
		want string
	}{
		{args: nodeArgs("session"), want: "accepts 1 arg(s), received 0"},
		{args: nodeArgs("close-session"), want: "accepts 1 arg(s), received 0"},
		{args: clientArgs("status"), want: "accepts 1 arg(s), received 0"},
		{args: clientArgs("stop"), want: "accepts 1 arg(s), received 0"},
		{args: []string{"daemon", "version"}, want: `required flag(s) "instance" not set`},
		{args: []string{"daemon", "version", "--instance", "x0"}, want: instance.ErrInvalidTag.Error()},
		{args: nodeArgs("heartbeat", "extra"), want: "unknown command \"extra\""},
		{args: nodeArgs("session", "nope"), want: "calling daemon: rpc error: code = NotFound"},
		{args: clientArgs("stop", "nope"), want: "calling daemon: rpc error: code = NotFound"},
	}

	for _, tc := range cases {
		root, _ := newRoot(t, endpoint, tc.args...)
		require.ErrorContains(t, root.ExecuteContext(t.Context()), tc.want, tc.args)
	}
}

func TestWrapCallError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		err  error
		name string
		want string
	}{
		{
			err:  status.Error(codes.Unimplemented, "method X not implemented"),
			name: "unimplemented",
			want: "node role is not served: rpc error: code = Unimplemented desc = method X not implemented",
		},
		{
			err:  status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial unix /tmp/daemon.sock: connect: no such file or directory"`),
			name: "unavailable_transport",
			want: "the node service (instance c0) isn't running: start it with `sentinel-dvpnd service start --instance c0` (or run `sentinel-dvpnd run --instance c0` in a terminal) (/tmp/daemon.sock)",
		},
		{
			err:  status.Error(codes.Unavailable, "unavailable: the kill switch and sessions a previous run left are still being recovered"),
			name: "unavailable_recovery",
			want: "the node daemon (instance c0) is not ready (starting up or stopping); try again (/tmp/daemon.sock): rpc error: code = Unavailable desc = unavailable: the kill switch and sessions a previous run left are still being recovered",
		},
		{
			err:  status.Error(codes.Internal, "boom"),
			name: "other",
			want: "calling daemon: rpc error: code = Internal desc = boom",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := cli.WrapCallError("node", instance.Tag("c0"), "/tmp/daemon.sock", tc.err, func() (map[string]bool, error) { return nil, nil }, func() string { return "" })
			require.EqualError(t, got, tc.want)
			require.Equal(t, status.Code(tc.err), status.Code(got), "the gRPC code stays readable through the wrap")
		})
	}
}

func TestWrapCallError_LeftoverTunnel(t *testing.T) {
	t.Parallel()

	transportErr := status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial unix /tmp/daemon.sock: connect: no such file or directory"`)
	up := func() (map[string]bool, error) { return map[string]bool{"dvpn-c0": true}, nil }

	got := cli.WrapCallError("node", instance.Tag("c0"), "/tmp/daemon.sock", transportErr, up, func() string { return "" })
	require.EqualError(t, got, "the node service (instance c0) isn't running, but a tunnel it started (dvpn-c0) is still up; start the service to recover and remove it: `sentinel-dvpnd service start --instance c0` (or run `sentinel-dvpnd run --instance c0` in a terminal) (/tmp/daemon.sock)")
	require.ErrorIs(t, got, transportErr, "the raw error stays in the chain")
}

func TestWrapCallError_LeftoverTunnel_LiveName(t *testing.T) {
	t.Parallel()

	transportErr := status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial unix /tmp/daemon.sock: connect: no such file or directory"`)
	// The daemon recorded a kernel-assigned name (a macOS utun) that is NOT dvpn-c0; only reading
	// the recorded live name lets the warning name it. ifaces reports that interface present.
	up := func() (map[string]bool, error) { return map[string]bool{"utun7": true}, nil }
	live := func() string { return "utun7" }

	got := cli.WrapCallError("node", instance.Tag("c0"), "/tmp/daemon.sock", transportErr, up, live)
	require.EqualError(t, got, "the node service (instance c0) isn't running, but a tunnel it started (utun7) is still up; start the service to recover and remove it: `sentinel-dvpnd service start --instance c0` (or run `sentinel-dvpnd run --instance c0` in a terminal) (/tmp/daemon.sock)")
}

func TestWrapCallError_LeftoverTunnel_StaleGone(t *testing.T) {
	t.Parallel()

	transportErr := status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial unix /tmp/daemon.sock: connect: no such file or directory"`)
	// The marker names utun7, but the interface is already gone (ifaces does not report it). A stale
	// marker must NOT produce a warning — the user gets the plain "isn't running" message.
	up := func() (map[string]bool, error) { return map[string]bool{}, nil }
	live := func() string { return "utun7" }

	got := cli.WrapCallError("node", instance.Tag("c0"), "/tmp/daemon.sock", transportErr, up, live)
	require.EqualError(t, got, "the node service (instance c0) isn't running: start it with `sentinel-dvpnd service start --instance c0` (or run `sentinel-dvpnd run --instance c0` in a terminal) (/tmp/daemon.sock)")
}

// A command run while its daemon is stopped printed the plain "isn't running" line followed by the raw
// gRPC dial error. The line is now plain words only; the raw error is logged at debug.
func TestDialer_CallError_PlainWordsAndTheRawErrorAtDebug(t *testing.T) {
	t.Parallel()

	raw := status.Error(codes.Unavailable,
		`connection error: desc = "transport: Error while dialing: dial unix /tmp/daemon.sock: connect: connection refused"`)

	var logged bytes.Buffer

	got := cli.CallError(&logged, "/tmp/daemon.sock", "client", instance.Tag("c0"), raw)

	require.EqualError(t, got, "the client service (instance c0) isn't running: start it with "+
		"`sentinel-dvpnd service start --instance c0` (or run `sentinel-dvpnd run --instance c0` in a terminal) (/tmp/daemon.sock)")
	require.NotContains(t, got.Error(), "rpc error")
	require.ErrorIs(t, got, raw, "the raw error stays in the chain")
	require.Contains(t, logged.String(), "connection refused", "the raw error is in the debug log")
}

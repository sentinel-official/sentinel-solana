package openvpn_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/openvpn"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
)

type failingStopProcess struct{}

func (failingStopProcess) Done() <-chan struct{} { return nil }

func (failingStopProcess) Err() error { return nil }

func (failingStopProcess) PID() int { return 4242 }

func (failingStopProcess) Stop(ctx context.Context) error {
	<-ctx.Done()

	return fmt.Errorf("stop blocked: %w", ctx.Err())
}

func (failingStopProcess) Wait(context.Context) error { return nil }

type recordingTapctl struct {
	canceled bool
	verbs    []string
}

func (r *recordingTapctl) attach(c *openvpn.Client) {
	names := func() (map[string]bool, error) { return nil, nil }
	openvpn.SetClientAdapter(c, openvpn.NewTapctlForTest(r.run, names, fakeLookPath))
}

func (r *recordingTapctl) run(ctx context.Context, c service.Command) ([]byte, error) {
	r.canceled = r.canceled || ctx.Err() != nil
	r.verbs = append(r.verbs, c.Args[0]+" "+c.Args[len(c.Args)-1])

	return nil, nil
}

func TestClient_Cleanup_DeletesLeftoverAdapter(t *testing.T) {
	t.Parallel()

	run := func(listed map[string]bool) []string {
		recorder := &recordingTapctl{}
		p := service.Placement{Dir: t.TempDir(), Interface: "dvpn-c0", ToolDir: t.TempDir()}
		c := openvpn.NewClientForTest(openvpn.DefaultClientConfig(), p, zerolog.Nop())
		recorder.attach(c)
		openvpn.SetClientInterfaceHooks(c, func() (map[string]bool, error) { return listed, nil }, nil)

		require.NoError(t, c.Cleanup(t.Context()))

		return recorder.verbs
	}

	require.Equal(t, []string{"delete dvpn-c0"}, run(map[string]bool{"dvpn-c0": true}))
	require.Empty(t, run(map[string]bool{"dvpn-c1": true}))
}

func TestClient_Cleanup_RemovesToolCopies(t *testing.T) {
	t.Parallel()

	stateDir, toolDir := t.TempDir(), t.TempDir()
	owned := []byte("# " + service.ManagedMarker + "\nbody\n")

	for _, dir := range []string{stateDir, toolDir} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "dvpn-c0.conf"), owned, 0o600))
	}

	p := service.Placement{Dir: stateDir, Interface: "dvpn-c0", ToolDir: toolDir}
	c := openvpn.NewClientForTest(openvpn.DefaultClientConfig(), p, zerolog.Nop())
	require.NoError(t, c.Cleanup(t.Context()))
	require.NoFileExists(t, filepath.Join(toolDir, "dvpn-c0.conf"))
	require.FileExists(t, filepath.Join(stateDir, "dvpn-c0.conf"))
}

func TestClient_Cleanup_SkipsAdapterWithoutOpenVPN(t *testing.T) {
	t.Parallel()

	recorder := &recordingTapctl{}
	failingLookPath := func(string) (string, error) { return "", service.ErrBinaryNotFound }
	fakeInterfaceNames := func() (map[string]bool, error) { return map[string]bool{adapterName: true}, nil }
	p := service.Placement{Dir: t.TempDir(), Interface: adapterName, ToolDir: t.TempDir()}
	c := openvpn.NewClientForTest(openvpn.DefaultClientConfig(), p, zerolog.Nop())
	openvpn.SetClientAdapter(c, openvpn.NewTapctlForTest(recorder.run, nil, failingLookPath))
	openvpn.SetClientInterfaceHooks(c, fakeInterfaceNames, nil)

	require.NoError(t, c.Cleanup(t.Context()))
	require.Empty(t, recorder.verbs)
}

func TestClient_Start_CanceledReadinessCleansUp(t *testing.T) {
	t.Parallel()

	listener, _ := startClientManagement(t, "RECONNECTING,wait", nil)

	client, process, input, placement := newProcessClient(t, listener, false)
	defer func() { _ = input.Close() }()

	recorder := &recordingTapctl{}
	recorder.attach(client)

	// The management stub never reports CONNECTED, so Start always waits out this deadline. It has to
	// leave room for the launch itself: with a few hundred ms, a loaded machine could reach the deadline
	// before openvpn started, and then there was no process to check (nil).
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	err := client.Start(ctx, "203.0.113.1", peerResponseFixture(t, client))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotNil(t, process(), "openvpn was started before the deadline")
	require.ErrorIs(t, process().Wait(t.Context()), service.ErrProcessExited)
	require.NoFileExists(t, placement.ToolPath(".conf"))
	require.Equal(t, []string{"create dvpn-c0", "delete dvpn-c0"}, recorder.verbs)
	require.False(t, recorder.canceled)
}

func TestClient_Start_CreatesDCOAdapter(t *testing.T) {
	t.Parallel()

	listener, _ := startClientManagement(t, "CONNECTED,SUCCESS", nil)

	client, _, input, _ := newProcessClient(t, listener, false)
	defer func() { _ = input.Close() }()

	recorder := &recordingTapctl{}
	recorder.attach(client)

	require.NoError(t, client.Start(t.Context(), "203.0.113.1", peerResponseFixture(t, client)))
	require.Equal(t, []string{"create dvpn-c0"}, recorder.verbs)
	require.NoError(t, client.Stop(t.Context()))
	require.Equal(t, []string{"create dvpn-c0", "delete dvpn-c0"}, recorder.verbs)
}

func TestClient_Start_DetectsInterface(t *testing.T) {
	t.Parallel()

	var input io.Writer

	listener, signals := startClientManagement(t, "CONNECTED,SUCCESS", func() {
		_, _ = io.WriteString(input, "exit\n")
	})
	client, process, writer, _ := newProcessClient(t, listener, true)

	input = writer
	defer func() { _ = writer.Close() }()

	cfgBefore := map[string]bool{"lo": true}

	var discovered bool

	openvpn.SetClientInterfaceHooks(client, func() (map[string]bool, error) {
		return cfgBefore, nil
	}, func(before map[string]bool) (string, error) {
		require.Equal(t, cfgBefore, before)

		discovered = true

		return "utun7", nil
	})

	require.NoError(t, client.Start(t.Context(), "203.0.113.1", peerResponseFixture(t, client)))
	require.True(t, discovered)
	require.Equal(t, "utun7", client.Interface())

	require.NoError(t, client.Stop(t.Context()))
	require.NoError(t, process().Wait(t.Context()))
	requireSignal(t, signals)
}

func TestClient_Start_ProcessExitInterruptsStalledManagementRead(t *testing.T) {
	t.Parallel()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	releaseCtx, release := context.WithCancel(t.Context())
	requested := make(chan error, 1)

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)

		conn, err := listener.Accept()
		if err != nil {
			requested <- err

			return
		}

		defer func() { _ = conn.Close() }()

		_ = conn.SetReadDeadline(time.Now().Add(time.Second))

		line, err := bufio.NewReader(conn).ReadString('\n')
		if err == nil && line != "state\n" {
			err = fmt.Errorf("unexpected command %q", line)
		}

		requested <- err

		<-releaseCtx.Done()
	}()

	defer func() { release(); _ = listener.Close(); <-serverDone }()

	client, _, writer, _ := newProcessClient(t, listener, false)
	defer func() { _ = writer.Close() }()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	result := make(chan error, 1)

	resp := peerResponseFixture(t, client)
	go func() { result <- client.Start(ctx, "203.0.113.1", resp) }()

	select {
	case err := <-requested:
		require.NoError(t, err)
	case <-ctx.Done():
		require.FailNow(t, "management state request was not received")
	}

	_, err = io.WriteString(writer, "exit\n")
	require.NoError(t, err)

	select {
	case err := <-result:
		require.ErrorIs(t, err, service.ErrProcessExited)
	case <-time.After(2 * time.Second):
		release()
		cancel()
		require.Error(t, <-result)
		require.FailNow(t, "process exit did not interrupt the management read")
	}
}

func TestClient_Start_RejectsForeignResponse(t *testing.T) {
	t.Parallel()

	foreign := "00000000-0000-4000-8000-000000000009"

	cases := []struct {
		edit func(t *testing.T, resp *service.PeerResponse)
		name string
	}{
		{
			edit: func(_ *testing.T, resp *service.PeerResponse) { resp.ID = service.PeerID(foreign) },
			name: "foreign id",
		},
		{
			edit: func(t *testing.T, resp *service.PeerResponse) {
				t.Helper()

				var p openvpn.PeerResponse
				require.NoError(t, resp.Unpack(&p))

				p.Username = foreign
				packed, err := service.PackResponse(openvpn.Name, resp.ID, p)
				require.NoError(t, err)

				*resp = packed
			},
			name: "foreign username",
		},
		{
			edit: func(_ *testing.T, resp *service.PeerResponse) { resp.Name = "wireguard" },
			name: "foreign service",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			start := func(context.Context, service.Command, zerolog.Logger) (service.Process, error) {
				return nil, errors.New("process started")
			}
			client := openvpn.NewClientForTest(
				openvpn.DefaultClientConfig(), service.Placement{Dir: t.TempDir(), Interface: "dvpn-c0"}, zerolog.Nop(),
				service.WithProcessStarter(start),
			)
			resp := peerResponseFixture(t, client)
			tc.edit(t, &resp)

			require.ErrorIs(t, client.Start(t.Context(), "203.0.113.1", resp), service.ErrInvalidPeer)
		})
	}
}

func TestClient_Stats_NotStarted(t *testing.T) {
	t.Parallel()

	c, err := openvpn.NewClient(
		openvpn.DefaultClientConfig(), service.Placement{Dir: t.TempDir(), Interface: "dvpn-c0"}, zerolog.Nop(),
	)
	require.NoError(t, err)

	_, err = c.Stats(t.Context())
	require.ErrorIs(t, err, service.ErrServiceNotStarted)
}

func TestClient_Stop_HonorsCanceledContext(t *testing.T) {
	t.Parallel()

	listener, _ := startClientManagement(t, "CONNECTED,SUCCESS", nil)

	cfg := openvpn.DefaultClientConfig()
	cfg.ManagementPort = uint16(listener.Addr().(*net.TCPAddr).Port)

	p := service.Placement{Dir: t.TempDir(), Interface: "dvpn-c0", ToolDir: t.TempDir()}
	client := openvpn.NewClientForTest(cfg, p, zerolog.Nop(),
		service.WithProcessStarter(func(context.Context, service.Command, zerolog.Logger) (service.Process, error) {
			return failingStopProcess{}, nil
		}),
	)
	require.NoError(t, client.Start(t.Context(), "203.0.113.1", peerResponseFixture(t, client)))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, client.Stop(ctx), context.Canceled)
}

func TestClient_Stop_NonresponsiveManagement(t *testing.T) {
	t.Parallel()

	listener, signals := startClientManagement(t, "CONNECTED,SUCCESS", nil)

	client, process, input, _ := newProcessClient(t, listener, false)
	defer func() { _ = input.Close() }()

	require.NoError(t, client.Start(t.Context(), "203.0.113.1", peerResponseFixture(t, client)))

	require.NoError(t, client.Stop(t.Context()))
	requireSignal(t, signals)
	require.ErrorIs(t, process().Wait(t.Context()), service.ErrProcessExited)
	require.NoError(t, client.Stop(t.Context()))
}

func TestClient_Stop_SignalsBeforeFallback(t *testing.T) {
	t.Parallel()

	var input io.Writer

	listener, signals := startClientManagement(t, "CONNECTED,SUCCESS", func() {
		_, _ = io.WriteString(input, "exit\n")
	})
	client, process, writer, placement := newProcessClient(t, listener, false)

	input = writer
	defer func() { _ = writer.Close() }()

	pidPath := filepath.Join(placement.Dir, "openvpn.pid")

	require.NoError(t, client.Start(t.Context(), "203.0.113.1", peerResponseFixture(t, client)))
	require.FileExists(t, pidPath)
	require.FileExists(t, placement.ToolPath(".conf"))
	require.NoError(t, client.Stop(t.Context()))
	require.NoFileExists(t, pidPath)
	require.NoFileExists(t, placement.ToolPath(".conf"))
	requireSignal(t, signals)
	require.NoError(t, process().Wait(t.Context()))
}

func TestClient_WriteConf(t *testing.T) {
	t.Parallel()

	cfg := openvpn.DefaultClientConfig()
	toolDir := filepath.Join(t.TempDir(), "OpenVPN client")
	require.NoError(t, os.Mkdir(toolDir, 0o700))

	c := openvpn.NewClientForTest(
		cfg, service.Placement{Dir: t.TempDir(), Interface: "dvpn-c0", ToolDir: toolDir}, zerolog.Nop(),
	)
	p := payloadFixture(t)

	conf, err := openvpn.ClientWriteConf(c, "203.0.113.1", p)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(toolDir, "dvpn-c0.conf"), conf)

	raw, err := os.ReadFile(conf) //#nosec G304
	require.NoError(t, err)

	quoted := func(name string) string { return openvpn.QuoteConfigPath(filepath.Join(toolDir, name)) }

	want := []string{"# " + service.ManagedMarker, "client"}

	dev, devNode := openvpn.TunDirectives("dvpn-c0", runtime.GOOS)
	want = append(want, "dev "+dev, "dev-type tun")

	if devNode != "" {
		want = append(want, `dev-node "`+devNode+`"`)
	}

	want = append(want,
		"remote 203.0.113.1 1194 udp",
		"nobind",
		"persist-key",
		"persist-tun",
		"remote-cert-tls server",
		"ca "+quoted("dvpn-c0.ca.crt"),
		"tls-crypt "+quoted("dvpn-c0.tls-crypt.key"),
		"auth-user-pass "+quoted("dvpn-c0.auth.txt"),
		"auth-nocache",
		fmt.Sprintf("management 127.0.0.1 %d", cfg.ManagementPort),
		"tmp-dir "+openvpn.QuoteConfigPath(toolDir),
		"redirect-gateway def1 ipv6 bypass-dhcp",
		`pull-filter accept "ifconfig "`,
		`pull-filter accept "ifconfig-ipv6 "`,
		`pull-filter accept "topology "`,
		`pull-filter accept "route-gateway "`,
		`pull-filter accept "tun-mtu "`,
		`pull-filter accept "ping"`,
		`pull-filter accept "cipher "`,
		`pull-filter accept "peer-id "`,
		`pull-filter accept "auth-token"`,
		`pull-filter accept "protocol-flags "`,
		`pull-filter accept "key-derivation "`,
		`pull-filter ignore ""`,
		"verb 3",
	)
	require.Equal(t, strings.Join(want, "\n")+"\n", string(raw))

	marker := "# " + service.ManagedMarker + "\n"

	files := []struct {
		name string
		want string
	}{
		{name: "dvpn-c0.ca.crt", want: marker + p.CA},
		{name: "dvpn-c0.tls-crypt.key", want: marker + p.TLSCrypt},
		{name: "dvpn-c0.auth.txt", want: p.Username + "\n" + p.Password + "\n"},
	}

	for _, tc := range files {
		path := filepath.Join(toolDir, tc.name)

		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

		content, err := os.ReadFile(path) //#nosec G304
		require.NoError(t, err)
		require.Equal(t, tc.want, string(content))
	}
}

func TestClient_WriteConf_RefusesForeignAuth(t *testing.T) {
	t.Parallel()

	toolDir := t.TempDir()
	auth := filepath.Join(toolDir, "dvpn-c0.auth.txt")
	require.NoError(t, os.WriteFile(auth, []byte("mine\n"), 0o600))

	c := openvpn.NewClientForTest(
		openvpn.DefaultClientConfig(),
		service.Placement{Dir: t.TempDir(), Interface: "dvpn-c0", ToolDir: toolDir}, zerolog.Nop(),
	)

	_, err := openvpn.ClientWriteConf(c, "203.0.113.1", payloadFixture(t))
	require.ErrorIs(t, err, service.ErrConfigNotOwned)
	require.NoFileExists(t, filepath.Join(toolDir, "dvpn-c0.conf"))

	got, err := os.ReadFile(auth) //#nosec G304
	require.NoError(t, err)
	require.Equal(t, "mine\n", string(got))
}

func TestNewClient_InvalidInterface(t *testing.T) {
	t.Parallel()

	_, err := openvpn.NewClient(openvpn.DefaultClientConfig(), service.Placement{Dir: t.TempDir()}, zerolog.Nop())
	require.ErrorIs(t, err, validate.ErrInvalidConfig)
}

func TestOpenVPNChildProcess(t *testing.T) {
	t.Parallel()

	if os.Getenv("OPENVPN_TEST_CHILD") != "1" {
		return
	}

	_, err := bufio.NewReader(os.Stdin).ReadString('\n')
	require.NoError(t, err)
}

func TestQuoteConfigPath(t *testing.T) {
	t.Parallel()

	path := `C:\Users\Jane Doe\OpenVPN\ca"root.crt`
	require.Equal(t, `"C:\\Users\\Jane Doe\\OpenVPN\\ca\"root.crt"`, openvpn.QuoteConfigPath(path))
}

func TestTunDirectives(t *testing.T) {
	t.Parallel()

	cases := []struct {
		dev     string
		devNode string
		goos    string
		name    string
	}{
		{dev: "utun", devNode: "", goos: "darwin", name: "darwin"},
		{dev: "dvpn-c0", devNode: "", goos: "linux", name: "linux"},
		{dev: "tun", devNode: "dvpn-c0", goos: "windows", name: "windows"},
	}
	for _, tc := range cases {
		dev, devNode := openvpn.TunDirectives("dvpn-c0", tc.goos)
		require.Equal(t, tc.dev, dev, tc.name)
		require.Equal(t, tc.devNode, devNode, tc.name)
	}
}

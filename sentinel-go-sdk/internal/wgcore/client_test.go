package wgcore_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/internal/wgcore"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
)

type fakeCleanupRunner struct {
	*fakeRunner

	downCalls []string
}

func (f *fakeCleanupRunner) Cleanup(ctx context.Context, conf string, owned bool) error {
	if !owned {
		return nil
	}

	return f.Down(ctx, conf)
}

func (f *fakeCleanupRunner) Down(ctx context.Context, conf string) error {
	f.downCalls = append(f.downCalls, conf)

	return f.fakeRunner.Down(ctx, conf)
}

func TestAllowedIPs(t *testing.T) {
	t.Parallel()

	require.Equal(t, "0.0.0.0/0, ::/0", wgcore.AllowedIPs("linux"))
	require.Equal(t, "0.0.0.0/0, ::/0", wgcore.AllowedIPs("darwin"))
	require.Equal(t, "0.0.0.0/1, 128.0.0.0/1, ::/1, 8000::/1", wgcore.AllowedIPs("windows"))
}

func TestAllowedIPs_NoIPv6(t *testing.T) {
	t.Parallel()

	require.Equal(t, "0.0.0.0/0", wgcore.AllowedIPsNoIPv6("linux"))
	require.Equal(t, "0.0.0.0/0", wgcore.AllowedIPsNoIPv6("darwin"))
	require.Equal(t, "0.0.0.0/1, 128.0.0.0/1", wgcore.AllowedIPsNoIPv6("windows"))
}

func TestClient_Cleanup_DownsAndRemovesConf(t *testing.T) {
	t.Parallel()

	p := clientPlacementFixture(t)
	conf := filepath.Join(p.ToolDir, "dvpn-c0.conf")
	require.NoError(t, os.WriteFile(conf, []byte("# "+service.ManagedMarker+"\n[Interface]\n"), 0o600))

	r := &fakeCleanupRunner{fakeRunner: &fakeRunner{}}
	c := wgcore.NewClientForTest[plainExtensions](plainVariantFixture(), p, zerolog.Nop(), r)

	require.NoError(t, c.Cleanup(t.Context()))
	require.Equal(t, []string{conf}, r.down)
	_, err := os.Stat(conf)
	require.ErrorIs(t, err, os.ErrNotExist)

	require.NoError(t, os.WriteFile(conf, []byte("# "+service.ManagedMarker+"\n[Interface]\n"), 0o600))

	r.downErr = errors.New("teardown denied")

	require.ErrorContains(t, c.Cleanup(t.Context()), "teardown denied")
	require.FileExists(t, conf)

	r.downErr = os.ErrNotExist

	require.ErrorIs(t, c.Cleanup(t.Context()), os.ErrNotExist)
	require.FileExists(t, conf)

	r.downErr = nil

	require.NoError(t, c.Cleanup(t.Context()))
	require.Equal(t, []string{conf, conf, conf, conf}, r.downCalls)
	_, err = os.Stat(conf)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, c.Cleanup(t.Context()))
	require.Len(t, r.downCalls, 4)
}

func TestClient_Cleanup_IgnoresNotOwnedConf(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
	}{
		{name: "unmanaged"},
		{name: "symlink"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.name == "symlink" && runtime.GOOS == "windows" {
				t.Skip("symlink creation requires privileges on Windows")
			}

			p := clientPlacementFixture(t)
			conf := filepath.Join(p.ToolDir, "dvpn-c0.conf")

			if tc.name == "unmanaged" {
				require.NoError(t, os.WriteFile(conf, []byte("[Interface]\n"), 0o600))
			} else {
				target := filepath.Join(p.ToolDir, "foreign.conf")
				require.NoError(t, os.WriteFile(target, []byte("# "+service.ManagedMarker+"\n"), 0o600))
				require.NoError(t, os.Symlink(target, conf))
			}

			r := &fakeCleanupRunner{fakeRunner: &fakeRunner{}}
			c := wgcore.NewClientForTest[plainExtensions](plainVariantFixture(), p, zerolog.Nop(), r)
			require.NoError(t, c.Cleanup(t.Context()))
			require.Empty(t, r.downCalls)

			_, err := os.Lstat(conf)
			require.NoError(t, err)
		})
	}
}

func TestClient_Cleanup_QuickRunner(t *testing.T) {
	t.Parallel()

	if runtime.GOOS != "linux" {
		t.Skip("the live interface is the config name on linux only")
	}

	cases := []struct {
		held bool
		name string
	}{
		{held: false, name: "interface gone"},
		{held: true, name: "down fails"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := clientPlacementFixture(t)
			conf := filepath.Join(p.ToolDir, "dvpn-c0.conf")
			require.NoError(t, os.WriteFile(conf, []byte("# "+service.ManagedMarker+"\n[Interface]\n"), 0o600))

			downErr := errors.New("running wg-quick: exit status 1")
			exec := func(_ context.Context, cmd service.Command) ([]byte, error) {
				switch {
				case cmd.Args[0] == "down":
					return nil, downErr
				case !tc.held:
					return nil, errors.New("running wg: exit status 1: Unable to access interface: No such device")
				default:
					return nil, nil
				}
			}
			r := wgcore.NewQuickRunnerForTest(plainVariantFixture(), "linux", exec)
			c := wgcore.NewClientForTest[plainExtensions](plainVariantFixture(), p, zerolog.Nop(), r)

			err := c.Cleanup(t.Context())

			if !tc.held {
				require.NoError(t, err)
				require.NoFileExists(t, conf)

				return
			}

			require.ErrorIs(t, err, downErr)
			require.ErrorContains(t, err, "stopping dvpn-c0: wg-quick down "+conf)
			require.FileExists(t, conf)
		})
	}
}

func TestClient_Interface_Live(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		p := clientPlacementFixture(t)
		r := &fakeRunner{live: "utun7"}
		c := wgcore.NewClientForTest[plainExtensions](plainVariantFixture(), p, zerolog.Nop(), r)
		require.Equal(t, p.Interface, c.Interface())

		_, err := c.PeerRequest(t.Context())
		require.NoError(t, err)
		require.NoError(t, c.Start(t.Context(), "203.0.113.1", peerResponseFixture(t, wgcore.ClientPublicKey(c))))
		require.Equal(t, "utun7", c.Interface())

		_, err = c.Stats(t.Context())
		require.NoError(t, err)
		require.Equal(t, []string{"utun7", "utun7"}, r.queried)

		r.transferErr = errors.New("interface disappeared")

		require.ErrorIs(t, c.Wait(t.Context()), service.ErrProcessExited)
		require.Equal(t, []string{"utun7", "utun7", "utun7"}, r.queried)
	})
}

func TestClient_Start_ConfigNotOwned(t *testing.T) {
	t.Parallel()

	p := clientPlacementFixture(t)
	r := &fakeRunner{}

	conf := filepath.Join(p.ToolDir, "dvpn-c0.conf")
	foreign := "[Interface]\nAddress = 10.1.0.1/24\n"
	require.NoError(t, os.WriteFile(conf, []byte(foreign), 0o600))

	c := wgcore.NewClientForTest[plainExtensions](plainVariantFixture(), p, zerolog.Nop(), r)
	_, err := c.PeerRequest(t.Context())
	require.NoError(t, err)

	err = c.Start(t.Context(), "203.0.113.1", peerResponseFixture(t, wgcore.ClientPublicKey(c)))
	require.ErrorIs(t, err, service.ErrConfigNotOwned)
	require.ErrorContains(t, err, conf)
	require.Empty(t, r.up)
	require.Empty(t, r.down)

	raw, err := os.ReadFile(conf) //#nosec G304
	require.NoError(t, err)
	require.Equal(t, foreign, string(raw))
}

func TestClient_Start_NotReady(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		p := clientPlacementFixture(t)
		r := &fakeRunner{live: "utun7", transferErr: errors.New("no such device")}

		c := wgcore.NewClientForTest[plainExtensions](plainVariantFixture(), p, zerolog.Nop(), r)

		_, err := c.PeerRequest(t.Context())
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
		defer cancel()

		err = c.Start(ctx, "203.0.113.1", peerResponseFixture(t, wgcore.ClientPublicKey(c)))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorContains(t, err, "starting dvpn-c0")
		require.Equal(t, p.Interface, c.Interface())
		require.NotEmpty(t, r.queried)

		for _, iface := range r.queried {
			require.Equal(t, "utun7", iface)
		}

		conf := filepath.Join(p.ToolDir, "dvpn-c0.conf")
		wantDown := []string{conf, conf}
		require.Equal(t, wantDown, r.down)
		require.Len(t, r.downRemaining, 2)
		require.Positive(t, r.downRemaining[1])
		require.LessOrEqual(t, r.downRemaining[1], 10*time.Second)

		_, err = os.Stat(conf)
		require.True(t, os.IsNotExist(err))
	})
}

func TestClient_Start_RejectsInvalidPeer(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		resp func(t *testing.T, clientKey string) service.PeerResponse
	}{
		{
			name: "foreign peer id",
			resp: func(t *testing.T, _ string) service.PeerResponse {
				t.Helper()

				return peerResponseFixture(t, mustPublicKey(t))
			},
		},
		{
			name: "wrong name",
			resp: func(t *testing.T, clientKey string) service.PeerResponse {
				t.Helper()

				resp := peerResponseFixture(t, clientKey)
				resp.Name = "other"

				return resp
			},
		},
		{
			name: "own public key",
			resp: func(t *testing.T, clientKey string) service.PeerResponse {
				t.Helper()

				payload := peerPayloadFixture(t)
				payload.PublicKey = clientKey
				resp, err := service.PackResponse(service.Name(fakeName), service.PeerID(clientKey), payload)
				require.NoError(t, err)

				return resp
			},
		},
		{
			name: "invalid payload",
			resp: func(t *testing.T, clientKey string) service.PeerResponse {
				t.Helper()

				payload := peerPayloadFixture(t)
				payload.Addresses = []string{"10.0.0.0/8"}
				resp, err := service.PackResponse(service.Name(fakeName), service.PeerID(clientKey), payload)
				require.NoError(t, err)

				return resp
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := clientPlacementFixture(t)
			r := &fakeRunner{}
			c := wgcore.NewClientForTest[plainExtensions](plainVariantFixture(), p, zerolog.Nop(), r)
			_, err := c.PeerRequest(t.Context())
			require.NoError(t, err)

			err = c.Start(t.Context(), "203.0.113.1", tc.resp(t, wgcore.ClientPublicKey(c)))
			require.ErrorIs(t, err, service.ErrInvalidPeer)
			require.Empty(t, r.up)
			require.NoFileExists(t, filepath.Join(p.ToolDir, "dvpn-c0.conf"))
		})
	}
}

func TestClient_Start_RendersOnlyKnownLines(t *testing.T) {
	t.Parallel()

	directives := []string{
		"Address = ", "PrivateKey = ", "MTU = ", "PublicKey = ", "Endpoint = ", "AllowedIPs = ", "PersistentKeepalive = ",
	}
	requireKnownLines(t, startClient(t, plainVariantFixture(), peerPayloadFixture(t)), directives)

	obfuscated := obfuscationFixture()
	obfuscated.I1 = "<b 0xf6ab><t>"
	plain := peerPayloadFixture(t)
	payload := wgcore.PeerResponse[extensions]{
		Addresses:  plain.Addresses,
		Extensions: extensions{Obfuscation: obfuscated},
		MTU:        plain.MTU,
		Port:       plain.Port,
		PublicKey:  plain.PublicKey,
	}
	variant := plainVariantFixture()
	variant.Templates = os.DirFS("../../amneziawg")
	obfuscatedDirectives := slices.Concat(directives, []string{
		"Jc = ", "Jmin = ", "Jmax = ", "S1 = ", "S2 = ", "S3 = ", "S4 = ", "H1 = ", "H2 = ", "H3 = ", "H4 = ", "I1 = ",
	})
	requireKnownLines(t, startClient(t, variant, payload), obfuscatedDirectives)
}

func TestClient_Start_ThenStop(t *testing.T) {
	t.Parallel()

	p := clientPlacementFixture(t)
	r := &fakeRunner{}

	c := wgcore.NewClientForTest[plainExtensions](plainVariantFixture(), p, zerolog.Nop(), r)
	require.Equal(t, p.Interface, c.Interface())
	require.Empty(t, c.ProxyEndpoint())
	require.Equal(t, service.Name(fakeName), c.Name())

	req, err := c.PeerRequest(t.Context())
	require.NoError(t, err)
	require.Equal(t, service.Name(fakeName), req.Name)
	require.Equal(t, fakeName, wireType(t, req))

	var peer wgcore.PeerRequest
	require.NoError(t, req.Unpack(&peer))
	require.NoError(t, peer.Validate())

	serverKey := mustPublicKey(t)
	payload := wgcore.PeerResponse[plainExtensions]{
		Addresses: []string{"10.0.0.2/32"},
		MTU:       1420,
		Port:      12345,
		PublicKey: serverKey,
	}

	resp, err := service.PackResponse(service.Name(fakeName), service.PeerID(peer.PublicKey), payload)
	require.NoError(t, err)
	require.NoError(t, c.Start(t.Context(), "203.0.113.1", resp))

	entries, err := os.ReadDir(p.Dir)
	require.NoError(t, err)
	require.Empty(t, entries)

	conf := filepath.Join(p.ToolDir, "dvpn-c0.conf")
	wantUp := []string{conf}
	require.Equal(t, wantUp, r.up)

	raw, err := os.ReadFile(conf) //#nosec G304
	require.NoError(t, err)
	requireClientConf(t, string(raw), wgcore.ClientPrivateKey(c), serverKey, "203.0.113.1:12345")
	require.NotContains(t, string(raw), "Jc =")

	r.transfer = map[string]service.PeerStats{serverKey: {RxBytes: 3, TxBytes: 4}}

	stats, err := c.Stats(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 3, stats.RxBytes)
	require.EqualValues(t, 4, stats.TxBytes)

	require.NoError(t, c.Stop(t.Context()))

	wantDown := []string{conf, conf}
	require.Equal(t, wantDown, r.down)

	_, err = os.Stat(conf)
	require.True(t, os.IsNotExist(err))

	waitCtx, cancel := context.WithTimeout(t.Context(), 0)
	defer cancel()

	require.NoError(t, c.Wait(waitCtx))
}

func TestClient_Start_UpErrorCleansConfig(t *testing.T) {
	t.Parallel()

	p := clientPlacementFixture(t)
	want := errors.New("name file missing")
	r := &fakeRunner{upErr: want}
	c := wgcore.NewClientForTest[plainExtensions](plainVariantFixture(), p, zerolog.Nop(), r)
	_, err := c.PeerRequest(t.Context())
	require.NoError(t, err)

	err = c.Start(t.Context(), "203.0.113.1", peerResponseFixture(t, wgcore.ClientPublicKey(c)))
	require.ErrorIs(t, err, want)
	require.Equal(t, p.Interface, c.Interface())

	conf := filepath.Join(p.ToolDir, "dvpn-c0.conf")
	require.Equal(t, []string{conf, conf}, r.down)
	_, err = os.Stat(conf)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestInterfaceFromNameFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "dvpn-c0.name"), []byte("utun4\n"), 0o600))

	live, err := wgcore.InterfaceFromNameFile(dir, "dvpn-c0")
	require.NoError(t, err)
	require.Equal(t, "utun4", live)

	_, err = wgcore.InterfaceFromNameFile(dir, "missing")
	require.ErrorIs(t, err, os.ErrNotExist)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "empty.name"), []byte("\n"), 0o600))
	_, err = wgcore.InterfaceFromNameFile(dir, "empty")
	require.ErrorIs(t, err, wgcore.ErrNameFileEmpty)
}

func TestNewClient_InvalidInterface(t *testing.T) {
	t.Parallel()

	_, err := wgcore.NewClient[plainExtensions](
		plainVariantFixture(), service.Placement{Dir: t.TempDir(), Interface: "../x"}, zerolog.Nop(),
	)
	require.ErrorIs(t, err, validate.ErrInvalidConfig)
}

func clientPlacementFixture(t *testing.T) service.Placement {
	t.Helper()

	return service.Placement{Dir: t.TempDir(), Interface: "dvpn-c0", ToolDir: t.TempDir()}
}

func requireKnownLines(t *testing.T, raw string, directives []string) {
	t.Helper()

	for line := range strings.Lines(raw) {
		line = strings.TrimSuffix(line, "\n")
		known := line == "" || line == "# "+service.ManagedMarker || line == "[Interface]" || line == "[Peer]" ||
			slices.ContainsFunc(directives, func(prefix string) bool { return strings.HasPrefix(line, prefix) })
		require.True(t, known, "unexpected line %q", line)
	}
}

func startClient[E service.Validator](t *testing.T, v wgcore.Variant, payload wgcore.PeerResponse[E]) string {
	t.Helper()

	p := clientPlacementFixture(t)
	c := wgcore.NewClientForTest[E](v, p, zerolog.Nop(), &fakeRunner{})
	_, err := c.PeerRequest(t.Context())
	require.NoError(t, err)

	resp, err := service.PackResponse(v.Name, service.PeerID(wgcore.ClientPublicKey(c)), payload)
	require.NoError(t, err)
	require.NoError(t, c.Start(t.Context(), "203.0.113.1", resp))

	raw, err := os.ReadFile(filepath.Join(p.ToolDir, "dvpn-c0.conf")) //#nosec G304
	require.NoError(t, err)

	return string(raw)
}

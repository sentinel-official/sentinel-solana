package hysteria2_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/hysteria2"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

type failingListener struct {
	err error
}

func (l failingListener) Accept() (net.Conn, error) {
	return nil, l.err
}

func (failingListener) Addr() net.Addr {
	return &net.TCPAddr{}
}

func (failingListener) Close() error {
	return nil
}

type fakeProcess struct{ service.Process }

func (fakeProcess) Done() <-chan struct{} { return nil }

func (fakeProcess) PID() int { return os.Getpid() }

func (fakeProcess) Stop(context.Context) error { return nil }

func (fakeProcess) Wait(context.Context) error { return nil }

func TestAuthHandler(t *testing.T) {
	t.Parallel()

	peers := service.NewPeers[struct{}]()
	require.NoError(t, peers.Add("abc", struct{}{}))

	h := hysteria2.AuthHandler(peers)

	w := httptest.NewRecorder()
	h.ServeHTTP(
		w,
		httptest.NewRequestWithContext(
			t.Context(),
			http.MethodPost,
			"/",
			strings.NewReader(`{"addr":"203.0.113.1:1234","auth":"abc","tx":0}`),
		),
	)
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"ok":true,"id":"abc"}`, w.Body.String())

	w = httptest.NewRecorder()
	h.ServeHTTP(
		w,
		httptest.NewRequestWithContext(
			t.Context(),
			http.MethodPost,
			"/",
			strings.NewReader(`{"addr":"203.0.113.1:1234","auth":"nope","tx":0}`),
		),
	)
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"ok":false,"id":""}`, w.Body.String())
}

func TestFetchTraffic(t *testing.T) {
	t.Parallel()

	var gotAuth string

	handle := func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")

		entries := map[string]hysteria2.TrafficEntry{"abc": {Rx: 1, Tx: 2}}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(entries) //nolint:errchkjson
	}

	srv := httptest.NewServer(http.HandlerFunc(handle))
	t.Cleanup(srv.Close)

	out, err := hysteria2.FetchTraffic(t.Context(), srv.Listener.Addr().String(), "s3cret")
	require.NoError(t, err)
	require.Equal(t, "s3cret", gotAuth)
	require.EqualValues(t, 1, out["abc"].Rx)
	require.EqualValues(t, 2, out["abc"].Tx)
}

func TestKick(t *testing.T) {
	t.Parallel()

	var gotAuth, gotBody string

	handle := func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")

		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)

		w.WriteHeader(http.StatusOK)
	}

	srv := httptest.NewServer(http.HandlerFunc(handle))
	t.Cleanup(srv.Close)

	require.NoError(t, hysteria2.Kick(t.Context(), srv.Listener.Addr().String(), "s3cret", "abc"))
	require.Equal(t, "s3cret", gotAuth)
	require.JSONEq(t, `["abc"]`, gotBody)
}

func TestNewServer_Registered(t *testing.T) {
	t.Parallel()

	sdktest.RequireServerRegistered(t, hysteria2.Name, 1<<5, hysteria2.ServerConfig{})

	dir, err := service.ServerToolDir(hysteria2.Name)
	require.NoError(t, err)
	require.Equal(t, "/etc/hysteria/sentinel-dvpn", dir)
}

func TestServer_AddPeer_NotStarted(t *testing.T) {
	t.Parallel()

	s, err := hysteria2.NewServerForTest(
		hysteria2.DefaultServerConfig(), service.Placement{}, zerolog.Nop(), "127.0.0.1:0",
	)
	require.NoError(t, err)

	req, err := service.PackRequest(hysteria2.Name, hysteria2.PeerRequest{UUID: "00000000-0000-4000-8000-000000000001"})
	require.NoError(t, err)

	_, err = s.AddPeer(t.Context(), req)
	require.ErrorIs(t, err, service.ErrServiceNotStarted)
}

func TestServer_AddPeer_ResponseValidates(t *testing.T) {
	t.Parallel()

	statsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{}"))
	}))
	t.Cleanup(statsSrv.Close)

	start := func(context.Context, service.Command, zerolog.Logger) (service.Process, error) {
		return fakeProcess{}, nil
	}

	s, err := hysteria2.NewServerForTest(
		hysteria2.DefaultServerConfig(), serverPlacementFixture(t), zerolog.Nop(), statsSrv.Listener.Addr().String(),
		service.WithProcessStarter(start),
	)
	require.NoError(t, err)

	require.NoError(t, s.Start(t.Context()))
	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	req, err := service.PackRequest(hysteria2.Name, hysteria2.PeerRequest{UUID: "00000000-0000-4000-8000-000000000001"})
	require.NoError(t, err)

	resp, err := s.AddPeer(t.Context(), req)
	require.NoError(t, err)

	_, err = service.UnpackResponse[hysteria2.PeerResponse](resp)
	require.NoError(t, err)

	_, err = service.UnpackMetadata[hysteria2.Metadata](s.Metadata())
	require.NoError(t, err)
}

func TestServer_RemovePeer_KeepsPeerOnFailure(t *testing.T) {
	t.Parallel()

	handle := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/kick" {
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		_, _ = w.Write([]byte("{}"))
	}

	statsSrv := httptest.NewServer(http.HandlerFunc(handle))
	t.Cleanup(statsSrv.Close)

	start := func(context.Context, service.Command, zerolog.Logger) (service.Process, error) {
		return fakeProcess{}, nil
	}

	s, err := hysteria2.NewServerForTest(
		hysteria2.DefaultServerConfig(), serverPlacementFixture(t), zerolog.Nop(), statsSrv.Listener.Addr().String(),
		service.WithProcessStarter(start),
	)
	require.NoError(t, err)

	require.NoError(t, s.Start(t.Context()))
	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	id := "00000000-0000-4000-8000-000000000001"
	req, err := service.PackRequest(hysteria2.Name, hysteria2.PeerRequest{UUID: id})
	require.NoError(t, err)

	_, err = s.AddPeer(t.Context(), req)
	require.NoError(t, err)
	require.Error(t, s.RemovePeer(t.Context(), service.PeerID(id)))

	_, err = s.AddPeer(t.Context(), req)
	require.ErrorIs(t, err, service.ErrPeerExists)
}

func TestServer_ServeAuthServer(t *testing.T) {
	t.Parallel()

	cases := []struct {
		closed bool
		name   string
	}{
		{closed: false, name: "unexpected failure"},
		{closed: true, name: "normal shutdown"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var logs bytes.Buffer

			s, err := hysteria2.NewServerForTest(
				hysteria2.DefaultServerConfig(),
				service.Placement{},
				zerolog.New(&logs),
				"127.0.0.1:1",
			)
			require.NoError(t, err)

			srv := &http.Server{ReadHeaderTimeout: time.Second}
			if tc.closed {
				require.NoError(t, srv.Close())
			}

			hysteria2.ServerServeAuthServer(s, srv, failingListener{err: errors.New("auth listener failed")})

			if tc.closed {
				require.Empty(t, logs.String())
			} else {
				require.Contains(t, logs.String(), "auth listener failed")
				require.Contains(t, logs.String(), "auth server failed")
				require.Contains(t, logs.String(), `"level":"error"`)
			}
		})
	}
}

func TestServer_Start_AuthPortInUse(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig

	listener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	cfg := hysteria2.DefaultServerConfig()
	cfg.AuthPort = uint16(listener.Addr().(*net.TCPAddr).Port)
	p := serverPlacementFixture(t)

	s, err := hysteria2.NewServer(cfg, p, zerolog.Nop())
	require.NoError(t, err)

	err = s.Start(t.Context())
	require.ErrorIs(t, err, syscall.EADDRINUSE)
	require.ErrorContains(t, err, "starting auth server")

	left, err := os.ReadDir(p.ToolDir)
	require.NoError(t, err)
	require.Empty(t, left)
}

func TestServer_Start_Lifecycle(t *testing.T) {
	if _, err := exec.LookPath(hysteria2.Binary); err != nil {
		t.Skip("hysteria2 binary not found")
	}

	t.Parallel()

	var kickedAuth, kickedBody string

	entries := map[string]hysteria2.TrafficEntry{}

	handle := func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/kick":
			kickedAuth = r.Header.Get("Authorization")

			raw, _ := io.ReadAll(r.Body)
			kickedBody = string(raw)

			w.WriteHeader(http.StatusOK)
		case "/traffic":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(entries) //nolint:errchkjson
		default:
			http.NotFound(w, r)
		}
	}

	statsSrv := httptest.NewServer(http.HandlerFunc(handle))
	t.Cleanup(statsSrv.Close)

	cfg := hysteria2.DefaultServerConfig()
	p := serverPlacementFixture(t)

	s, err := hysteria2.NewServerForTest(cfg, p, zerolog.Nop(), statsSrv.Listener.Addr().String())
	require.NoError(t, err)

	require.NoError(t, s.Start(t.Context()))
	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	raw, err := os.ReadFile(p.ToolPath(".yaml")) //#nosec G304
	require.NoError(t, err)

	doc := string(raw)
	require.Contains(t, doc, "auth:")
	require.Contains(t, doc, "type: http")
	require.Contains(t, doc, "trafficStats:")
	require.Contains(t, doc, "obfs:")

	require.NotEmpty(t, hysteria2.ServerTLSPin(s))

	id1 := "00000000-0000-4000-8000-000000000001"
	peerReq1 := hysteria2.PeerRequest{UUID: id1}
	req1, err := service.PackRequest(hysteria2.Name, peerReq1)
	require.NoError(t, err)

	resp1, err := s.AddPeer(t.Context(), req1)
	require.NoError(t, err)
	require.Equal(t, service.PeerID(id1), resp1.ID)

	var p1 hysteria2.PeerResponse
	require.NoError(t, resp1.Unpack(&p1))
	require.Equal(t, cfg.ObfsPassword, p1.ObfsPassword)
	require.Equal(t, cfg.ListenPort, p1.Port)
	require.Equal(t, hysteria2.ServerTLSPin(s), p1.TLSPin)

	id2 := "00000000-0000-4000-8000-000000000002"
	peerReq2 := hysteria2.PeerRequest{UUID: id2}
	req2, err := service.PackRequest(hysteria2.Name, peerReq2)
	require.NoError(t, err)

	_, err = s.AddPeer(t.Context(), req2)
	require.NoError(t, err)

	_, err = s.AddPeer(t.Context(), req1)
	require.ErrorIs(t, err, service.ErrPeerExists)

	entries[id1] = hysteria2.TrafficEntry{
		Rx: 5,
		Tx: 7,
	}

	stats, err := s.PeerStats(t.Context(), service.PeerID(id1))
	require.NoError(t, err)
	require.EqualValues(t, 7, stats.RxBytes)
	require.EqualValues(t, 5, stats.TxBytes)

	_, err = s.PeerStats(t.Context(), service.PeerID("unknown"))
	require.ErrorIs(t, err, service.ErrPeerNotFound)
	require.ErrorContains(t, err, "unknown")

	require.NoError(t, s.RemovePeer(t.Context(), service.PeerID(id1)))
	require.Equal(t, cfg.StatsSecret, kickedAuth)
	require.JSONEq(t, `["`+id1+`"]`, kickedBody)

	require.ErrorIs(t, s.RemovePeer(t.Context(), service.PeerID(id1)), service.ErrPeerNotFound)
}

func TestServer_Start_WritesToolCopies(t *testing.T) {
	t.Parallel()

	statsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{}"))
	}))
	t.Cleanup(statsSrv.Close)

	var args []string

	start := func(_ context.Context, cmd service.Command, _ zerolog.Logger) (service.Process, error) {
		args = cmd.Args

		return fakeProcess{}, nil
	}

	p := serverPlacementFixture(t)

	s, err := hysteria2.NewServerForTest(
		hysteria2.DefaultServerConfig(), p, zerolog.Nop(), statsSrv.Listener.Addr().String(),
		service.WithProcessStarter(start),
	)
	require.NoError(t, err)
	require.NoError(t, s.Start(t.Context()))
	require.Equal(t, []string{"server", "-c", p.ToolPath(".yaml")}, args)

	conf, err := os.ReadFile(p.ToolPath(".yaml")) //#nosec G304
	require.NoError(t, err)
	require.Contains(t, string(conf), "cert: "+p.ToolPath(".tls.crt"))
	require.Contains(t, string(conf), "key: "+p.ToolPath(".tls.key"))
	require.True(t, strings.HasPrefix(string(conf), "# "+service.ManagedMarker+"\n"))

	for _, name := range []string{"tls.crt", "tls.key"} {
		orig, err := os.ReadFile(filepath.Join(p.Dir, name)) //#nosec G304
		require.NoError(t, err)

		copied, err := os.ReadFile(p.ToolPath("." + name)) //#nosec G304
		require.NoError(t, err)
		require.Equal(t, "# "+service.ManagedMarker+"\n"+string(orig), string(copied))
	}

	require.NoError(t, s.Stop(t.Context()))

	left, err := os.ReadDir(p.ToolDir)
	require.NoError(t, err)
	require.Empty(t, left)
}

func serverPlacementFixture(t *testing.T) service.Placement {
	t.Helper()

	return service.Placement{Dir: t.TempDir(), Interface: "dvpn-n0", ToolDir: t.TempDir()}
}

func TestServer_Start_OpensFirewall_Stop_Closes(t *testing.T) {
	t.Parallel()

	statsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{}"))
	}))
	t.Cleanup(statsSrv.Close)

	start := func(context.Context, service.Command, zerolog.Logger) (service.Process, error) {
		return fakeProcess{}, nil
	}

	s, err := hysteria2.NewServerForTest(
		hysteria2.DefaultServerConfig(), serverPlacementFixture(t), zerolog.Nop(), statsSrv.Listener.Addr().String(),
		service.WithProcessStarter(start),
	)
	require.NoError(t, err)

	var calls []string
	hysteria2.SetServerRun(s, func(_ context.Context, name string, args ...string) ([]byte, error) {
		calls = append(calls, name+" "+strings.Join(args, " "))
		if name == "firewall-cmd" && len(args) == 1 && args[0] == "--state" {
			return []byte("running\n"), nil // report firewalld active so Start picks the firewalld backend
		}

		return nil, nil
	})

	require.NoError(t, s.Start(t.Context()))
	require.True(t, fwHasCallPrefix(calls, "firewall-cmd --add-port="), "Start must open the inbound udp port, calls=%v", calls)

	require.NoError(t, s.Stop(context.Background()))
	require.True(t, fwHasCallPrefix(calls, "firewall-cmd --remove-port="), "Stop must close the inbound udp port, calls=%v", calls)
}

func fwHasCallPrefix(calls []string, prefix string) bool {
	for _, c := range calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}

	return false
}

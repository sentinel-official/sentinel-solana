package openvpn_test

import (
	"bufio"
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/vishvananda/netlink"

	"github.com/sentinel-official/sentinel-go-sdk/v3/api"
	"github.com/sentinel-official/sentinel-go-sdk/v3/openvpn"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
)

type fakeProcess struct{ service.Process }

func (fakeProcess) Done() <-chan struct{} { return nil }

func (fakeProcess) PID() int { return os.Getpid() }

func (fakeProcess) Stop(context.Context) error { return nil }

func TestNewServer_InvalidInterface(t *testing.T) {
	t.Parallel()

	_, err := openvpn.NewServer(
		openvpn.DefaultServerConfig(), service.Placement{Dir: t.TempDir(), Table: "sentinel-dvpn-n0"}, zerolog.Nop(),
	)
	require.ErrorIs(t, err, validate.ErrInvalidConfig)
}

func TestNewServer_Registered(t *testing.T) {
	t.Parallel()

	sdktest.RequireServerRegistered(t, openvpn.Name, 1<<2, openvpn.ServerConfig{})

	dir, err := service.ServerToolDir(openvpn.Name)
	require.NoError(t, err)
	require.Equal(t, "/etc/openvpn/server/sentinel-dvpn", dir)
}

func TestNewServer_RejectsInsecurePeerSecret(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		seed func(t *testing.T, dir string)
	}{
		{
			name: "permissive file",
			seed: func(t *testing.T, dir string) {
				t.Helper()

				path := filepath.Join(dir, "peer-secret.key")
				require.NoError(t, os.WriteFile(path, make([]byte, 32), 0o644)) //#nosec G306
				require.NoError(t, os.Chmod(path, 0o644))                       //#nosec G302
			},
		},
		{
			name: "symlink",
			seed: func(t *testing.T, dir string) {
				t.Helper()

				target := filepath.Join(dir, "target")
				require.NoError(t, os.WriteFile(target, make([]byte, 32), 0o600))
				require.NoError(t, os.Symlink(target, filepath.Join(dir, "peer-secret.key")))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p := serverPlacementFixture(t)
			tc.seed(t, p.Dir)
			_, err := openvpn.NewServerForTest(
				openvpn.DefaultServerConfig(), p, zerolog.Nop(),
				openvpn.NewManagement(map[string]string{}), nil,
			)
			require.Error(t, err)
		})
	}
}

func TestNewServer_RejectsMalformedPeerSecret(t *testing.T) {
	t.Parallel()

	p := serverPlacementFixture(t)
	path := filepath.Join(p.Dir, "peer-secret.key")
	require.NoError(t, os.WriteFile(path, []byte("invalid"), 0o600))

	_, err := openvpn.NewServerForTest(
		openvpn.DefaultServerConfig(), p, zerolog.Nop(), openvpn.NewManagement(map[string]string{}), nil,
	)
	require.ErrorIs(t, err, validate.ErrInvalidConfig)
	require.ErrorContains(t, err, "peer secret length")
	content, err := os.ReadFile(path) //#nosec G304
	require.NoError(t, err)
	require.Equal(t, "invalid", string(content))
}

func TestServer_AddPeer(t *testing.T) {
	t.Parallel()

	s := serverFixture(t)

	id1 := "00000000-0000-4000-8000-000000000001"
	peerReq1 := openvpn.PeerRequest{UUID: id1}
	req1, err := service.PackRequest(openvpn.Name, peerReq1)
	require.NoError(t, err)

	resp1, err := s.AddPeer(t.Context(), req1)
	require.NoError(t, err)

	var p1 openvpn.PeerResponse
	require.NoError(t, resp1.Unpack(&p1))
	require.Regexp(t, "^[0-9a-f]{64}$", p1.Password)
	require.True(t, strings.HasPrefix(p1.CA, "-----BEGIN CERTIFICATE-----"))

	id2 := "00000000-0000-4000-8000-000000000002"
	peerReq2 := openvpn.PeerRequest{UUID: id2}
	req2, err := service.PackRequest(openvpn.Name, peerReq2)
	require.NoError(t, err)

	resp2, err := s.AddPeer(t.Context(), req2)
	require.NoError(t, err)

	var p2 openvpn.PeerResponse
	require.NoError(t, resp2.Unpack(&p2))
	require.NotEqual(t, p1.Password, p2.Password)

	_, err = s.AddPeer(t.Context(), req1)
	require.ErrorIs(t, err, service.ErrPeerExists)
}

func TestServer_AddPeer_DerivesStablePassword(t *testing.T) {
	t.Parallel()

	p := serverPlacementFixture(t)
	mgmt := openvpn.NewManagement(map[string]string{})
	s := serverFixtureWithPlacement(t, mgmt, p)
	firstID := "00000000-0000-4000-8000-000000000001"
	add := func(s *openvpn.Server, id string) string {
		t.Helper()

		req, err := service.PackRequest(openvpn.Name, openvpn.PeerRequest{UUID: id})
		require.NoError(t, err)
		packed, err := s.AddPeer(t.Context(), req)
		require.NoError(t, err)

		var peer openvpn.PeerResponse
		require.NoError(t, packed.Unpack(&peer))

		return peer.Password
	}

	first := add(s, firstID)
	server, client := net.Pipe()

	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	openvpn.ManagementAttach(mgmt, client)

	served := make(chan error, 1)
	go func() { served <- openvpn.ManagementServe(mgmt, bufio.NewReader(client), client) }()

	require.NoError(t, server.SetDeadline(time.Now().Add(2*time.Second)))

	removed := make(chan error, 1)
	go func() { removed <- s.RemovePeer(t.Context(), service.PeerID(firstID)) }()

	line, err := bufio.NewReader(server).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "status 3\n", line)

	_, err = server.Write([]byte("HEADER\tCLIENT_LIST\tUsername\tBytes Received\tBytes Sent\nEND\n"))
	require.NoError(t, err)
	require.NoError(t, <-removed)
	require.NoError(t, server.Close())
	require.NoError(t, <-served)

	require.Equal(t, first, add(s, firstID))
	require.NotEqual(t, first, add(s, "00000000-0000-4000-8000-000000000002"))
	require.Equal(t, first, add(serverFixtureWithPlacement(t, openvpn.NewManagement(map[string]string{}), p), firstID))
	other := serverFixtureWithPlacement(t, openvpn.NewManagement(map[string]string{}), serverPlacementFixture(t))
	require.NotEqual(t, first, add(other, firstID))

	info, err := os.Stat(filepath.Join(p.Dir, "peer-secret.key"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestServer_AddPeer_NotStarted(t *testing.T) {
	t.Parallel()

	s, err := openvpn.NewServerForTest(
		openvpn.DefaultServerConfig(), serverPlacementFixture(t), zerolog.Nop(),
		openvpn.NewManagement(map[string]string{}), nil,
	)
	require.NoError(t, err)

	req, err := service.PackRequest(openvpn.Name, openvpn.PeerRequest{UUID: "00000000-0000-4000-8000-000000000001"})
	require.NoError(t, err)

	_, err = s.AddPeer(t.Context(), req)
	require.ErrorIs(t, err, service.ErrServiceNotStarted)
}

func TestServer_AddPeer_ResponseValidates(t *testing.T) {
	t.Parallel()

	s := serverFixture(t)

	req, err := service.PackRequest(openvpn.Name, openvpn.PeerRequest{UUID: "00000000-0000-4000-8000-000000000001"})
	require.NoError(t, err)

	resp, err := s.AddPeer(t.Context(), req)
	require.NoError(t, err)

	_, err = service.UnpackResponse[openvpn.PeerResponse](resp)
	require.NoError(t, err)

	_, err = service.UnpackMetadata[openvpn.Metadata](s.Metadata())
	require.NoError(t, err)
}

func TestServer_RemovePeer_VerifiesDisconnect(t *testing.T) {
	t.Parallel()

	const peerID = "00000000-0000-4000-8000-000000000001"

	cases := []struct {
		killReply string
		name      string
		status    []bool
		timeout   time.Duration
		want      []string
		wantErr   string
	}{
		{
			name:   "disconnects after HALT",
			status: []bool{true, false},
			want:   []string{"client-kill 7 HALT", "status 3", "status 3"},
		},
		{
			name:    "stays connected after fallback",
			status:  []bool{true, true},
			timeout: 50 * time.Millisecond,
			want:    []string{"client-kill 7 HALT", "status 3", "kill " + peerID, "status 3"},
			wantErr: "peer not removed",
		},
		{
			// The client was crash-killed, so the kill is rejected, but status shows it gone.
			killReply: "ERROR: no such client\n",
			name:      "HALT rejected but the client is gone",
			status:    []bool{false},
			want:      []string{"client-kill 7 HALT", "status 3"},
		},
		{
			killReply: "ERROR: no such client\n",
			name:      "HALT rejected and still connected",
			status:    []bool{true, true},
			timeout:   50 * time.Millisecond,
			want:      []string{"client-kill 7 HALT", "status 3", "kill " + peerID, "status 3"},
			wantErr:   "management command failed: ERROR: no such client",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mgmt := openvpn.NewManagement(map[string]string{})
			s := serverFixtureWithManagement(t, mgmt)
			req, err := service.PackRequest(openvpn.Name, openvpn.PeerRequest{UUID: peerID})
			require.NoError(t, err)
			packed, err := s.AddPeer(t.Context(), req)
			require.NoError(t, err)

			var peer openvpn.PeerResponse
			require.NoError(t, packed.Unpack(&peer))

			server, client := net.Pipe()

			t.Cleanup(func() { _ = server.Close(); _ = client.Close() })

			served := make(chan error, 1)
			go func() { served <- openvpn.ManagementServe(mgmt, bufio.NewReader(client), client) }()

			require.NoError(t, server.SetDeadline(time.Now().Add(2*time.Second)))
			r := bufio.NewReader(server)
			_, err = fmt.Fprintf(server,
				">CLIENT:CONNECT,7,2\n>CLIENT:ENV,username=%s\n>CLIENT:ENV,password=%s\n>CLIENT:ENV,END\n",
				peerID, peer.Password,
			)
			require.NoError(t, err)
			line, err := r.ReadString('\n')
			require.NoError(t, err)
			require.Equal(t, "client-auth-nt 7 2\n", line)

			_, err = server.Write([]byte("SUCCESS: authenticated\n"))
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				return openvpn.ManagementHasClient(mgmt, peerID, "7")
			}, time.Second, time.Millisecond)

			if tc.timeout > 0 {
				openvpn.SetServerRemovePeerTimeout(s, tc.timeout)
			}

			removed := make(chan error, 1)
			go func() { removed <- s.RemovePeer(t.Context(), service.PeerID(peer.Username)) }()

			for _, cmd := range tc.want {
				line, err = r.ReadString('\n')
				require.NoError(t, err)
				require.Equal(t, cmd+"\n", line)

				reply := "SUCCESS: client killed\n"

				switch cmd {
				case "client-kill 7 HALT":
					if tc.killReply != "" {
						reply = tc.killReply
					}
				case "status 3":
					reply = "HEADER\tCLIENT_LIST\tUsername\tBytes Received\tBytes Sent\n"
					if tc.status[0] {
						reply += "CLIENT_LIST\t" + peerID + "\t0\t0\n"
					}

					reply += "END\n"
					tc.status = tc.status[1:]
				}

				_, err = server.Write([]byte(reply))
				require.NoError(t, err)
			}

			err = <-removed
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
			}

			if tc.wantErr == "" {
				// Removed means forgotten: the next close is "not found", which the node treats as done.
				require.ErrorIs(t, s.RemovePeer(t.Context(), service.PeerID(peer.Username)), service.ErrPeerNotFound)
			}

			_ = server.Close()

			require.NoError(t, <-served)
		})
	}
}

func TestServer_Start_MissingLinkContinues(t *testing.T) {
	t.Parallel()

	cfg := openvpn.DefaultServerConfig()
	p := serverPlacementFixture(t)
	require.NoError(t, os.Mkdir(filepath.Join(p.Dir, "tls.crt"), 0o700))

	var calls []string

	kill := func(context.Context, string, string, string, string) error {
		calls = append(calls, "kill")

		return nil
	}
	s, err := openvpn.NewServerForTest(
		cfg, p, zerolog.Nop(), openvpn.NewManagement(map[string]string{}), nil,
		service.WithStaleKiller(kill),
	)
	require.NoError(t, err)

	openvpn.SetServerLinkHook(s, func(iface string) (netlink.Link, error) {
		calls = append(calls, "lookup")

		return nil, netlink.LinkNotFoundError{}
	})

	err = s.Start(t.Context())
	require.ErrorContains(t, err, "loading server certificate")
	require.Equal(t, []string{"kill", "lookup"}, calls)
}

func TestServer_Start_RejectsNotOwnedExistingLink(t *testing.T) {
	t.Parallel()

	cfg := openvpn.DefaultServerConfig()
	p := serverPlacementFixture(t)

	var calls []string

	kill := func(_ context.Context, path, bin, flag, marker string) error {
		require.Equal(t, filepath.Join(p.Dir, "openvpn.pid"), path)
		require.Equal(t, "--config", flag)
		require.Equal(t, filepath.Join(p.ToolDir, "dvpn-n0.conf"), marker)

		calls = append(calls, "kill")

		return nil
	}
	s, err := openvpn.NewServerForTest(
		cfg, p, zerolog.Nop(), openvpn.NewManagement(map[string]string{}), nil,
		service.WithStaleKiller(kill),
	)
	require.NoError(t, err)

	openvpn.SetServerLinkHook(s, func(iface string) (netlink.Link, error) {
		require.Equal(t, "dvpn-n0", iface)

		calls = append(calls, "lookup")

		return &netlink.Dummy{Name: iface}, nil
	})

	err = s.Start(t.Context())
	require.ErrorContains(t, err, "already exists")
	require.Equal(t, []string{"kill", "lookup"}, calls)
	require.NoFileExists(t, filepath.Join(p.Dir, "tls.crt"))
}

func TestServer_Start_StopsOnPIDCleanupError(t *testing.T) {
	t.Parallel()

	cfg := openvpn.DefaultServerConfig()
	cleanupErr := errors.New("owned process did not exit")

	kill := func(context.Context, string, string, string, string) error { return cleanupErr }
	s, err := openvpn.NewServerForTest(
		cfg, serverPlacementFixture(t), zerolog.Nop(), openvpn.NewManagement(map[string]string{}), nil,
		service.WithStaleKiller(kill),
	)
	require.NoError(t, err)

	openvpn.SetServerLinkHook(s, func(string) (netlink.Link, error) {
		require.FailNow(t, "link lookup ran after failed PID cleanup")

		return nil, nil
	})

	require.ErrorIs(t, s.Start(t.Context()), cleanupErr)
}

func TestServer_Start_WritesToolCopies(t *testing.T) {
	t.Parallel()

	p := serverPlacementFixture(t)
	names := []string{"dvpn-n0.conf", "dvpn-n0.tls.crt", "dvpn-n0.tls.key", "dvpn-n0.tls-crypt.key"}
	launchErr := errors.New("launch refused")

	var (
		recorded service.Command
		atLaunch = map[string]string{}
	)

	start := func(_ context.Context, cmd service.Command, _ zerolog.Logger) (service.Process, error) {
		recorded = cmd

		for _, name := range names {
			raw, err := os.ReadFile(filepath.Join(p.ToolDir, name)) //#nosec G304
			require.NoError(t, err)

			atLaunch[name] = string(raw)
		}

		return nil, launchErr
	}
	kill := func(context.Context, string, string, string, string) error { return nil }
	s, err := openvpn.NewServerForTest(
		openvpn.DefaultServerConfig(), p, zerolog.Nop(), openvpn.NewManagement(map[string]string{}), nil,
		service.WithProcessStarter(start), service.WithStaleKiller(kill),
	)
	require.NoError(t, err)

	openvpn.SetServerLinkHook(s, func(string) (netlink.Link, error) { return nil, netlink.LinkNotFoundError{} })

	require.ErrorIs(t, s.Start(t.Context()), launchErr)
	require.Equal(t, []string{"--config", filepath.Join(p.ToolDir, "dvpn-n0.conf")}, recorded.Args)

	for _, name := range names {
		require.True(t, strings.HasPrefix(atLaunch[name], "# "+service.ManagedMarker+"\n"), name)
	}

	require.Contains(t, atLaunch["dvpn-n0.conf"], "cert "+filepath.Join(p.ToolDir, "dvpn-n0.tls.crt")+"\n")
	require.NotContains(t, atLaunch["dvpn-n0.conf"], p.Dir)

	left, err := os.ReadDir(p.ToolDir)
	require.NoError(t, err)
	require.Empty(t, left)

	require.FileExists(t, filepath.Join(p.Dir, "tls.key"))
}

func TestServer_Stop_RemovesToolCopies(t *testing.T) {
	t.Parallel()

	p := serverPlacementFixture(t)
	s := serverFixtureWithPlacement(t, openvpn.NewManagement(map[string]string{}), p)
	conf := p.ToolPath(".conf")

	require.NoError(t, service.WriteOwned(conf, nil, "#"))
	require.NoError(t, s.Stop(t.Context()))
	require.NoFileExists(t, conf)
}

func TestServer_WaitPeerGone_RejectsMalformedStatus(t *testing.T) {
	t.Parallel()

	mgmt := openvpn.NewManagement(map[string]string{})
	s, err := openvpn.NewServerForTest(
		openvpn.DefaultServerConfig(), serverPlacementFixture(t), zerolog.Nop(), mgmt, nil,
	)
	require.NoError(t, err)

	server, client := net.Pipe()

	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	require.NoError(t, server.SetDeadline(time.Now().Add(2*time.Second)))
	openvpn.ManagementAttach(mgmt, client)

	result := make(chan error, 1)
	go func() { result <- openvpn.ServerWaitPeerGone(s, t.Context(), "user-1") }()

	line, err := bufio.NewReader(server).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "status 3\n", line)
	openvpn.ManagementDeliver(mgmt, "HEADER\tCLIENT_LIST\tCommon Name\nCLIENT_LIST\tunknown\nEND")
	require.ErrorContains(t, <-result, "invalid management status")
}

func TestTemplates_Server(t *testing.T) {
	t.Parallel()

	cfg := openvpn.DefaultServerConfig()
	cfg.Addresses = []string{"10.0.0.1/24"}
	dir := t.TempDir()

	network, netmask := openvpn.NetworkAndNetmask(netip.MustParsePrefix(cfg.Addresses[0]))

	data := openvpn.ServerTemplateData{
		CertFile:       filepath.Join(dir, "dvpn-n0.tls.crt"),
		Interface:      "dvpn-n0",
		KeyFile:        filepath.Join(dir, "dvpn-n0.tls.key"),
		ManagementPort: cfg.ManagementPort,
		Netmask:        netmask,
		Network:        network,
		Port:           cfg.ListenPort,
		Protocol:       cfg.Protocol,
		TLSCryptFile:   filepath.Join(dir, "dvpn-n0.tls-crypt.key"),
		TmpDir:         dir,
	}

	conf := filepath.Join(dir, "dvpn-n0.conf")
	require.NoError(t, service.WriteTemplate(openvpn.Templates(), "templates/server.conf.tmpl", conf, data, "#"))

	raw, err := os.ReadFile(conf) //#nosec G304
	require.NoError(t, err)

	content := string(raw)
	require.Contains(t, content, "management-client-auth\n")
	require.Contains(t, content, "tmp-dir "+data.TmpDir+"\n")
	require.Contains(t, content, "verify-client-cert none\n")
	require.Contains(t, content, fmt.Sprintf("management 127.0.0.1 %d\n", cfg.ManagementPort))
	require.Contains(t, content, "server 10.0.0.0 255.255.255.0\n")
	require.Contains(t, content, fmt.Sprintf("ca %s\n", data.CertFile))
	require.NotContains(t, content, "status")
}

func fakeStart(context.Context, service.Command, zerolog.Logger) (service.Process, error) {
	return fakeProcess{}, nil
}

func serverFixture(t *testing.T) *openvpn.Server {
	t.Helper()

	return serverFixtureWithManagement(t, openvpn.NewManagement(map[string]string{}))
}

func serverFixtureWithManagement(t *testing.T, mgmt *openvpn.Management) *openvpn.Server {
	t.Helper()

	return serverFixtureWithPlacement(t, mgmt, serverPlacementFixture(t))
}

func serverFixtureWithPlacement(t *testing.T, mgmt *openvpn.Management, p service.Placement) *openvpn.Server {
	t.Helper()

	cfg := openvpn.DefaultServerConfig()

	cert, _, err := api.LoadOrCreateServerCert(p.Dir)
	require.NoError(t, err)

	block := &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert.Certificate[0],
	}
	certPEM := pem.EncodeToMemory(block)

	setup := func(context.Context, string, string, string, []netip.Prefix) (service.Egress, error) {
		return &sdktest.FakeEgress{}, nil
	}

	s, err := openvpn.NewServerForTest(
		cfg, p, zerolog.Nop(), mgmt, setup, service.WithProcessStarter(fakeStart),
	)
	require.NoError(t, err)
	require.NoError(t, openvpn.ServerLaunch(s, t.Context(), service.Command{}, func(context.Context) error { return nil }))

	tlsCrypt, err := os.ReadFile("testdata/tls-crypt.key")
	require.NoError(t, err)

	m := openvpn.Metadata{
		CA:       string(certPEM),
		Port:     cfg.ListenPort,
		Protocol: cfg.Protocol,
		TLSCrypt: string(tlsCrypt),
	}

	md, err := service.PackMetadata(openvpn.Name, m)
	require.NoError(t, err)

	openvpn.SetServerMetadata(s, md)

	return s
}

func serverPlacementFixture(t *testing.T) service.Placement {
	t.Helper()

	return service.Placement{
		Dir:       t.TempDir(),
		Interface: "dvpn-n0",
		Table:     "sentinel-dvpn-n0",
		ToolDir:   t.TempDir(),
	}
}

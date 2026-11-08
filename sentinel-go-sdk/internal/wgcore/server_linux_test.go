package wgcore_test

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/internal/wgcore"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

type failingJSON func()

func (failingJSON) Validate() error {
	return nil
}

func TestServer_AddPeer_NotStarted(t *testing.T) {
	t.Parallel()

	setup := func(context.Context, string, string, string, []netip.Prefix) (service.Egress, error) {
		return &sdktest.FakeEgress{}, nil
	}

	cfg := serverConfigFixture(t, plainExtensions{})

	s, err := wgcore.NewServerForTest(
		plainVariantFixture(), cfg, serverPlacementFixture(t), zerolog.Nop(), &fakeRunner{}, setup,
	)
	require.NoError(t, err)

	req, err := service.PackRequest(service.Name(fakeName), wgcore.PeerRequest{PublicKey: mustPublicKey(t)})
	require.NoError(t, err)

	_, err = s.AddPeer(t.Context(), req)
	require.ErrorIs(t, err, service.ErrServiceNotStarted)
}

func TestServer_AddPeer_ResponseValidates(t *testing.T) {
	t.Parallel()

	cfg := serverConfigFixture(t, extensions{Obfuscation: obfuscationFixture()})
	setup := func(context.Context, string, string, string, []netip.Prefix) (service.Egress, error) {
		return &sdktest.FakeEgress{}, nil
	}

	s, err := wgcore.NewServerForTest(
		plainVariantFixture(), cfg, serverPlacementFixture(t), zerolog.Nop(), &fakeRunner{}, setup,
	)
	require.NoError(t, err)
	require.NoError(t, s.Start(t.Context()))

	req, err := service.PackRequest(service.Name(fakeName), wgcore.PeerRequest{PublicKey: mustPublicKey(t)})
	require.NoError(t, err)

	resp, err := s.AddPeer(t.Context(), req)
	require.NoError(t, err)

	_, err = service.UnpackResponse[wgcore.PeerResponse[extensions]](resp)
	require.NoError(t, err)

	_, err = service.UnpackMetadata[wgcore.Metadata[extensions]](s.Metadata())
	require.NoError(t, err)
}

func TestServer_AddPeer_Rollback(t *testing.T) {
	t.Parallel()

	cfg := serverConfigFixture(t, plainExtensions{})
	r := &fakeRunner{setErr: errors.New("device busy")}
	setup := func(context.Context, string, string, string, []netip.Prefix) (service.Egress, error) {
		return &sdktest.FakeEgress{}, nil
	}

	s, err := wgcore.NewServerForTest(plainVariantFixture(), cfg, serverPlacementFixture(t), zerolog.Nop(), r, setup)
	require.NoError(t, err)
	require.NoError(t, s.Start(t.Context()))

	peerKey := mustPublicKey(t)
	payload := wgcore.PeerRequest{PublicKey: peerKey}
	req, err := service.PackRequest(service.Name(fakeName), payload)
	require.NoError(t, err)

	_, err = s.AddPeer(t.Context(), req)
	require.ErrorContains(t, err, "adding peer "+peerKey)
	require.ErrorContains(t, err, "device busy")

	_, err = s.PeerStats(t.Context(), service.PeerID(peerKey))
	require.ErrorIs(t, err, service.ErrPeerNotFound)

	r.setErr = nil

	resp, err := s.AddPeer(t.Context(), req)
	require.NoError(t, err)

	var p wgcore.PeerResponse[plainExtensions]
	require.NoError(t, resp.Unpack(&p))

	wantAddresses := []string{"10.0.0.2/32"}
	require.Equal(t, wantAddresses, p.Addresses)
}

func TestServer_AddPeer_UnencodableResponse(t *testing.T) {
	t.Parallel()

	cfg := serverConfigFixture(t, failingJSON(func() {}))
	r := &fakeRunner{}
	setup := func(context.Context, string, string, string, []netip.Prefix) (service.Egress, error) {
		return &sdktest.FakeEgress{}, nil
	}

	s, err := wgcore.NewServerForTest(plainVariantFixture(), cfg, serverPlacementFixture(t), zerolog.Nop(), r, setup)
	require.NoError(t, err)
	wgcore.SetServerStarted(s)

	req, err := service.PackRequest(service.Name(fakeName), wgcore.PeerRequest{PublicKey: mustPublicKey(t)})
	require.NoError(t, err)

	_, err = s.AddPeer(t.Context(), req)
	require.ErrorContains(t, err, "encoding peer response:")
	require.NotContains(t, err.Error(), "packing")
}

func TestServer_RestorePeer_KeepsAddress(t *testing.T) {
	t.Parallel()

	cfg := serverConfigFixture(t, plainExtensions{})
	cfg.Addresses = []netip.Prefix{netip.MustParsePrefix("10.47.90.1/24")}
	r := &fakeRunner{}
	setup := func(context.Context, string, string, string, []netip.Prefix) (service.Egress, error) {
		return &sdktest.FakeEgress{}, nil
	}
	s, err := wgcore.NewServerForTest(plainVariantFixture(), cfg, serverPlacementFixture(t), zerolog.Nop(), r, setup)
	require.NoError(t, err)
	require.NoError(t, s.Start(t.Context()))

	key := mustPublicKey(t)
	req, err := service.PackRequest(service.Name(fakeName), wgcore.PeerRequest{PublicKey: key})
	require.NoError(t, err)
	resp, err := service.PackResponse(
		service.Name(fakeName),
		service.PeerID(key),
		wgcore.PeerResponse[plainExtensions]{Addresses: []string{"10.47.90.3/32"}},
	)
	require.NoError(t, err)
	require.NoError(t, s.RestorePeer(t.Context(), req, resp))
	conflictKey := mustPublicKey(t)
	conflictReq, err := service.PackRequest(service.Name(fakeName), wgcore.PeerRequest{PublicKey: conflictKey})
	require.NoError(t, err)

	conflictResp := resp
	conflictResp.ID = service.PeerID(conflictKey)
	require.ErrorIs(t, s.RestorePeer(t.Context(), conflictReq, conflictResp), service.ErrAddressInUse)
	_, err = s.PeerStats(t.Context(), service.PeerID(conflictKey))
	require.ErrorIs(t, err, service.ErrPeerNotFound)

	for _, want := range []string{"10.47.90.2/32", "10.47.90.4/32"} {
		key := mustPublicKey(t)
		req, err := service.PackRequest(service.Name(fakeName), wgcore.PeerRequest{PublicKey: key})
		require.NoError(t, err)
		got, err := s.AddPeer(t.Context(), req)
		require.NoError(t, err)

		var p wgcore.PeerResponse[plainExtensions]
		require.NoError(t, got.Unpack(&p))
		require.Equal(t, []string{want}, p.Addresses)
	}

	_, err = s.PeerStats(t.Context(), service.PeerID(key))
	require.NoError(t, err)
	require.Contains(t, r.set, "dvpn-n0 "+key+" 10.47.90.3/32")
}

func TestServer_RestorePeer_RollsBack(t *testing.T) {
	t.Parallel()

	cfg := serverConfigFixture(t, plainExtensions{})
	cfg.Addresses = append(cfg.Addresses, netip.MustParsePrefix("fd00::1/64"))
	key := mustPublicKey(t)
	req, err := service.PackRequest(service.Name(fakeName), wgcore.PeerRequest{PublicKey: key})
	require.NoError(t, err)
	valid, err := service.PackResponse(
		service.Name(fakeName),
		service.PeerID(key),
		wgcore.PeerResponse[plainExtensions]{Addresses: []string{"10.0.0.3/32", "fd00::3/128"}},
	)
	require.NoError(t, err)

	cases := []struct {
		addresses []string
		name      string
		setErr    error
	}{
		{
			addresses: []string{"10.0.0.3/32", "invalid"},
			name:      "invalid",
		},
		{
			addresses: []string{"10.0.0.3/32", "10.0.0.4/32"},
			name:      "duplicate family",
		},
		{
			addresses: []string{"10.0.0.3/32", "fd00::3/128"},
			name:      "runner failure",
			setErr:    errors.New("device busy"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := &fakeRunner{setErr: tc.setErr}
			s, err := wgcore.NewServerForTest(plainVariantFixture(), cfg, serverPlacementFixture(t), zerolog.Nop(), r, nil)
			require.NoError(t, err)
			resp, err := service.PackResponse(
				service.Name(fakeName),
				service.PeerID(key),
				wgcore.PeerResponse[plainExtensions]{Addresses: tc.addresses},
			)
			require.NoError(t, err)
			require.Error(t, s.RestorePeer(t.Context(), req, resp))

			r.setErr = nil

			require.NoError(t, s.RestorePeer(t.Context(), req, valid))
		})
	}
}

func TestServer_Start_ConfigNotOwned(t *testing.T) {
	t.Parallel()

	cfg := serverConfigFixture(t, plainExtensions{})
	placement := serverPlacementFixture(t)
	r := &fakeRunner{}
	setup := func(context.Context, string, string, string, []netip.Prefix) (service.Egress, error) {
		return &sdktest.FakeEgress{}, nil
	}

	conf := filepath.Join(placement.ToolDir, "dvpn-n0.conf")
	foreign := "[Interface]\nAddress = 10.1.0.1/24\n"
	require.NoError(t, os.WriteFile(conf, []byte(foreign), 0o600))

	s, err := wgcore.NewServerForTest(plainVariantFixture(), cfg, placement, zerolog.Nop(), r, setup)
	require.NoError(t, err)

	err = s.Start(t.Context())
	require.ErrorIs(t, err, service.ErrConfigNotOwned)
	require.ErrorContains(t, err, conf)
	require.Empty(t, r.up)
	require.Empty(t, r.down)

	raw, err := os.ReadFile(conf) //#nosec G304
	require.NoError(t, err)
	require.Equal(t, foreign, string(raw))
}

func TestServer_Start_ThenStop(t *testing.T) {
	t.Parallel()

	cfg := serverConfigFixture(t, plainExtensions{})
	r := &fakeRunner{}
	egress := &sdktest.FakeEgress{}
	setup := func(_ context.Context, table, iface, outIface string, prefixes []netip.Prefix) (service.Egress, error) {
		if table != "sentinel-dvpn-n0" || iface != "dvpn-n0" || outIface != "eth0" || len(prefixes) != 1 {
			return nil, fmt.Errorf("unexpected egress setup: %s %s %s %v", table, iface, outIface, prefixes)
		}

		return egress, nil
	}

	placement := serverPlacementFixture(t)

	s, err := wgcore.NewServerForTest(plainVariantFixture(), cfg, placement, zerolog.Nop(), r, setup)
	require.NoError(t, err)
	require.Equal(t, service.Name(fakeName), s.Name())
	require.NoError(t, s.Start(t.Context()))

	entries, err := os.ReadDir(placement.Dir)
	require.NoError(t, err)
	require.Empty(t, entries)

	conf := filepath.Join(placement.ToolDir, "dvpn-n0.conf")
	wantUp := []string{conf}
	require.Equal(t, wantUp, r.up)

	wantDown := []string{conf}
	require.Equal(t, wantDown, r.down)

	raw, err := os.ReadFile(conf) //#nosec G304
	require.NoError(t, err)
	requireServerConf(t, string(raw), cfg.PrivateKey)
	require.NotContains(t, string(raw), "Jc =")

	serverKey, err := wgcore.DerivePublicKey(cfg.PrivateKey)
	require.NoError(t, err)
	require.Equal(t, fakeName, wireType(t, s.Metadata()))

	var md wgcore.Metadata[plainExtensions]
	require.NoError(t, s.Metadata().Unpack(&md))
	require.Equal(t, uint16(12345), md.Port)
	require.Equal(t, serverKey, md.PublicKey)

	peerPrivate, err := wgcore.NewPrivateKey()
	require.NoError(t, err)

	peerKey, err := wgcore.DerivePublicKey(peerPrivate)
	require.NoError(t, err)

	payload := wgcore.PeerRequest{PublicKey: peerKey}
	req, err := service.PackRequest(service.Name(fakeName), payload)
	require.NoError(t, err)

	resp, err := s.AddPeer(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, service.PeerID(peerKey), resp.ID)
	require.Equal(t, fakeName, wireType(t, resp))

	var p wgcore.PeerResponse[plainExtensions]
	require.NoError(t, resp.Unpack(&p))

	wantAddresses := []string{"10.0.0.2/32"}
	require.Equal(t, wantAddresses, p.Addresses)
	require.Equal(t, uint16(12345), p.Port)
	require.Equal(t, uint16(1420), p.MTU)
	require.Equal(t, serverKey, p.PublicKey)

	wantSet := []string{"dvpn-n0 " + peerKey + " 10.0.0.2/32"}
	require.Equal(t, wantSet, r.set)

	_, err = s.AddPeer(t.Context(), req)
	require.ErrorIs(t, err, service.ErrPeerExists)

	r.transfer = map[string]service.PeerStats{peerKey: {RxBytes: 10, TxBytes: 20}}

	stats, err := s.PeerStats(t.Context(), resp.ID)
	require.NoError(t, err)
	require.EqualValues(t, 10, stats.RxBytes)
	require.EqualValues(t, 20, stats.TxBytes)

	_, err = s.PeerStats(t.Context(), "unknown")
	require.ErrorIs(t, err, service.ErrPeerNotFound)
	require.ErrorContains(t, err, "unknown")

	require.NoError(t, s.RemovePeer(t.Context(), resp.ID))

	wantRemoved := []string{"dvpn-n0 " + peerKey}
	require.Equal(t, wantRemoved, r.removed)
	require.ErrorIs(t, s.RemovePeer(t.Context(), resp.ID), service.ErrPeerNotFound)

	again, err := s.AddPeer(t.Context(), req)
	require.NoError(t, err)
	require.NoError(t, again.Unpack(&p))

	wantAddresses2 := []string{"10.0.0.2/32"}
	require.Equal(t, wantAddresses2, p.Addresses)

	require.NoError(t, s.Stop(t.Context()))

	wantDown2 := []string{conf, conf}
	require.Equal(t, wantDown2, r.down)
	require.True(t, egress.TornDown)

	_, err = os.Stat(conf)
	require.True(t, os.IsNotExist(err))

	_, err = s.PeerStats(t.Context(), resp.ID)
	require.ErrorIs(t, err, service.ErrServiceNotStarted)

	waitCtx, cancel := context.WithTimeout(t.Context(), 0)
	t.Cleanup(cancel)

	require.NoError(t, s.Wait(waitCtx))

	t.Run("down failure keeps managed conf", func(t *testing.T) {
		t.Parallel()

		placement := serverPlacementFixture(t)
		r := &fakeRunner{}
		s, err := wgcore.NewServerForTest(plainVariantFixture(), cfg, placement, zerolog.Nop(), r, setup)
		require.NoError(t, err)
		require.NoError(t, s.Start(t.Context()))

		r.downErr = errors.New("device busy")

		err = s.Stop(t.Context())
		require.ErrorContains(t, err, "stopping dvpn-n0")
		require.ErrorContains(t, err, "device busy")

		require.FileExists(t, filepath.Join(placement.ToolDir, "dvpn-n0.conf"))
	})
}

func requireServerConf(t *testing.T, raw, privateKey string) {
	t.Helper()

	require.Contains(t, raw, "Address = 10.0.0.1/24\n")
	require.Contains(t, raw, "ListenPort = 12345\n")
	require.Contains(t, raw, "PrivateKey = "+privateKey+"\n")
	require.Contains(t, raw, "MTU = 1420\n")
}

func serverConfigFixture[E service.Validator](t *testing.T, ext E) wgcore.ServerConfig[E] {
	t.Helper()

	key, err := wgcore.NewPrivateKey()
	require.NoError(t, err)

	return wgcore.ServerConfig[E]{
		Addresses:       []netip.Prefix{netip.MustParsePrefix("10.0.0.1/24")},
		EgressInterface: "eth0",
		Extensions:      ext,
		ListenPort:      12345,
		MTU:             1420,
		PrivateKey:      key,
	}
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

func TestServer_LeftoverPeerStats_ReadsBeforeStart(t *testing.T) {
	t.Parallel()

	setup := func(context.Context, string, string, string, []netip.Prefix) (service.Egress, error) {
		return &sdktest.FakeEgress{}, nil
	}

	runner := &fakeRunner{transfer: map[string]service.PeerStats{"peer-a": {RxBytes: 7, TxBytes: 9}}}
	placement := serverPlacementFixture(t)

	s, err := wgcore.NewServerForTest(
		plainVariantFixture(), serverConfigFixture(t, plainExtensions{}), placement, zerolog.Nop(), runner, setup,
	)
	require.NoError(t, err)

	stats, err := s.LeftoverPeerStats(t.Context())
	require.NoError(t, err)
	require.Equal(t, map[service.PeerID]service.PeerStats{"peer-a": {RxBytes: 7, TxBytes: 9}}, stats)
	require.Equal(t, []string{placement.Interface}, runner.queried)

	var _ service.LeftoverStatsReader = s

	runner.transferErr = errors.New("no such device")

	_, err = s.LeftoverPeerStats(t.Context())
	require.ErrorContains(t, err, "no such device")
}

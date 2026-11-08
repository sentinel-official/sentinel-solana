package wgcore_test

import (
	"context"
	"errors"
	"net/netip"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/internal/wgcore"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

// a removed peer's NAT flows must be cleared before its address can go to the next peer,
// and the next peer gets a different address while others are free.
func TestServer_RemovePeer_FlushesConntrackBeforeReuse(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		flushErr error
	}{
		{name: "flush ok"},
		{name: "flush fails: removal still completes", flushErr: errors.New("operation not permitted")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := serverConfigFixture(t, plainExtensions{})
			setup := func(context.Context, string, string, string, []netip.Prefix) (service.Egress, error) {
				return &sdktest.FakeEgress{}, nil
			}

			s, err := wgcore.NewServerForTest(plainVariantFixture(), cfg, serverPlacementFixture(t), zerolog.Nop(), &fakeRunner{}, setup)
			require.NoError(t, err)
			require.NoError(t, s.Start(t.Context()))

			var flushed [][]netip.Addr

			wgcore.SetFlushConntrackForTest(s, func(addrs []netip.Addr) (uint, error) {
				flushed = append(flushed, addrs)

				return 2, tc.flushErr
			})

			add := func() (service.PeerID, string) {
				key := mustPublicKey(t)
				req, err := service.PackRequest(service.Name(fakeName), wgcore.PeerRequest{PublicKey: key})
				require.NoError(t, err)

				resp, err := s.AddPeer(t.Context(), req)
				require.NoError(t, err)

				var p wgcore.PeerResponse[plainExtensions]
				require.NoError(t, resp.Unpack(&p))
				require.Len(t, p.Addresses, 1)

				return service.PeerID(key), p.Addresses[0]
			}

			a, addrA := add()
			require.Equal(t, "10.0.0.2/32", addrA)

			require.NoError(t, s.RemovePeer(t.Context(), a))
			require.Equal(t, [][]netip.Addr{{netip.MustParseAddr("10.0.0.2")}}, flushed,
				"the removed peer's address is flushed exactly once")

			_, err = s.PeerStats(t.Context(), a)
			require.ErrorIs(t, err, service.ErrPeerNotFound, "the peer is gone even when the flush failed")

			_, addrB := add()
			require.NotEqual(t, addrA, addrB, "the next peer does not inherit the address just released")
		})
	}
}

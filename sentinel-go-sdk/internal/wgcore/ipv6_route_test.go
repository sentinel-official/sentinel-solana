package wgcore_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/internal/wgcore"
)

func TestClient_Start_TunnelIPv6OffRoutesIPv4Only(t *testing.T) {
	t.Parallel()

	for _, on := range []bool{true, false} {
		synctest.Test(t, func(t *testing.T) {
			p := clientPlacementFixture(t)
			c := wgcore.NewClientForTest[plainExtensions](plainVariantFixture(), p, zerolog.Nop(), &fakeRunner{live: "utun7"})
			c.SetTunnelIPv6(on)

			_, err := c.PeerRequest(t.Context())
			require.NoError(t, err)
			require.NoError(t, c.Start(t.Context(), "203.0.113.1", peerResponseFixture(t, wgcore.ClientPublicKey(c))))

			raw, err := os.ReadFile(filepath.Join(p.ToolDir, p.Interface+".conf")) //#nosec G304 -- test temp dir
			require.NoError(t, err)

			var allowed string
			for line := range strings.Lines(string(raw)) {
				if v, ok := strings.CutPrefix(line, "AllowedIPs = "); ok {
					allowed = strings.TrimSpace(v)
				}
			}

			want := wgcore.AllowedIPs(runtime.GOOS)
			if !on {
				want = wgcore.AllowedIPsNoIPv6(runtime.GOOS)
			}

			require.Equal(t, want, allowed)
		})
	}
}

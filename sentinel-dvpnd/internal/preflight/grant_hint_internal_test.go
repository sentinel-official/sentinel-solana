package preflight

import (
	"strings"
	"testing"

	"github.com/solana-foundation/solana-go/v2"
	"github.com/stretchr/testify/require"
)

// The hint must be a command the operator can paste: sentinel-dvpn selects the signing key with
// --key.name, and an unknown --key flag makes the command fail before it sends anything.
func TestGrantHint_UsesTheCLIsKeyFlag(t *testing.T) {
	t.Parallel()

	signer := solana.MustPublicKeyFromBase58("2CR1dpuWJWa82dWwbeaWtpQ3Sjputb4oDH55NgJTuEcv")
	hint := grantHint(signer, 6, 0)

	require.True(t, strings.HasPrefix(hint, "sentinel-dvpn grant open-grant "), hint)
	require.Contains(t, hint, " --key.name <owner> ")
	require.NotContains(t, hint, " --key <")
	require.Contains(t, hint, " --grantee "+signer.String()+" ")
	require.Contains(t, hint, " --target 11111111111111111111111111111111 ")
	require.Contains(t, hint, " --permissions 6 --expires-at 0 --allowance 0")
}

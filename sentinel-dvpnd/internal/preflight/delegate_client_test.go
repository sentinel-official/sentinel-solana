package preflight_test

import (
	"bytes"
	"testing"

	bin "github.com/gagliardetto/binary"
	"github.com/rs/zerolog"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/programs/token"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/preflight"
	"github.com/sentinel-official/sentinel-dvpnd/internal/testutil"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

// A client opens channels through the same delegate as a provider, so its start (and doctor) names a
// missing or used-up approval instead of leaving it to a raw Token-2022 refusal at connect.
func TestRun_ClientWarnsWithoutDelegate(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	programID := dvpn.ProgramID
	mint := solana.PublicKey{1}
	owner := solana.PublicKey{2}

	sdktest.PinBuildIdentity(t, programID, mint)
	require.NoError(t, fakeRPC.SetConfig(programID, dvpn.Config{Mint: mint}))

	file := config.DefaultFile("localnet")
	file.RPC = fakeRPC.Config()
	clientCfg := config.DefaultClient()
	clientCfg.Owner = owner.String()
	clientCfg.Network.DNSBackend = config.DNSNone
	clientCfg.Network.KillSwitch = config.KillSwitchOff
	loaded := &config.Loaded{Client: &clientCfg, File: file, Role: config.RoleClient}

	address, _, err := solana.FindAssociatedTokenAddressWithProgram(owner, mint, solana.Token2022ProgramID)
	require.NoError(t, err)
	delegate, _, err := dvpn.FindConfigAddress(programID)
	require.NoError(t, err)

	for _, tc := range []struct {
		name     string
		delegate *solana.PublicKey
		amount   uint64
		warning  bool
	}{
		{name: "missing", warning: true},
		{name: "exhausted", delegate: &delegate, warning: true},
		{name: "approved", delegate: &delegate, amount: 5},
	} {
		var data bytes.Buffer
		account := token.Account{Mint: mint, Owner: owner, Delegate: tc.delegate, DelegatedAmount: tc.amount}
		require.NoError(t, bin.NewBinEncoder(&data).Encode(account))
		fakeRPC.SetAccount(address, sdktest.Account{Data: data.Bytes(), Owner: solana.Token2022ProgramID})

		checks, _ := preflight.Run(t.Context(), loaded, zerolog.Nop(), fakeRPC.NewClient())
		check, ok := testutil.ChecksByName(checks)["delegate"]
		require.True(t, ok, tc.name)
		require.True(t, check.OK, tc.name+": a warning, never a failed start")

		if tc.warning {
			require.Contains(t, check.Detail, "client delegate approval is missing or exhausted", tc.name)
			require.Contains(t, check.Detail, "approve-delegate", tc.name)
		} else {
			require.Empty(t, check.Detail, tc.name)
		}
	}
}

package client_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	bin "github.com/gagliardetto/binary"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/programs/token"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
	"github.com/sentinel-official/sentinel-dvpnd/internal/testutil"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

// The program moves a new channel's escrow as the owner's delegate, so with no approval (or a used-up
// one) Token-2022 refuses open_channel with "insufficient funds", the same answer as an empty wallet.
// The refusal now names the fix; with the approval in place it names the balance instead.
func TestRuntime_StartSession_InsufficientFundsNamesTheDelegateFix(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		approved bool
		code     string // the Token-2022 custom error: 0x1 InsufficientFunds, 0x4 OwnerMismatch
		raw      string
		want     string
	}{
		{name: "no approval", code: "0x1", raw: "InsufficientFunds", want: "approve-delegate"},
		{name: "approved", approved: true, code: "0x1", raw: "InsufficientFunds", want: "wallet balance or allowance is below the escrow"},
		// A revoked approval: the config is no longer the delegate, and Token-2022 says "owner does not match".
		{name: "revoked approval", code: "0x4", raw: "OwnerMismatch", want: "approve-delegate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			registerFakeClientOnce()

			fakeRPC := sdktest.NewFakeRPC(t)
			home := testutil.NewClientHome(t, fakeRPC)
			programClient := installOpenChannelReactor(t, fakeRPC, home)
			fakeRPC.Simulate = func(*solana.Transaction) sdktest.SimulateResult {
				return sdktest.SimulateResult{
					Err: map[string]any{"InstructionError": []any{0, map[string]any{"Custom": customCode(tc.code)}}},
					Logs: []string{
						"Program " + programClient.ProgramID.String() + " invoke [1]",
						"Program " + solana.Token2022ProgramID.String() + " invoke [2]",
						"Program " + solana.Token2022ProgramID.String() + " failed: custom program error: " + tc.code,
						"Program " + programClient.ProgramID.String() + " failed: custom program error: " + tc.code,
					},
				}
			}

			account := token.Account{Mint: programClient.Mint, Owner: home.OwnerAddress}
			if tc.approved {
				delegate, _, err := dvpn.FindConfigAddress(programClient.ProgramID)
				require.NoError(t, err)

				account.Delegate, account.DelegatedAmount = &delegate, 5
			}

			var data bytes.Buffer
			require.NoError(t, bin.NewBinEncoder(&data).Encode(account))

			address, err := programClient.AssociatedTokenAddress(home.OwnerAddress)
			require.NoError(t, err)
			fakeRPC.SetAccount(address, sdktest.Account{Data: data.Bytes(), Owner: solana.Token2022ProgramID})

			rt := newRuntime(t, home, programClient)

			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			_, err = client.StartSession(ctx, rt, newChannelRequest(home.NodeAddress.String()))
			require.ErrorContains(t, err, tc.raw)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func customCode(hex string) int {
	if hex == "0x4" {
		return 4
	}

	return 1
}

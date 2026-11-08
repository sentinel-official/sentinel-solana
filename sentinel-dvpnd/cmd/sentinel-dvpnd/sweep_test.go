package main_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/programs/system"
	"github.com/stretchr/testify/require"

	main "github.com/sentinel-official/sentinel-dvpnd/cmd/sentinel-dvpnd"
	"github.com/sentinel-official/sentinel-dvpnd/internal/daemon"
	"github.com/sentinel-official/sentinel-go-sdk/v3/key"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
)

const sweepBalance = 490_000_000

func newSweepInstance(t *testing.T) (string, solana.PublicKey) {
	t.Helper()

	home := t.TempDir()

	result, err := runInitCmd(t, home, "--instance", "c0", "--owner", randomOwner(t), "--plain-key")
	require.NoError(t, err)

	privateKey, err := key.Load(filepath.Join(home, "keys", result.KeyName+".json"), "")
	require.NoError(t, err)

	return home, privateKey.PublicKey()
}

func runSweepCmd(t *testing.T, home string, fakeRPC *sdktest.FakeRPC, args ...string) (main.SweepResult, error) {
	t.Helper()

	cmd := main.NewRootCmd()

	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&bytes.Buffer{})
	cmd.SetArgs(append([]string{
		"key", "sweep", "--home", home, "--instance", "c0",
		"--rpc.url", fakeRPC.Config().URL, "--rpc.ws-url", fakeRPC.Config().WSURL,
	}, args...))

	if err := cmd.ExecuteContext(t.Context()); err != nil {
		return main.SweepResult{}, fmt.Errorf("running key sweep: %w", err)
	}

	var result main.SweepResult
	require.NoError(t, json.NewDecoder(&output).Decode(&result))

	return result, nil
}

// A retired device kept its fee SOL: no shipped command could move it off the device key.
func TestKeySweep_SendsEverythingButTheFee(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	home, from := newSweepInstance(t)
	fakeRPC.SetAccount(from, sdktest.Account{Lamports: sweepBalance, Owner: solana.SystemProgramID})
	dest := solana.MustPublicKeyFromBase58(randomOwner(t))

	result, err := runSweepCmd(t, home, fakeRPC, "--to", dest.String())
	require.NoError(t, err)

	require.Equal(t, uint64(sdktest.FeeLamports), result.Fee)
	require.Equal(t, uint64(sweepBalance-sdktest.FeeLamports), result.Lamports)
	require.Equal(t, from.String(), result.From)
	require.NotEmpty(t, result.Signature)

	sent := fakeRPC.Sent()
	require.Len(t, sent, 1)
	require.Equal(t, from, sent[0].Message.AccountKeys[0], "the device key pays its own fee")
	require.Len(t, sent[0].Message.Instructions, 1, "one transfer and no compute-budget instructions, so the quoted fee is the whole cost")

	ix := sent[0].Message.Instructions[0]
	accounts, err := ix.ResolveInstructionAccounts(&sent[0].Message)
	require.NoError(t, err)

	decoded, err := system.DecodeInstruction(accounts, ix.Data)
	require.NoError(t, err)

	transfer, ok := decoded.Impl.(*system.Transfer)
	require.True(t, ok, "a system transfer")
	require.Equal(t, uint64(sweepBalance-sdktest.FeeLamports), *transfer.Lamports)
	require.Equal(t, dest, transfer.GetRecipientAccount().PublicKey)
}

func TestKeySweep_DryRunSendsNothing(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	home, from := newSweepInstance(t)
	fakeRPC.SetAccount(from, sdktest.Account{Lamports: sweepBalance, Owner: solana.SystemProgramID})

	result, err := runSweepCmd(t, home, fakeRPC, "--to", randomOwner(t), "--dry-run")
	require.NoError(t, err)

	require.True(t, result.DryRun)
	require.Equal(t, uint64(sweepBalance-sdktest.FeeLamports), result.Lamports)
	require.Empty(t, result.Signature)
	require.Empty(t, fakeRPC.Sent())
}

func TestKeySweep_Refusals(t *testing.T) {
	t.Parallel()

	t.Run("empty key", func(t *testing.T) {
		t.Parallel()

		fakeRPC := sdktest.NewFakeRPC(t)
		home, _ := newSweepInstance(t)

		_, err := runSweepCmd(t, home, fakeRPC, "--to", randomOwner(t))
		require.ErrorContains(t, err, "nothing to sweep")
		require.Empty(t, fakeRPC.Sent())
	})

	t.Run("daemon running", func(t *testing.T) {
		t.Parallel()

		fakeRPC := sdktest.NewFakeRPC(t)
		home, from := newSweepInstance(t)
		fakeRPC.SetAccount(from, sdktest.Account{Lamports: sweepBalance, Owner: solana.SystemProgramID})

		release, err := daemon.Lock(home)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, release()) })

		_, err = runSweepCmd(t, home, fakeRPC, "--to", randomOwner(t))
		require.ErrorContains(t, err, "stop the daemon first")
		require.Empty(t, fakeRPC.Sent())
	})

	t.Run("to itself", func(t *testing.T) {
		t.Parallel()

		fakeRPC := sdktest.NewFakeRPC(t)
		home, from := newSweepInstance(t)
		fakeRPC.SetAccount(from, sdktest.Account{Lamports: sweepBalance, Owner: solana.SystemProgramID})

		_, err := runSweepCmd(t, home, fakeRPC, "--to", from.String())
		require.ErrorContains(t, err, "own key")
		require.Empty(t, fakeRPC.Sent())
	})
}

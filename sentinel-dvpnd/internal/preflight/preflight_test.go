package preflight_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	bin "github.com/gagliardetto/binary"
	"github.com/rs/zerolog"
	"github.com/solana-foundation/solana-go/v2"
	"github.com/solana-foundation/solana-go/v2/programs/token"
	"github.com/solana-foundation/solana-go/v2/rpc"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/config"
	"github.com/sentinel-official/sentinel-dvpnd/internal/preflight"
	"github.com/sentinel-official/sentinel-dvpnd/internal/testutil"
	"github.com/sentinel-official/sentinel-go-sdk/v3/key"
	"github.com/sentinel-official/sentinel-go-sdk/v3/sdktest"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/tx"
	"github.com/sentinel-official/sentinel-go-sdk/v3/version"
	_ "github.com/sentinel-official/sentinel-go-sdk/v3/wireguard"
	"github.com/sentinel-official/sentinel-programs/clients/go/dvpn"
)

func TestCheckDNSBackend_None(t *testing.T) {
	t.Parallel()
	require.NoError(t, preflight.CheckDNSBackend(config.DNSNone))
}

func TestCheckKillSwitchBackend_Canceled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, preflight.CheckKillSwitchBackend(ctx), context.Canceled)
}

func TestCheckKillSwitchBackend_ReportsMissingTool(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	err := preflight.CheckKillSwitchBackend(t.Context())
	if err == nil {
		t.Skip("kill switch backend found outside PATH")
	}

	require.ErrorContains(t, err, "kill switch backend")
}

func TestCheckPrivileges_ReturnsDecision(t *testing.T) {
	t.Parallel()

	if err := preflight.CheckPrivileges(); err != nil {
		require.ErrorContains(t, err, "privileges")
	}
}

func TestDNSBackendHelp(t *testing.T) {
	t.Parallel()

	cases := []struct {
		goos string
		want string
	}{
		{goos: "linux", want: "want resolvectl, resolvconf, or none (or set SENTINEL_DVPND_NETWORK_DNS_BACKEND)"},
		{goos: "darwin", want: "want networksetup or none (or set SENTINEL_DVPND_NETWORK_DNS_BACKEND)"},
		{goos: "windows", want: "want netsh or none (or set SENTINEL_DVPND_NETWORK_DNS_BACKEND)"},
	}

	for _, tc := range cases {
		t.Run(tc.goos, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, preflight.DNSBackendHelp(tc.goos))
		})
	}
}

//nolint:paralleltest
func TestRun_BuildFailure(t *testing.T) {
	sdktest.PinBuildIdentity(t, solana.PublicKey{}, solana.PublicKey{})

	version.ProgramID = "invalid"

	file := config.DefaultFile("localnet")
	loaded := &config.Loaded{
		File: file,
		Role: config.RoleNode,
	}
	checks, err := preflight.Run(t.Context(), loaded, zerolog.Nop(), nil)
	require.ErrorIs(t, err, preflight.ErrPreflightFailed)
	require.EqualError(t, err, "preflight failed: build")

	byName := testutil.ChecksByName(checks)
	require.False(t, byName["build"].OK)
	require.True(t, strings.HasPrefix(byName["build"].Detail, "parsing program id"))
}

//nolint:paralleltest
func TestRun_BuildFailureWhenVersionUnset(t *testing.T) {
	previousProgramID, previousMint := version.ProgramID, version.Mint

	t.Cleanup(func() { version.ProgramID, version.Mint = previousProgramID, previousMint })

	version.ProgramID, version.Mint = "", ""
	file := config.DefaultFile("localnet")
	file.RPC.URL = "http://127.0.0.1:1"
	file.RPC.WSURL = "ws://127.0.0.1:1"
	loaded := &config.Loaded{File: file, Node: &config.Node{Service: "wireguard"}, Role: config.RoleNode}
	checks, err := preflight.Run(t.Context(), loaded, zerolog.Nop(), nil)
	require.ErrorIs(t, err, preflight.ErrPreflightFailed)
	require.EqualError(t, err, "preflight failed: build")

	byName := testutil.ChecksByName(checks)
	require.False(t, byName["build"].OK)
	require.Contains(t, byName["build"].Detail, version.ErrVersionUnset.Error())
	require.NotContains(t, byName, "rpc")
}

func TestRun_ClientChecks(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	programID := dvpn.ProgramID
	sdktest.PinBuildIdentity(t, programID, solana.PublicKey{1})
	require.NoError(t, fakeRPC.SetConfig(programID, dvpn.Config{Mint: solana.PublicKey{2}}))

	file := config.DefaultFile("localnet")
	file.RPC = fakeRPC.Config()
	client := config.DefaultClient()
	client.Network.DNSBackend = config.DNSNone
	loaded := &config.Loaded{Client: &client, File: file, Role: config.RoleClient}

	checks, err := preflight.Run(t.Context(), loaded, zerolog.Nop(), fakeRPC.NewClient())
	require.Error(t, err)

	byName := testutil.ChecksByName(checks)
	require.Contains(t, byName, "privileges")
	require.Contains(t, byName, "kill_switch_backend")
	require.True(t, byName["dns_backend"].OK)

	client.Network.KillSwitch = config.KillSwitchOff
	checks, _ = preflight.Run(t.Context(), loaded, zerolog.Nop(), fakeRPC.NewClient())
	require.NotContains(t, testutil.ChecksByName(checks), "kill_switch_backend")
}

func TestRun_MintMismatch(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	programID := dvpn.ProgramID
	sdktest.PinBuildIdentity(t, programID, solana.PublicKey{1})
	require.NoError(t, fakeRPC.SetConfig(programID, dvpn.Config{Mint: solana.PublicKey{2}}))

	file := config.DefaultFile("localnet")
	file.RPC = fakeRPC.Config()
	loaded := &config.Loaded{File: file, Node: &config.Node{Service: "wireguard"}, Role: config.RoleNode}
	checks, err := preflight.Run(t.Context(), loaded, zerolog.Nop(), fakeRPC.NewClient())
	require.Error(t, err)

	byName := testutil.ChecksByName(checks)
	require.False(t, byName["mint"].OK)
	require.True(t, byName["rpc"].OK)
	require.True(t, byName["service_binaries_wireguard"].OK)

	for _, check := range checks {
		if strings.HasPrefix(check.Name, "service_userspace") {
			require.True(t, check.OK)
		}
	}
}

func TestRun_PaymasterCheck(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	mint := solana.PublicKey{1}
	sdktest.PinBuildIdentity(t, dvpn.ProgramID, mint)
	require.NoError(t, fakeRPC.SetConfig(dvpn.ProgramID, dvpn.Config{Mint: mint}))

	owner := solana.NewWallet()
	provider := config.DefaultProvider()
	provider.API.Port = 0
	provider.ID = strings.Repeat("00", 32)
	provider.Owner = owner.PublicKey().String()

	file := config.DefaultFile("localnet")
	file.RPC = fakeRPC.Config()
	base := &config.Loaded{Dir: t.TempDir(), File: file, Provider: &provider, Role: config.RoleProvider}
	keyPath, err := base.KeyPath()
	require.NoError(t, err)
	require.NoError(t, key.Create(keyPath, owner.PrivateKey, ""))

	signer := solana.NewWallet().PublicKey()
	kora := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":{"payment_address":%[1]q,"signer_address":%[1]q,`+
			`"validation_config":{"price":{"type":"margin"}}}}`, signer)
	}))
	t.Cleanup(kora.Close)

	cases := []struct {
		fallback   bool
		name       string
		url        string
		wantDetail []string
		wantErr    error
		wantNoText []string
		wantOK     bool
	}{
		{
			name:       "reachable",
			url:        kora.URL,
			wantDetail: []string{kora.URL, signer.String(), "margin", mint.String()},
			wantOK:     true,
		},
		{
			name:       "reachable redacts the url",
			url:        kora.URL + "/rpc?token=secret",
			wantDetail: []string{kora.URL},
			wantNoText: []string{"secret", "/rpc"},
			wantOK:     true,
		},
		{
			name:       "unreachable fallback off",
			url:        "http://127.0.0.1:1",
			wantDetail: []string{"paymaster getPayerSigner"},
			wantErr:    preflight.ErrPreflightFailed,
		},
		{
			fallback:   true,
			name:       "unreachable fallback on",
			url:        "http://127.0.0.1:1",
			wantDetail: []string{"paymaster getPayerSigner", "the signer pays fees and rent"},
			wantOK:     true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			loaded := *base
			loaded.File.Tx.Paymaster = tx.PaymasterConfig{Fallback: tc.fallback, URL: tc.url}

			checks, err := preflight.Run(t.Context(), &loaded, zerolog.Nop(), fakeRPC.NewClient())
			require.ErrorIs(t, err, tc.wantErr)

			check := testutil.ChecksByName(checks)["paymaster"]
			require.Equal(t, tc.wantOK, check.OK)

			for _, want := range tc.wantDetail {
				require.Contains(t, check.Detail, want)
			}

			for _, unwanted := range tc.wantNoText {
				require.NotContains(t, check.Detail, unwanted)
			}
		})
	}
}

func TestRun_ProviderWarnsWithoutDelegate(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	programID := dvpn.ProgramID
	mint := solana.PublicKey{1}
	owner := solana.PublicKey{2}

	sdktest.PinBuildIdentity(t, programID, mint)
	require.NoError(t, fakeRPC.SetConfig(programID, dvpn.Config{Mint: mint}))

	file := config.DefaultFile("localnet")
	file.RPC = fakeRPC.Config()
	loaded := &config.Loaded{
		File: file,
		Provider: &config.Provider{
			Owner: owner.String(),
		},
		Role: config.RoleProvider,
	}
	checks, _ := preflight.Run(t.Context(), loaded, zerolog.Nop(), fakeRPC.NewClient())

	byName := testutil.ChecksByName(checks)
	require.Contains(t, byName, "delegate")
	require.True(t, byName["delegate"].OK)
	require.Contains(t, byName["delegate"].Detail, "approve-delegate")

	address, _, err := solana.FindAssociatedTokenAddressWithProgram(owner, mint, solana.Token2022ProgramID)
	require.NoError(t, err)
	delegate, _, err := dvpn.FindConfigAddress(programID)
	require.NoError(t, err)

	wrongDelegate := solana.PublicKey{3}
	for _, tc := range []struct {
		name     string
		delegate *solana.PublicKey
		amount   uint64
		warning  bool
	}{
		{
			name:     "wrong",
			delegate: &wrongDelegate,
			amount:   5,
			warning:  true,
		},
		{
			name:     "exhausted",
			delegate: &delegate,
			warning:  true,
		},
		{
			name:     "approved",
			delegate: &delegate,
			amount:   5,
		},
	} {
		account := token.Account{
			Mint:            mint,
			Owner:           owner,
			Delegate:        tc.delegate,
			DelegatedAmount: tc.amount,
		}

		var data bytes.Buffer
		require.NoError(t, bin.NewBinEncoder(&data).Encode(account))
		fakeRPC.SetAccount(address, sdktest.Account{
			Data:  data.Bytes(),
			Owner: solana.Token2022ProgramID,
		})

		checks, _ := preflight.Run(t.Context(), loaded, zerolog.Nop(), fakeRPC.NewClient())
		check := testutil.ChecksByName(checks)["delegate"]
		require.True(t, check.OK, tc.name)

		if tc.warning {
			require.Contains(t, check.Detail, "approve-delegate", tc.name)
		} else {
			require.Empty(t, check.Detail, tc.name)
		}
	}
}

func TestRun_RPCUnreachable(t *testing.T) {
	t.Parallel()

	sdktest.PinBuildIdentity(t, dvpn.ProgramID, solana.PublicKey{1})

	file := config.DefaultFile("localnet")
	file.RPC.URL = "http://127.0.0.1:1"
	file.RPC.WSURL = "ws://127.0.0.1:1"
	file.RPC.Timeout = 100 * time.Millisecond
	loaded := &config.Loaded{
		File: file,
		Node: &config.Node{Service: "wireguard"},
		Role: config.RoleNode,
	}
	checks, err := preflight.Run(t.Context(), loaded, zerolog.Nop(), rpc.New(file.RPC.URL))
	require.EqualError(t, err, "preflight failed: rpc")
	require.ErrorIs(t, err, preflight.ErrRPCUnreachable, "an unanswered health check is a network failure")

	byName := testutil.ChecksByName(checks)
	require.False(t, byName["rpc"].OK)
	require.Equal(t, service.SeverityError, byName["rpc"].Severity)
}

func TestRun_WarnsPeerAllowPrivate(t *testing.T) {
	t.Parallel()

	fakeRPC := sdktest.NewFakeRPC(t)
	programID := dvpn.ProgramID
	sdktest.PinBuildIdentity(t, programID, solana.PublicKey{1})
	require.NoError(t, fakeRPC.SetConfig(programID, dvpn.Config{Mint: solana.PublicKey{2}}))

	file := config.DefaultFile("localnet")
	file.RPC = fakeRPC.Config()
	client := config.DefaultClient()
	client.Network.DNSBackend = config.DNSNone
	loaded := &config.Loaded{Client: &client, File: file, Role: config.RoleClient}

	checks, _ := preflight.Run(t.Context(), loaded, zerolog.Nop(), fakeRPC.NewClient())
	require.NotContains(t, testutil.ChecksByName(checks), "peer_allow_private")

	loaded.File.Peer.AllowPrivate = true
	checks, _ = preflight.Run(t.Context(), loaded, zerolog.Nop(), fakeRPC.NewClient())
	require.Contains(t, testutil.ChecksByName(checks), "peer_allow_private")
	require.True(t, testutil.ChecksByName(checks)["peer_allow_private"].OK)
	require.Equal(t, service.SeverityWarning, testutil.ChecksByName(checks)["peer_allow_private"].Severity)
}

package wgcore_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/internal/wgcore"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

func TestParseTransfer(t *testing.T) {
	t.Parallel()

	out := "key-1\t1024\t2048\n" +
		"key-2\t0\t7\n"

	transfer, err := wgcore.ParseTransfer(out)
	require.NoError(t, err)
	require.Len(t, transfer, 2)
	require.EqualValues(t, 1024, transfer["key-1"].RxBytes)
	require.EqualValues(t, 2048, transfer["key-1"].TxBytes)
	require.EqualValues(t, 7, transfer["key-2"].TxBytes)

	transfer, err = wgcore.ParseTransfer("")
	require.NoError(t, err)
	require.Empty(t, transfer)

	_, err = wgcore.ParseTransfer("key\tx\t1\n")
	require.Error(t, err)
}

func TestParseTransfer_ScannerError(t *testing.T) {
	t.Parallel()

	_, err := wgcore.ParseTransfer("key\t" + strings.Repeat("1", 70*1024) + "\t2\n")
	require.ErrorContains(t, err, "scanning transfer")
}

func TestQuickRunner_Cleanup(t *testing.T) {
	t.Parallel()

	if runtime.GOOS != "linux" {
		t.Skip("the live interface is the config name on linux only")
	}

	cases := []struct {
		name    string
		showErr string
		want    []string
		wantErr string
	}{
		{
			name: "held",
			want: []string{"show", "down"},
		},
		{
			name:    "interface gone",
			showErr: "running wg: exit status 1: Unable to access interface: No such device",
			want:    []string{"show"},
		},
		{
			name:    "not a wireguard device",
			showErr: "running wg: exit status 1: Unable to access interface: Operation not supported",
			want:    []string{"show"},
		},
		{
			name:    "kernel module missing",
			showErr: "running awg: exit status 1: Unable to access interface: Protocol not supported",
			want:    []string{"show"},
		},
		{
			name:    "show denied",
			showErr: "running wg: exit status 1: Unable to access interface: Permission denied",
			want:    []string{"show"},
			wantErr: "Permission denied",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var verbs []string

			exec := func(_ context.Context, c service.Command) ([]byte, error) {
				verbs = append(verbs, c.Args[0])

				if c.Args[0] == "show" && tc.showErr != "" {
					return nil, errors.New(tc.showErr)
				}

				return nil, nil
			}
			r := wgcore.NewQuickRunnerForTest(plainVariantFixture(), "linux", exec)

			err := r.Cleanup(t.Context(), "/etc/wireguard/sentinel-dvpn/dvpn-c0.conf", true)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.want, verbs)
		})
	}
}

func TestQuickRunner_UpDown(t *testing.T) {
	t.Parallel()

	if runtime.GOOS != "linux" {
		t.Skip("the live interface is the config name on linux only")
	}

	var commands []service.Command

	exec := func(_ context.Context, c service.Command) ([]byte, error) {
		commands = append(commands, c)

		return []byte("key-1\t1\t2\n"), nil
	}
	r := wgcore.NewQuickRunnerForTest(plainVariantFixture(), "linux", exec)
	conf := "/var/lib/sentinel-dvpn/c0/wireguard/dvpn-c0.conf"

	live, err := r.Up(t.Context(), conf)
	require.NoError(t, err)
	require.Equal(t, "dvpn-c0", live)
	require.NoError(t, r.Down(t.Context(), conf))
	transfer, err := r.Transfer(t.Context(), live)
	require.NoError(t, err)
	require.Len(t, transfer, 1)
	require.Equal(t, []service.Command{
		{Args: []string{"up", conf}, Path: "wg-quick", Unset: []string{"WG_QUICK_USERSPACE_IMPLEMENTATION"}},
		{Args: []string{"down", conf}, Path: "wg-quick", Unset: []string{"WG_QUICK_USERSPACE_IMPLEMENTATION"}},
		{Args: []string{"show", live, "transfer"}, Path: "wg"},
	}, commands)
}

// A userspace implementation that failed the trust check is invisible to awg-quick, which then
// tries the kernel module and fails with a kernel error. The start error names the refused tool,
// why it was refused and what to do.
func TestQuickRunner_UpError_NamesARefusedUserspaceTool(t *testing.T) {
	t.Parallel()

	amneziawg := wgcore.Variant{Binary: "awg", TunnelBinary: "awg-quick", UserspaceBinary: "amneziawg-go"}
	kernel := errors.New("running awg-quick: exit status 1: [#] ip link add dvpn-n0 type amneziawg\n" +
		"Error: Unknown device type.\nUnable to access interface: Protocol not supported")
	refused := fmt.Errorf("%w: /opt/lab/bin/amneziawg-go (owner uid 1000, mode -rwxrwxr-x)", service.ErrUntrustedBinary)
	run := func(context.Context, service.Command) ([]byte, error) { return nil, kernel }

	cases := []struct {
		lookErr error
		name    string
		want    []string
		without string
	}{
		{
			lookErr: refused,
			name:    "refused",
			want: []string{
				"amneziawg-go was found but not used, because another account could replace it",
				"/opt/lab/bin/amneziawg-go (owner uid 1000, mode -rwxrwxr-x)",
				"owned by root and writable only by root",
			},
		},
		{
			lookErr: fmt.Errorf("%w: amneziawg-go", service.ErrBinaryNotFound),
			name:    "absent",
			without: "found but not used",
		},
		{
			name:    "trusted",
			without: "found but not used",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			look := func(string) (string, error) { return "/usr/local/bin/amneziawg-go", tc.lookErr }
			_, err := wgcore.NewQuickRunnerWithLookPathForTest(amneziawg, "linux", run, look).
				Up(t.Context(), "/etc/amnezia/amneziawg/dvpn-n0.conf")
			require.ErrorIs(t, err, kernel)
			require.ErrorContains(t, err, "awg-quick up: running awg-quick")

			for _, want := range tc.want {
				require.ErrorContains(t, err, want)
			}

			if tc.lookErr != nil && errors.Is(tc.lookErr, service.ErrUntrustedBinary) {
				require.ErrorIs(t, err, service.ErrUntrustedBinary)
			}

			if tc.without != "" {
				require.NotContains(t, err.Error(), tc.without)
			}
		})
	}
}

func TestQuickRunner_UpError(t *testing.T) {
	t.Parallel()

	wireguard := wgcore.Variant{
		Binary: "wg", Confinable: true, TunnelBinary: "wg-quick", UserspaceBinary: "wireguard-go",
	}
	amneziawg := wgcore.Variant{Binary: "awg", TunnelBinary: "awg-quick", UserspaceBinary: "amneziawg-go"}
	denied := "running wg-quick: exit status 1: [#] wireguard-go dvpn-n0\nUnable to modify interface: Permission denied"

	cases := []struct {
		err     string
		goos    string
		name    string
		variant wgcore.Variant
		want    string
	}{
		{
			err:     denied,
			goos:    "linux",
			name:    "userspace denied",
			variant: wireguard,
			want:    "this host confines `wg`; load the WireGuard kernel module: running wg-quick",
		},
		{
			err:     "running wg-quick: exit status 1: mkdir /etc/wireguard: Permission denied",
			goos:    "linux",
			name:    "other denied",
			variant: wireguard,
			want:    "wg-quick up: running wg-quick",
		},
		{
			err:     denied,
			goos:    "darwin",
			name:    "not linux",
			variant: wireguard,
			want:    "wg-quick up: running wg-quick",
		},
		{
			err:     "running awg-quick: exit status 1: [#] amneziawg-go dvpn-n0\nUnable to modify interface: Permission denied",
			goos:    "linux",
			name:    "unconfined variant",
			variant: amneziawg,
			want:    "awg-quick up: running awg-quick",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			want := errors.New(tc.err)
			r := wgcore.NewQuickRunnerForTest(tc.variant, tc.goos, func(context.Context, service.Command) ([]byte, error) {
				return nil, want
			})
			live, err := r.Up(t.Context(), "/etc/wireguard/sentinel-dvpn/dvpn-n0.conf")
			require.Empty(t, live)
			require.ErrorIs(t, err, want)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestWindowsServiceRunner_Cleanup(t *testing.T) {
	t.Parallel()

	lookupErr := errors.New("scm unavailable")
	conf := `C:\ProgramData\sentinel-dvpn\c0\wireguard\dvpn-c0.conf`

	cases := []struct {
		installed bool
		lookupErr error
		name      string
		owned     bool
		want      []service.Command
		wantErr   error
	}{
		{
			name:  "absent",
			owned: true,
		},
		{
			installed: true,
			name:      "installed",
			want:      []service.Command{{Args: []string{"/uninstalltunnelservice", "dvpn-c0"}, Path: "wireguard"}},
		},
		{
			lookupErr: lookupErr,
			name:      "lookup error",
			owned:     true,
			wantErr:   lookupErr,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var (
				commands []service.Command
				looked   []string
			)

			exec := func(_ context.Context, c service.Command) ([]byte, error) {
				commands = append(commands, c)

				return nil, nil
			}
			installed := func(name string) (bool, error) {
				looked = append(looked, name)

				return tc.installed, tc.lookupErr
			}
			r := wgcore.NewWindowsServiceRunnerForTest("wg", "wireguard", "WireGuardTunnel$", exec, installed)

			err := r.Cleanup(t.Context(), conf, tc.owned)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, []string{"WireGuardTunnel$dvpn-c0"}, looked)
			require.Equal(t, tc.want, commands)
		})
	}
}

func TestWindowsServiceRunner_UpDown(t *testing.T) {
	t.Parallel()

	var commands []service.Command

	exec := func(_ context.Context, c service.Command) ([]byte, error) {
		commands = append(commands, c)

		return nil, nil
	}
	r := wgcore.NewWindowsServiceRunnerForTest("wg", "wireguard", "WireGuardTunnel$", exec, nil)

	conf := `C:\ProgramData\sentinel-dvpn\c0\wireguard\dvpn-c0.conf`

	live, err := r.Up(t.Context(), conf)
	require.NoError(t, err)
	require.Equal(t, "dvpn-c0", live)
	require.NoError(t, r.Down(t.Context(), conf))

	want := []service.Command{
		{Args: []string{"/installtunnelservice", conf}, Path: "wireguard"},
		{Args: []string{"/uninstalltunnelservice", "dvpn-c0"}, Path: "wireguard"},
	}
	require.Equal(t, want, commands)
}

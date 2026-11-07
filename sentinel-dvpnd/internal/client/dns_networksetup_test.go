package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-dvpnd/internal/client"
)

const listServicesOutput = "An asterisk (*) denotes that a network service is disabled.\n" +
	"Wi-Fi\n*Thunderbolt Bridge\nUSB 10/100/1000 LAN\n"

func TestNetworksetupBackend_Apply_KeepsExistingBackup(t *testing.T) {
	t.Parallel()

	backup := filepath.Join(t.TempDir(), "networksetup-dns.json.bak")
	require.NoError(t, os.WriteFile(backup, []byte(`{"Wi-Fi":["192.0.2.53"]}`), 0o600))

	run := &fakeRun{outputs: map[string]string{"networksetup -listallnetworkservices": "Wi-Fi\n"}}
	backend := client.NewNetworksetupBackend(backup, run.run)

	require.NoError(t, backend.Apply(t.Context(), "utun3", []string{"1.1.1.1"}))

	raw, err := os.ReadFile(backup) //#nosec G304
	require.NoError(t, err)
	require.JSONEq(t, `{"Wi-Fi":["192.0.2.53"]}`, string(raw))
	require.NotContains(t, run.commands(), "networksetup -getdnsservers Wi-Fi")
	require.NotContains(t, run.commands(), "networksetup -listallnetworkservices")
}

func TestNetworksetupBackend_Apply_ThenRevert(t *testing.T) {
	t.Parallel()

	backup := filepath.Join(t.TempDir(), "networksetup-dns.json.bak")
	run := &fakeRun{outputs: map[string]string{
		"networksetup -listallnetworkservices":            listServicesOutput,
		"networksetup -getdnsservers Wi-Fi":               "192.0.2.53\n",
		"networksetup -getdnsservers USB 10/100/1000 LAN": "There aren't any DNS Servers set on USB 10/100/1000 LAN.\n",
	}}
	backend := client.NewNetworksetupBackend(backup, run.run)

	require.NoError(t, backend.Apply(t.Context(), "utun3", []string{"1.1.1.1", "1.0.0.1"}))

	raw, err := os.ReadFile(backup) //#nosec G304
	require.NoError(t, err)

	var saved map[string][]string
	require.NoError(t, json.Unmarshal(raw, &saved))
	require.Equal(t, map[string][]string{"Wi-Fi": {"192.0.2.53"}, "USB 10/100/1000 LAN": nil}, saved)
	require.Equal(t, []string{
		"networksetup -listallnetworkservices",
		"networksetup -getdnsservers Wi-Fi",
		"networksetup -getdnsservers USB 10/100/1000 LAN",
		"networksetup -setdnsservers USB 10/100/1000 LAN 1.1.1.1 1.0.0.1",
		"networksetup -setdnsservers Wi-Fi 1.1.1.1 1.0.0.1",
		"dscacheutil -flushcache",
		"killall -HUP mDNSResponder",
	}, run.commands())
	require.Equal(t, []string{"-setdnsservers", "USB 10/100/1000 LAN", "1.1.1.1", "1.0.0.1"}, run.calls[3].args)

	run.calls = nil

	require.NoError(t, backend.Revert(t.Context(), "utun3"))
	require.Equal(t, []string{
		"networksetup -setdnsservers USB 10/100/1000 LAN Empty",
		"networksetup -setdnsservers Wi-Fi 192.0.2.53",
		"dscacheutil -flushcache",
		"killall -HUP mDNSResponder",
	}, run.commands())
	require.Equal(t, []string{"-setdnsservers", "USB 10/100/1000 LAN", "Empty"}, run.calls[0].args)

	_, err = os.Stat(backup)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestNetworksetupBackend_Apply_UsesSavedServices(t *testing.T) {
	t.Parallel()

	backup := filepath.Join(t.TempDir(), "networksetup-dns.json.bak")
	run := &fakeRun{outputs: map[string]string{
		"networksetup -getdnsservers Wi-Fi": "192.0.2.53\n",
	}}
	lists := 0
	runChangingServices := func(ctx context.Context, input, name string, args ...string) ([]byte, error) {
		if name == "networksetup" && len(args) == 1 && args[0] == "-listallnetworkservices" {
			lists++
			if lists == 1 {
				return []byte("Wi-Fi\n"), nil
			}

			return []byte("Wi-Fi\nNew Office LAN\n"), nil
		}

		return run.run(ctx, input, name, args...)
	}
	backend := client.NewNetworksetupBackend(backup, runChangingServices)

	require.NoError(t, backend.Apply(t.Context(), "utun3", []string{"1.1.1.1"}))
	require.Equal(t, 1, lists)
	require.Equal(t, []string{
		"networksetup -getdnsservers Wi-Fi",
		"networksetup -setdnsservers Wi-Fi 1.1.1.1",
		"dscacheutil -flushcache",
		"killall -HUP mDNSResponder",
	}, run.commands())
	require.NoError(t, backend.Revert(t.Context(), "utun3"))
	require.NotContains(t, run.commands(), "networksetup -setdnsservers New Office LAN 1.1.1.1")
}

func TestNetworksetupBackend_Cleanup_AcrossModeSwitch(t *testing.T) {
	t.Parallel()

	backup := filepath.Join(t.TempDir(), "networksetup-dns.json.bak")
	run := &fakeRun{}
	backend := client.NewDarwinDNSBackend("none", filepath.Dir(backup), run.run)

	require.NoError(t, backend.Cleanup(t.Context()))
	require.Empty(t, run.commands())
	require.NoError(t, os.WriteFile(backup, []byte(`{"Wi-Fi":["192.0.2.53"]}`), 0o600))
	require.NoError(t, backend.Cleanup(t.Context()))
	require.Equal(t, []string{
		"networksetup -setdnsservers Wi-Fi 192.0.2.53",
		"dscacheutil -flushcache",
		"killall -HUP mDNSResponder",
	}, run.commands())
}

func TestNetworksetupBackend_Cleanup_StatError(t *testing.T) {
	t.Parallel()

	// A file used as a directory gives ENOTDIR on macOS/Linux, but "path not found" (= not exist)
	// on Windows, so the stat error this test needs cannot happen there. The backend is macOS-only.
	if runtime.GOOS == "windows" {
		t.Skip("Windows reports a file-as-directory path as not existing; networksetup runs on macOS only")
	}

	parent := filepath.Join(t.TempDir(), "parent")
	require.NoError(t, os.WriteFile(parent, []byte("file"), 0o600))
	backend := client.NewNetworksetupBackend(filepath.Join(parent, "networksetup-dns.json.bak"), (&fakeRun{}).run)

	require.Error(t, backend.Cleanup(t.Context()))
}

func TestNetworksetupBackend_Revert_KeepsBackupOnFailure(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		fail  string
		apply bool
	}{
		{"partial apply", "networksetup -setdnsservers Wi-Fi 1.1.1.1", true},
		{"apply flush", "dscacheutil -flushcache", true},
		{"revert service", "networksetup -setdnsservers Wi-Fi 192.0.2.53", false},
		{"revert flush", "dscacheutil -flushcache", false},
		{"revert responder", "killall -HUP mDNSResponder", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backup := filepath.Join(t.TempDir(), "networksetup-dns.json.bak")
			run := &fakeRun{outputs: map[string]string{
				"networksetup -listallnetworkservices":            listServicesOutput,
				"networksetup -getdnsservers Wi-Fi":               "192.0.2.53\n",
				"networksetup -getdnsservers USB 10/100/1000 LAN": "There aren't any DNS Servers set on USB 10/100/1000 LAN.\n",
			}, errs: map[string]error{tc.fail: errors.New("command failed")}}

			backend := client.NewNetworksetupBackend(backup, run.run)
			if tc.apply {
				require.ErrorContains(t, backend.Apply(t.Context(), "utun3", []string{"1.1.1.1"}), "command failed")
			} else {
				require.NoError(t, os.WriteFile(backup, []byte(`{"USB 10/100/1000 LAN":null,"Wi-Fi":["192.0.2.53"]}`), 0o600))
				require.ErrorContains(t, backend.Revert(t.Context(), "utun3"), "command failed")
			}

			_, err := os.Stat(backup)
			require.NoError(t, err)

			delete(run.errs, tc.fail)
			run.calls = nil

			require.NoError(t, backend.Cleanup(t.Context()))
			require.Equal(t, []string{
				"networksetup -setdnsservers USB 10/100/1000 LAN Empty",
				"networksetup -setdnsservers Wi-Fi 192.0.2.53",
				"dscacheutil -flushcache",
				"killall -HUP mDNSResponder",
			}, run.commands())

			_, err = os.Stat(backup)
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func TestParseDNSServers(t *testing.T) {
	t.Parallel()

	require.Nil(t, client.ParseDNSServers([]byte("There aren't any DNS Servers set on Wi-Fi.\n")))
	require.Equal(t, []string{"192.0.2.53", "2001:db8::53"}, client.ParseDNSServers([]byte("192.0.2.53\n2001:db8::53\n")))
}

func TestParseNetworkServices(t *testing.T) {
	t.Parallel()

	require.Equal(t, []string{"Wi-Fi", "USB 10/100/1000 LAN"}, client.ParseNetworkServices([]byte(listServicesOutput)))
}

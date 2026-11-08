//go:build linux

package hysteria2_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/hysteria2"
)

// fakeFirewall records every action command and answers the detection/ownership probes from a
// script, so the test drives backend selection and the "already open" check without a real firewall.
type fakeFirewall struct {
	calls []string
	// detect maps a probe to its stdout. Known probes: "firewall-cmd --state",
	// "firewall-cmd --query-port=<p>/udp", "ufw status". A probe absent from the map errors, as a
	// not-running firewall / closed port would.
	detect map[string]string
	// failAdd makes the add action (firewall-cmd --add-port / ufw allow) fail, so a test can assert
	// openInboundUDP surfaces the error.
	failAdd bool
}

func (f *fakeFirewall) run(_ context.Context, name string, args ...string) ([]byte, error) {
	line := name
	for _, a := range args {
		line += " " + a
	}

	switch {
	case name == "firewall-cmd" && len(args) == 1 && args[0] == "--state":
		if out, ok := f.detect["firewall-cmd --state"]; ok {
			return []byte(out), nil
		}

		return nil, errors.New("not running")
	case name == "firewall-cmd" && len(args) == 1 && strings.HasPrefix(args[0], "--query-port="):
		if out, ok := f.detect[line]; ok {
			return []byte(out), nil
		}

		return nil, errors.New("not open") // --query-port exits non-zero when the port is not open
	case name == "ufw" && len(args) == 1 && args[0] == "status":
		if out, ok := f.detect["ufw status"]; ok {
			return []byte(out), nil
		}

		return nil, errors.New("inactive")
	}

	if f.failAdd &&
		((name == "firewall-cmd" && len(args) >= 1 && strings.HasPrefix(args[0], "--add-port=")) ||
			(name == "ufw" && len(args) >= 2 && args[0] == "allow")) {
		f.calls = append(f.calls, line)

		return []byte("ERROR: access denied"), errors.New("add failed")
	}

	f.calls = append(f.calls, line)

	return nil, nil
}

// A failing firewall add must be surfaced by openInboundUDP (the server logs it and serves anyway,
// but the error must not be swallowed). Pins that serve-anyway does not hide a real failure.
func TestOpenInboundUDP_SurfacesCommandError(t *testing.T) {
	t.Parallel()

	fw := &fakeFirewall{detect: map[string]string{"firewall-cmd --state": "running\n"}, failAdd: true}

	backend, err := hysteria2.OpenInboundUDPForTest(context.Background(), fw.run, 51822, zerolog.Nop())
	require.Error(t, err)
	require.Equal(t, "", backend)
}

func TestOpenCloseInboundUDP(t *testing.T) {
	t.Parallel()

	const port uint16 = 51822

	cases := []struct {
		name        string
		detect      map[string]string
		wantBackend string
		wantOpen    string
		wantClose   string
	}{
		{
			name:        "firewalld opens when not already open",
			detect:      map[string]string{"firewall-cmd --state": "running\n"},
			wantBackend: "firewalld",
			wantOpen:    "firewall-cmd --add-port=51822/udp",
			wantClose:   "firewall-cmd --remove-port=51822/udp",
		},
		{
			// Already open (operator's rule, or permanent/in a zone): own nothing, so Stop touches nothing.
			name:        "firewalld leaves an already-open port",
			detect:      map[string]string{"firewall-cmd --state": "running\n", "firewall-cmd --query-port=51822/udp": "yes\n"},
			wantBackend: "",
			wantOpen:    "",
			wantClose:   "",
		},
		{
			name:        "ufw opens when not already open",
			detect:      map[string]string{"ufw status": "Status: active\n"},
			wantBackend: "ufw",
			wantOpen:    "ufw allow 51822/udp",
			wantClose:   "ufw delete allow 51822/udp",
		},
		{
			name:        "ufw leaves an already-open port",
			detect:      map[string]string{"ufw status": "Status: active\n51822/udp                   ALLOW       Anywhere\n"},
			wantBackend: "",
			wantOpen:    "",
			wantClose:   "",
		},
		{
			name:        "no managed firewall",
			detect:      map[string]string{},
			wantBackend: "",
			wantOpen:    "",
			wantClose:   "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fw := &fakeFirewall{detect: tc.detect}

			backend, err := hysteria2.OpenInboundUDPForTest(context.Background(), fw.run, port, zerolog.Nop())
			require.NoError(t, err)
			require.Equal(t, tc.wantBackend, backend)

			if tc.wantOpen == "" {
				require.Empty(t, fw.calls, "no opening to make: must issue no add command")
			} else {
				require.Contains(t, fw.calls, tc.wantOpen)
			}

			fw.calls = nil

			require.NoError(t, hysteria2.CloseInboundUDPForTest(context.Background(), fw.run, backend, port, zerolog.Nop()))

			if tc.wantClose == "" {
				require.Empty(t, fw.calls, "nothing opened by us: must issue no close command")
			} else {
				require.Contains(t, fw.calls, tc.wantClose)
			}
		})
	}
}

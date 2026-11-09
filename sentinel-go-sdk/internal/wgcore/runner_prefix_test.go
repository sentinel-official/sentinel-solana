package wgcore_test

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/internal/wgcore"
	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

func TestQuickRunner_TunnelPrefix(t *testing.T) {
	t.Parallel()

	if runtime.GOOS != "linux" {
		t.Skip("the live interface is the config name on linux only")
	}

	var commands []service.Command

	exec := func(_ context.Context, c service.Command) ([]byte, error) {
		commands = append(commands, c)

		return nil, nil
	}

	v := plainVariantFixture()
	v.TunnelBinary = "/opt/homebrew/bin/bash"
	v.TunnelPrefix = []string{"/tools/dvpn-c0.awg-quick.bash"}
	r := wgcore.NewQuickRunnerForTest(v, "linux", exec)
	conf := "/var/lib/sentinel-dvpn/c0/amneziawg/dvpn-c0.conf"

	_, err := r.Up(t.Context(), conf)
	require.NoError(t, err)
	require.NoError(t, r.Down(t.Context(), conf))
	require.Equal(t, []string{"/tools/dvpn-c0.awg-quick.bash", "up", conf}, commands[0].Args, "the script runs under the interpreter")
	require.Equal(t, []string{"/tools/dvpn-c0.awg-quick.bash", "down", conf}, commands[1].Args)
	require.Equal(t, "/opt/homebrew/bin/bash", commands[0].Path)
	require.Equal(t, []string{"/tools/dvpn-c0.awg-quick.bash"}, v.TunnelPrefix, "the variant's prefix is not modified")
}

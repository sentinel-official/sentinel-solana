package instance_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/daemon/instance"
)

func TestList(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	for _, name := range []string{"c0", "n1", "x9", "c01"} {
		require.NoError(t, os.Mkdir(filepath.Join(root, name), 0o750))
	}

	require.NoError(t, os.WriteFile(filepath.Join(root, "p0"), nil, 0o600))

	tags, err := instance.List(root)
	require.NoError(t, err)
	require.Equal(t, []instance.Tag{"c0", "n1"}, tags)

	tags, err = instance.List(filepath.Join(root, "missing"))
	require.NoError(t, err)
	require.Empty(t, tags)
}

func TestNewNotFoundError(t *testing.T) {
	t.Parallel()

	err := instance.NewNotFoundError("c0", []instance.Tag{"n0", "n1"})
	require.ErrorIs(t, err, instance.ErrInstanceNotFound)
	require.EqualError(t, err, "instance c0 not found; available: n0, n1")

	require.EqualError(t, instance.NewNotFoundError("c0", nil), "instance c0 not found; available: none")
}

func TestParse(t *testing.T) {
	t.Parallel()

	cases := []struct {
		err   error
		input string
		name  string
	}{
		{input: "c0", name: "client"},
		{input: "n12", name: "node"},
		{input: "p999999999", name: "provider at maximum width"},
		{err: instance.ErrInvalidTag, input: "", name: "empty"},
		{err: instance.ErrInvalidTag, input: "c", name: "missing number"},
		{err: instance.ErrInvalidTag, input: "c01", name: "leading zero"},
		{err: instance.ErrInvalidTag, input: "c1000000000", name: "too many digits"},
		{err: instance.ErrInvalidTag, input: "x0", name: "unknown role"},
		{err: instance.ErrInvalidTag, input: "C0", name: "uppercase role"},
		{err: instance.ErrInvalidTag, input: " c0", name: "leading space"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tag, err := instance.Parse(tc.input)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				require.ErrorContains(t, err, tc.input)

				return
			}

			require.NoError(t, err)
			require.Equal(t, instance.Tag(tc.input), tag)
		})
	}
}

func TestStateRoot(t *testing.T) {
	tag := instance.Tag("c0")
	cases := []struct {
		endpoint    string
		goos        string
		logPath     string
		name        string
		programData string
		root        string
		stateDir    string
	}{
		{
			endpoint: "/run/sentinel-dvpn/c0/daemon.sock",
			goos:     "linux",
			name:     "linux",
			root:     "/var/lib/sentinel-dvpn",
			stateDir: "/var/lib/sentinel-dvpn/c0",
		},
		{
			endpoint: "/Library/Application Support/sentinel-dvpn/c0/daemon.sock",
			goos:     "darwin",
			logPath:  "/Library/Logs/sentinel-dvpn/c0.log",
			name:     "darwin",
			root:     "/Library/Application Support/sentinel-dvpn",
			stateDir: "/Library/Application Support/sentinel-dvpn/c0",
		},
		{
			endpoint:    `\\.\pipe\ProtectedPrefix\Administrators\sentinel-dvpn\c0`,
			goos:        "windows",
			logPath:     `D:\Data\sentinel-dvpn\c0\daemon.log`,
			name:        "windows",
			programData: `D:\Data`,
			root:        `D:\Data\sentinel-dvpn`,
			stateDir:    `D:\Data\sentinel-dvpn\c0`,
		},
		{
			endpoint: `\\.\pipe\ProtectedPrefix\Administrators\sentinel-dvpn\c0`,
			goos:     "windows",
			logPath:  `C:\ProgramData\sentinel-dvpn\c0\daemon.log`,
			name:     "windows without program data",
			root:     `C:\ProgramData\sentinel-dvpn`,
			stateDir: `C:\ProgramData\sentinel-dvpn\c0`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ProgramData", tc.programData)

			require.Equal(t, tc.root, instance.StateRoot(tc.goos))
			require.Equal(t, tc.stateDir, tag.StateDir(tc.goos))
			require.Equal(t, tc.endpoint, tag.Endpoint(tc.goos))
			require.Equal(t, tc.logPath, tag.LogPath(tc.goos))
		})
	}

	require.Equal(t, "/etc/sentinel-dvpn/c0.env", tag.EnvPath())
}

func TestTag(t *testing.T) {
	t.Parallel()

	tag := instance.Tag("n0")

	require.Equal(t, "node", tag.Role())
	require.Equal(t, "client", instance.Tag("c0").Role())
	require.Equal(t, "provider", instance.Tag("p1").Role())
	require.Empty(t, instance.Tag("").Role())
	require.Empty(t, instance.Tag("x0").Role())

	require.Equal(t, "dvpn-n0", tag.Interface())
	require.Equal(t, "sentinel-dvpn-n0", tag.ObjectName())
	require.Equal(t, "sentinel-dvpnd@n0", tag.ServiceName("linux"))
	require.Equal(t, "sentinel-dvpnd@n0", tag.ServiceName("freebsd"))
	require.Equal(t, "co.sentinel.sentinel-dvpnd.n0", tag.ServiceName("darwin"))
	require.Equal(t, "sentinel-dvpnd$n0", tag.ServiceName("windows"))
	require.Len(t, instance.Tag("p999999999").Interface(), 15)
}

// The Linux socket is under /run (var_run_t under SELinux), never in the var_lib_t state folder,
// where systemd's socket unit was refused (Fedora 44, Rocky 9, enforcing).
func TestEndpoint_LinuxUnderRun(t *testing.T) {
	t.Parallel()

	require.Equal(t, "/run/sentinel-dvpn/c0/daemon.sock", instance.Tag("c0").Endpoint("linux"))
	require.Equal(t, "/Library/Application Support/sentinel-dvpn/c0/daemon.sock", instance.Tag("c0").Endpoint("darwin"))
}

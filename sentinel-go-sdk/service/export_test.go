package service

import (
	"context"
	"time"
)

type KillOwnedImageFunc = func(context.Context, int, func(string, time.Time) bool) error

type ReadProcessArgsFunc = func(context.Context, int) (string, []string, error)

func EnvironWithout(env, names []string, goos string) []string {
	return environWithout(env, names, goos)
}

func KillPIDFileFor(ctx context.Context, path, binary, configFlag, conf, goos string) error {
	identity := processIdentity{
		binary:     binary,
		conf:       conf,
		configFlag: configFlag,
	}

	return killPIDFile(ctx, path, identity, goos)
}

func KnownBinaryDirs(goos string) []string { return knownBinaryDirs(goos) }

func OwnsImage(image, binary string, started, written time.Time) bool {
	return ownsImage(image, binary, started, written)
}

func ParseNetstat(out []byte, iface string) (PeerStats, error) { return parseNetstat(out, iface) }

func ProbeKernelSupport(ctx context.Context, kind, goos string, exec execFunc) (bool, error) {
	return probeKernelSupport(ctx, kind, goos, exec)
}

func SelectNewInterface(before, after map[string]bool, goos string) (string, error) {
	return selectNewInterface(before, after, goos)
}

func SetKillOwnedImage(kill KillOwnedImageFunc) KillOwnedImageFunc {
	previous := killOwnedImage
	killOwnedImage = kill

	return previous
}

func SetReadProcessArgs(read ReadProcessArgsFunc) ReadProcessArgsFunc {
	previous := readProcessArgs
	readProcessArgs = read

	return previous
}

func WritePIDRecord(path string, pid int, executable string) error {
	return writePIDRecord(path, pid, executable)
}

// DeregisterForTest undoes a RegisterClient/RegisterServer so a test can run under -count>1
// without the package-global registry panicking on the re-registration. Tests only; there is
// no production deregister.
func DeregisterForTest(name Name) {
	mu.Lock()
	defer mu.Unlock()

	delete(clients, name)
	delete(servers, name)
	delete(protocolBits, name)
}

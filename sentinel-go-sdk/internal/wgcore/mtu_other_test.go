//go:build !linux

package wgcore

// On non-Linux the server test file (mtu_linux_test.go) is not built, so the client-fit test's
// shared helpers live here: the uplink stub (tests never do a real lookup) and the
// extension-overhead stand-in.
func init() { uplinkMTU = func(string) int { return 0 } }

type padded struct{ s4 int }

func (p padded) TransportOverhead() int { return p.s4 }

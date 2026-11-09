//go:build !linux

package hysteria2

// localBypassV4: Linux only for now; other systems keep their current routes.
func localBypassV4(string) []string { return nil }

// localBypassV6: Linux only for now.
func localBypassV6(string) []string { return nil }

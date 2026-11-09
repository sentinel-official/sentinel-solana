//go:build !linux

package hysteria2

import "context"

// removeRouteRules: sing-tun's policy rules exist only on Linux.
func removeRouteRules(context.Context, string) error { return nil }

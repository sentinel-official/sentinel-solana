//go:build linux

package hysteria2

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The exact rules a killed hysteria2 left on a Debian 12 client, among rules that are not ours.
const leftRulesV4 = `0:	from all lookup local
8990:	from 10.0.0.4 lookup 51820
9000:	from all to 100.100.100.100/30 lookup 25687943
9002:	from all iif dvpn-hy2-client [detached] goto 9010
9003:	not from all iif lo lookup 25687943
9003:	from 0.0.0.0 iif lo lookup 25687943
9003:	from 100.100.100.100/30 iif lo lookup 25687943
9010:	from all nop
32764:	from all lookup main suppress_prefixlength 0
32766:	from all lookup main
32767:	from all lookup default
`

func TestRemoveRouteRules_DeletesOnlySingTunsLeftovers(t *testing.T) { //nolint:paralleltest // swaps ipRule
	var deleted []string

	orig := ipRule
	t.Cleanup(func() { ipRule = orig })

	ipRule = func(_ context.Context, args ...string) ([]byte, error) {
		if args[1] == "rule" && args[2] == "show" {
			if args[0] == "-4" {
				return []byte(leftRulesV4), nil
			}

			return []byte("0:\tfrom all lookup local\n32766:\tfrom all lookup main\n"), nil
		}

		deleted = append(deleted, strings.Join(args, " "))

		return nil, nil
	}

	require.NoError(t, removeRouteRules(t.Context(), "dvpn-hy2-client"))
	require.Equal(t, []string{
		"-4 rule del pref 9000 from all to 100.100.100.100/30 lookup 25687943",
		"-4 rule del pref 9002 from all iif dvpn-hy2-client goto 9010",
		"-4 rule del pref 9003 not from all iif lo lookup 25687943",
		"-4 rule del pref 9003 from 0.0.0.0 iif lo lookup 25687943",
		"-4 rule del pref 9003 from 100.100.100.100/30 iif lo lookup 25687943",
		"-4 rule del pref 9010 from all nop",
	}, deleted, "every sing-tun rule of this tunnel, nothing else ([detached] dropped)")
}

func TestRemoveRouteRules_NothingOfOursLeftDeletesNothing(t *testing.T) { //nolint:paralleltest // swaps ipRule
	orig := ipRule
	t.Cleanup(func() { ipRule = orig })

	ipRule = func(_ context.Context, args ...string) ([]byte, error) {
		require.Equal(t, "show", args[2], "no rule of this tunnel: nothing deleted")

		return []byte("0:\tfrom all lookup local\n9010:\tfrom all nop\n32766:\tfrom all lookup main\n"), nil
	}

	require.NoError(t, removeRouteRules(t.Context(), "dvpn-hy2-client"))
}

// The IPv6 rules a killed hysteria2 left (Ubuntu 24.04 client). The table
// is named only by "from <tun prefix>", and the goto rules use iif lo.
const leftRulesV6 = `0:	from all lookup local
9001:	from 2001::ffff:ffff:ffff:fff0/126 iif lo lookup 1818566472
9002:	from ::/1 iif lo goto 9010 [unresolved]
9002:	from 8000::/1 iif lo goto 9010 [unresolved]
9003:	from all lookup 1818566472
20000:	from all iif lo goto 32766
32766:	from all lookup main
`

func TestRemoveRouteRules_IPv6LeftoversToo(t *testing.T) { //nolint:paralleltest // swaps ipRule
	var deleted []string

	orig := ipRule
	t.Cleanup(func() { ipRule = orig })

	ipRule = func(_ context.Context, args ...string) ([]byte, error) {
		if args[2] == "show" {
			if args[0] == "-6" {
				return []byte(leftRulesV6), nil
			}

			return []byte("0:\tfrom all lookup local\n"), nil
		}

		deleted = append(deleted, strings.Join(args, " "))

		return nil, nil
	}

	require.NoError(t, removeRouteRules(t.Context(), "dvpn-hy2-client"))
	require.Equal(t, []string{
		"-6 rule del pref 9001 from 2001::ffff:ffff:ffff:fff0/126 iif lo lookup 1818566472",
		"-6 rule del pref 9002 from ::/1 iif lo goto 9010",
		"-6 rule del pref 9002 from 8000::/1 iif lo goto 9010",
		"-6 rule del pref 9003 from all lookup 1818566472",
	}, deleted, "all four sing-tun IPv6 rules; a foreign goto outside its range stays")
}

// After a daemon SIGKILL during a session, sing-tun's DNS bypass rule (IPv4 at 9002, IPv6 at 9001) was left
// too; it names neither the table nor the tun. The same rule outside sing-tun's range belongs to someone else.
func TestRemoveRouteRules_DNSBypassRuleToo(t *testing.T) { //nolint:paralleltest // swaps ipRule
	const bypass = "not from all dport 53 lookup main suppress_prefixlength 0"

	v4 := strings.Replace(leftRulesV4, "9003:\tnot from all iif lo", "9002:\t"+bypass+"\n9003:\tnot from all iif lo", 1)
	v4 = strings.Replace(v4, "32764:", "31000:\t"+bypass+"\n32764:", 1)
	v6 := strings.Replace(leftRulesV6, "9002:\tfrom ::/1", "9001:\t"+bypass+"\n9002:\tfrom ::/1", 1)

	var deleted []string

	orig := ipRule
	t.Cleanup(func() { ipRule = orig })

	ipRule = func(_ context.Context, args ...string) ([]byte, error) {
		if args[2] == "show" {
			if args[0] == "-4" {
				return []byte(v4), nil
			}

			return []byte(v6), nil
		}

		deleted = append(deleted, strings.Join(args, " "))

		return nil, nil
	}

	require.NoError(t, removeRouteRules(t.Context(), "dvpn-hy2-client"))
	require.Contains(t, deleted, "-4 rule del pref 9002 "+bypass)
	require.Contains(t, deleted, "-6 rule del pref 9001 "+bypass)
	require.NotContains(t, deleted, "-4 rule del pref 31000 "+bypass, "a bypass rule outside sing-tun's range stays")
	require.Len(t, deleted, 6+1+4+1)
}

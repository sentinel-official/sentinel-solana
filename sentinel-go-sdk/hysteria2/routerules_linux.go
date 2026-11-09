//go:build linux

package hysteria2

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os/exec"
	"slices"
	"strconv"
	"strings"
)

// sing-tun (hysteria2's tun, auto route) adds policy rules at pref 9000-9010 for its own table and
// removes them when hysteria2 exits cleanly. A killed hysteria2 leaves them (seen after
// cleanup: "9000: to 100.100.100.100/30 lookup T", "9002: iif dvpn-hy2-client goto 9010", three
// "9003: ... lookup T", "9010: nop"). The table is found from the tun prefix client.yaml.tmpl gives
// sing-tun, so only rules of this tunnel are touched.
var tunRulePrefixes = map[string]string{
	"-4": "100.100.100.100/30",
	"-6": "2001::ffff:ffff:ffff:fff0/126",
}

var ipRule = func(ctx context.Context, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, "ip", args...).CombinedOutput() //#nosec G204 -- fixed binary, parsed args
}

// dnsBypassSelector is the rule sing-tun adds next to its table rules so that only DNS is forced into the
// tun: every other packet may use a main-table route more specific than the default. It names neither the
// table nor the tun, so it is matched by its exact selector inside the pref range of this tunnel's rules.
var dnsBypassSelector = []string{"not", "from", "all", "dport", "53", "lookup", "main", "suppress_prefixlength", "0"}

type ipRuleLine struct {
	pref     string
	selector []string
}

func parseIPRules(out []byte) []ipRuleLine {
	var rules []ipRuleLine

	for _, line := range strings.Split(string(out), "\n") {
		pref, rest, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok {
			continue
		}

		if _, err := strconv.Atoi(pref); err != nil {
			continue
		}

		fields := slices.DeleteFunc(strings.Fields(rest), func(f string) bool {
			return f == "[detached]" || f == "[unresolved]"
		})
		rules = append(rules, ipRuleLine{pref: pref, selector: fields})
	}

	return rules
}

func ruleValue(sel []string, key string) string {
	if i := slices.Index(sel, key); i >= 0 && i+1 < len(sel) {
		return sel[i+1]
	}

	return ""
}

// staleTunRules picks this tunnel's sing-tun rules: everything that looks up its table, the rules for
// its interface, the goto rules sing-tun puts between its table rules (IPv6: "from ::/1 iif lo goto
// 9010"), and the nop those jump to.
func staleTunRules(rules []ipRuleLine, prefix, iface string) []ipRuleLine {
	table := ""

	for _, r := range rules {
		// IPv4 names the prefix as "to", IPv6 only as "from ... iif lo".
		if (ruleValue(r.selector, "to") == prefix || ruleValue(r.selector, "from") == prefix) && ruleValue(r.selector, "lookup") != "" {
			table = ruleValue(r.selector, "lookup")

			break
		}
	}

	low, high := math.MaxInt, -1

	for _, r := range rules {
		if table != "" && ruleValue(r.selector, "lookup") == table {
			p, _ := strconv.Atoi(r.pref)
			low, high = min(low, p), max(high, p)
		}
	}

	var (
		stale   []ipRuleLine
		targets []string
	)

	for _, r := range rules {
		p, _ := strconv.Atoi(r.pref)
		goTo := ruleValue(r.selector, "goto")

		switch {
		case table != "" && ruleValue(r.selector, "lookup") == table:
			stale = append(stale, r)
		case table != "" && p >= low && p <= high && slices.Equal(r.selector, dnsBypassSelector):
			stale = append(stale, r)
		case iface != "" && ruleValue(r.selector, "iif") == iface, goTo != "" && p >= low && p <= high:
			stale = append(stale, r)

			if goTo != "" {
				targets = append(targets, goTo)
			}
		}
	}

	for _, r := range rules {
		if slices.Contains(targets, r.pref) && slices.Equal(r.selector, []string{"from", "all", "nop"}) {
			stale = append(stale, r)
		}
	}

	return stale
}

// removeRouteRules deletes sing-tun's rules a killed hysteria2 left. Run only once hysteria2 is gone.
func removeRouteRules(ctx context.Context, iface string) error {
	var errs []error

	for _, family := range []string{"-4", "-6"} {
		out, err := ipRule(ctx, family, "rule", "show")
		if err != nil {
			errs = append(errs, fmt.Errorf("listing ip %s rules: %w: %s", family, err, strings.TrimSpace(string(out))))

			continue
		}

		for _, r := range staleTunRules(parseIPRules(out), tunRulePrefixes[family], iface) {
			args := append([]string{family, "rule", "del", "pref", r.pref}, r.selector...)
			if out, err := ipRule(ctx, args...); err != nil {
				errs = append(errs, fmt.Errorf("ip %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out))))
			}
		}
	}

	return errors.Join(errs...)
}

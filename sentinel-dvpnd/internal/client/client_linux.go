package client

import (
	"bytes"
	"context"
	"fmt"
	"net/netip"
	"os/exec"
	"strings"
	"text/template"
)

const nftRulesetSource = `table inet {{ .Table }}
delete table inet {{ .Table }}
table inet {{ .Table }} {
  chain output {
    type filter hook output priority 0; policy drop;
    oif "lo" accept
    {{ if .Iface }}oifname "{{ .Iface }}" accept
    {{ end }}{{ range .NodeAddrs }}{{ addrFamily . }} daddr {{ . }} accept
    {{ end }}{{ range .RPCAndWSAddrs }}{{ addrPortFamily . }} daddr {{ .Addr }} tcp dport {{ .Port }} accept
    {{ end }}udp dport { 67, 68, 546, 547 } accept
    ip daddr 169.254.0.0/16 accept
    ip6 daddr fe80::/10 accept
    {{ if .AllowLAN }}ip daddr { 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16 } accept
    ip6 daddr fc00::/7 accept
    {{ end }}}
}
`

//nolint:gochecknoglobals
var nftRulesetTemplate = template.Must(template.New("nft").Funcs(template.FuncMap{
	"addrFamily":     addrFamily,
	"addrPortFamily": addrPortFamily,
}).Parse(nftRulesetSource))

type nftFirewall struct {
	table string
}

func (f nftFirewall) Apply(
	ctx context.Context,
	iface string,
	nodeAddrs []netip.Addr,
	rpcAndWSAddrs []netip.AddrPort,
	allowLAN bool,
) error {
	rules := RenderNftRuleset(f.table, iface, nodeAddrs, rpcAndWSAddrs, allowLAN)

	cmd := exec.CommandContext(ctx, "nft", "-f", "-")
	cmd.Stdin = strings.NewReader(rules)

	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("applying firewall: %w: %s", err, output)
	}

	return nil
}

func (f nftFirewall) Flush(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "nft", "delete", "table", "inet", f.table)

	if output, err := cmd.CombinedOutput(); err != nil && !strings.Contains(string(output), "No such file") {
		return fmt.Errorf("flushing firewall: %w: %s", err, output)
	}

	return nil
}

func KillSwitchLoaded(ctx context.Context, name string) (bool, error) {
	output, err := exec.CommandContext(ctx, "nft", "list", "table", "inet", name).CombinedOutput()

	return parseLoaded(output, err)
}

func RenderNftRuleset(
	table, iface string,
	nodeAddrs []netip.Addr,
	rpcAndWSAddrs []netip.AddrPort,
	allowLAN bool,
) string {
	data := struct {
		AllowLAN      bool
		Iface         string
		NodeAddrs     []netip.Addr
		RPCAndWSAddrs []netip.AddrPort
		Table         string
	}{allowLAN, iface, nodeAddrs, rpcAndWSAddrs, table}

	var b bytes.Buffer

	_ = nftRulesetTemplate.Execute(&b, data)

	return b.String()
}

func addrFamily(a netip.Addr) string {
	if a.Is6() {
		return "ip6"
	}

	return "ip"
}

func addrPortFamily(ap netip.AddrPort) string {
	return addrFamily(ap.Addr())
}

func newFirewall(_, name string) firewallBackend { return nftFirewall{table: name} }

func parseLoaded(output []byte, err error) (bool, error) {
	switch {
	case err == nil:
		return true, nil
	case strings.Contains(string(output), "No such file"):
		return false, nil
	default:
		detail := strings.TrimSpace(string(output))
		if detail == "" {
			return false, fmt.Errorf("inspecting firewall: %w", err)
		}

		return false, fmt.Errorf("inspecting firewall: %w: %s", err, detail)
	}
}

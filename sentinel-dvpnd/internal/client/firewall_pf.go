package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

const (
	pfBinary         = "pfctl"
	pfCleanupTimeout = 15 * time.Second
	pfRulesetSource  = `block drop out all
pass out quick on lo0 all
{{ if .Iface }}pass out quick on {{ .Iface }} all
{{ end }}{{ range .NodeAddrs }}pass out quick to {{ . }}
{{ end }}{{ range .RPCAndWSAddrs }}pass out quick proto tcp to {{ .Addr }} port {{ .Port }}
{{ end }}pass out quick proto udp to any port { 67 68 546 547 }
pass out quick to 169.254.0.0/16
pass out quick to fe80::/10
{{ if .AllowLAN }}pass out quick to { 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 }
{{ end }}`
	pfTokenFileName = "pf.token" //#nosec G101 -- A file name, not a credential.
)

//nolint:gochecknoglobals
var (
	errInvalidPFToken         = errors.New("invalid or missing pf enable token")
	errPFAnchorConflict       = errors.New("conflicting pf anchor attachment")
	errUnexpectedPFOutput     = errors.New("unrecognized pf inspection output")
	errUnsupportedPFInterface = errors.New("pf skips a non-loopback interface")
	pfRulesetTemplate         = template.Must(template.New("pf").Parse(pfRulesetSource))
)

type pfFirewall struct {
	anchor    string
	run       runFunc
	tokenPath string
}

func (f pfFirewall) Apply(
	ctx context.Context,
	iface string,
	nodeAddrs []netip.Addr,
	rpcAndWSAddrs []netip.AddrPort,
	allowLAN bool,
) (err error) {
	if err := checkPFInterfaces(ctx, f.run); err != nil {
		return err
	}

	if err := f.attachAnchor(ctx); err != nil {
		return err
	}

	defer func() {
		if err != nil {
			err = errors.Join(errFirewallChanged, err)
		}
	}()

	rules := RenderPFRuleset(iface, nodeAddrs, rpcAndWSAddrs, allowLAN)
	if out, err := f.run(ctx, rules, pfBinary, "-a", f.anchor, "-f", "-"); err != nil {
		return fmt.Errorf("loading pf rules: %w: %s", err, out)
	}

	if err := f.enable(ctx); err != nil {
		return err
	}

	if out, err := f.run(ctx, "", pfBinary, "-F", "states"); err != nil {
		return fmt.Errorf("flushing pf states: %w: %s", err, out)
	}

	return nil
}

func (f pfFirewall) Flush(ctx context.Context) error {
	if out, err := f.run(ctx, "", pfBinary, "-a", f.anchor, "-F", "all"); err != nil {
		return fmt.Errorf("flushing pf anchor: %w: %s", err, out)
	}

	return f.disable(ctx)
}

func (f pfFirewall) attachAnchor(ctx context.Context) error {
	out, err := f.run(ctx, "", pfBinary, "-sr")
	if err != nil {
		return fmt.Errorf("reading pf rules: %w: %s", err, out)
	}

	if pfAnchorPresent(out, f.anchor) {
		return nil
	}

	lines := strings.SplitAfter(string(out), "\n")

	position := len(lines)
	for i, line := range lines {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}

		if len(fields) > 1 && fields[0] == "anchor" && fields[1] == strconv.Quote(f.anchor) {
			return errPFAnchorConflict
		}

		if position == len(lines) && (fields[0] == "anchor" || fields[0] == "pass" || fields[0] == "block") {
			position = i
		}
	}

	prefix := strings.Join(lines[:position], "")
	if prefix != "" && !strings.HasSuffix(prefix, "\n") {
		prefix += "\n"
	}

	rules := prefix + pfAnchorRule(f.anchor) + "\n" + strings.Join(lines[position:], "")
	if out, err := f.run(ctx, rules, pfBinary, "-R", "-f", "-"); err != nil {
		return fmt.Errorf("attaching pf anchor: %w: %s", err, out)
	}

	return nil
}

func (f pfFirewall) disable(ctx context.Context) error {
	token, err := f.readToken()
	if err != nil {
		return err
	}

	if token == "" {
		return nil
	}

	live, err := pfTokenLive(ctx, f.run, token)
	if err != nil {
		return err
	}

	if live {
		if err := f.release(ctx, token); err != nil {
			return err
		}
	}

	if err := os.Remove(f.tokenPath); err != nil {
		return fmt.Errorf("removing pf token: %w", err)
	}

	return nil
}

func (f pfFirewall) enable(ctx context.Context) error {
	token, err := f.readToken()
	if err != nil {
		return err
	}

	if token != "" {
		live, err := pfTokenLive(ctx, f.run, token)
		if err != nil {
			return err
		}

		if live {
			return nil
		}
	}

	out, err := f.run(ctx, "", pfBinary, "-E")
	if err != nil {
		return fmt.Errorf("enabling pf: %w: %s", err, out)
	}

	token, err = parsePFToken(out)
	if err != nil {
		return err
	}

	if err := f.saveToken(token); err != nil {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), pfCleanupTimeout)
		defer cancel()

		return errors.Join(fmt.Errorf("saving pf token: %w", err), f.release(cleanup, token))
	}

	return nil
}

func (f pfFirewall) readToken() (string, error) {
	info, err := os.Lstat(f.tokenPath)
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}

	if err != nil {
		return "", fmt.Errorf("inspecting pf token: %w", err)
	}

	if !info.Mode().IsRegular() {
		return "", errInvalidPFToken
	}

	file, err := os.Open(f.tokenPath)
	if err != nil {
		return "", fmt.Errorf("opening pf token: %w", err)
	}

	current, statErr := file.Stat()
	if statErr != nil || !os.SameFile(info, current) {
		return "", errors.Join(errInvalidPFToken, statErr, file.Close())
	}

	raw, readErr := io.ReadAll(file)
	if err := errors.Join(readErr, file.Close()); err != nil {
		return "", fmt.Errorf("reading pf token: %w", err)
	}

	token := strings.TrimSpace(string(raw))
	if !validPFToken(token) {
		return "", errInvalidPFToken
	}

	return token, nil
}

func (f pfFirewall) release(ctx context.Context, token string) error {
	if out, err := f.run(ctx, "", pfBinary, "-X", token); err != nil {
		return fmt.Errorf("releasing pf token %s: %w: %s", token, err, out)
	}

	return nil
}

func (f pfFirewall) saveToken(token string) (err error) {
	file, err := os.CreateTemp(filepath.Dir(f.tokenPath), ".pf.token-*")
	if err != nil {
		return fmt.Errorf("creating pf token: %w", err)
	}

	defer func() {
		if removeErr := os.Remove(file.Name()); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("removing temporary pf token: %w", removeErr))
		}
	}()

	_, writeErr := file.WriteString(token)
	syncErr := file.Sync()
	closeErr := file.Close()

	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return fmt.Errorf("writing pf token: %w", err)
	}

	info, err := os.Lstat(f.tokenPath)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("inspecting pf token: %w", err)
	}

	if err == nil && !info.Mode().IsRegular() {
		return errInvalidPFToken
	}

	if err := os.Rename(file.Name(), f.tokenPath); err != nil {
		return fmt.Errorf("publishing pf token: %w", err)
	}

	return nil
}

func RenderPFRuleset(iface string, nodeAddrs []netip.Addr, rpcAndWSAddrs []netip.AddrPort, allowLAN bool) string {
	data := struct {
		AllowLAN      bool
		Iface         string
		NodeAddrs     []netip.Addr
		RPCAndWSAddrs []netip.AddrPort
	}{allowLAN, iface, nodeAddrs, rpcAndWSAddrs}

	var b bytes.Buffer

	_ = pfRulesetTemplate.Execute(&b, data)

	return b.String()
}

func checkPFInterfaces(ctx context.Context, run runFunc) error {
	out, err := run(ctx, "", pfBinary, "-v", "-s", "Interfaces")
	if err != nil {
		return fmt.Errorf("inspecting pf interfaces: %w: %s", err, out)
	}

	lines := pfInspectionLines(out)
	if len(lines) == 0 {
		return errUnexpectedPFOutput
	}

	seen := make(map[string]bool)

	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 1 || len(fields) > 2 || !validPFInterface(fields[0]) {
			return errUnexpectedPFOutput
		}

		if seen[fields[0]] {
			return errUnexpectedPFOutput
		}

		seen[fields[0]] = true
		if len(fields) == 2 {
			if fields[1] != "(skip)" {
				return errUnexpectedPFOutput
			}

			if fields[0] != "lo0" {
				return fmt.Errorf("%w: %s", errUnsupportedPFInterface, fields[0])
			}
		}
	}

	if !seen["lo0"] {
		return errUnexpectedPFOutput
	}

	return nil
}

func parsePFToken(out []byte) (string, error) {
	token := ""

	for line := range bytes.SplitSeq(out, []byte("\n")) {
		before, after, ok := strings.Cut(string(line), ":")
		if !ok || strings.TrimSpace(before) != "Token" {
			continue
		}

		if token != "" {
			return "", errInvalidPFToken
		}

		token = strings.TrimSpace(after)
		if !validPFToken(token) {
			return "", errInvalidPFToken
		}
	}

	if token == "" {
		return "", errInvalidPFToken
	}

	return token, nil
}

func pfAnchorPresent(out []byte, anchor string) bool {
	first := true
	present := false

	for line := range strings.SplitSeq(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}

		if fields[0] != "anchor" && fields[0] != "pass" && fields[0] != "block" {
			continue
		}

		if len(fields) > 1 && fields[0] == "anchor" && fields[1] == strconv.Quote(anchor) {
			if !first || present || strings.Join(fields, " ") != pfAnchorRule(anchor) {
				return false
			}

			present = true
		}

		first = false
	}

	return present
}

func pfAnchorRule(anchor string) string {
	return "anchor " + strconv.Quote(anchor) + " out quick all"
}

func pfInspectionLines(out []byte) []string {
	text := strings.TrimSpace(string(out))

	text = strings.TrimPrefix(text, "No ALTQ support in kernel\nALTQ related functions disabled\n")
	if text == "" {
		return nil
	}

	return strings.Split(text, "\n")
}

func pfLoaded(ctx context.Context, run runFunc, anchor string) (bool, error) {
	if err := checkPFInterfaces(ctx, run); err != nil {
		return false, err
	}

	info, err := run(ctx, "", pfBinary, "-s", "info")
	if err != nil {
		return false, fmt.Errorf("inspecting pf: %w: %s", err, info)
	}

	enabled := false

	for line := range strings.SplitSeq(string(info), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "Status:" && fields[1] == "Enabled" {
			enabled = true
		}
	}

	if !enabled {
		return false, nil
	}

	root, err := run(ctx, "", pfBinary, "-sr")
	if err != nil {
		return false, fmt.Errorf("inspecting pf attachment: %w: %s", err, root)
	}

	if !pfAnchorPresent(root, anchor) {
		return false, nil
	}

	rules, err := run(ctx, "", pfBinary, "-a", anchor, "-sr")
	if err != nil {
		return false, fmt.Errorf("inspecting the pf anchor: %w: %s", err, rules)
	}

	return strings.HasPrefix(string(rules), "block drop out all\n"), nil
}

func pfTokenLive(ctx context.Context, run runFunc, token string) (bool, error) {
	out, err := run(ctx, "", pfBinary, "-s", "References")
	if err != nil {
		return false, fmt.Errorf("inspecting pf references: %w: %s", err, out)
	}

	lines := pfInspectionLines(out)
	if len(lines) == 1 && lines[0] == "No pf starter references held" {
		return false, nil
	}

	if len(lines) < 3 || strings.TrimSpace(lines[0]) != "TOKENS:" ||
		strings.Join(strings.Fields(lines[1]), " ") != "PID Process Name TOKEN TIMESTAMP" {
		return false, errUnexpectedPFOutput
	}

	found := false

	for _, line := range lines[2:] {
		fields := strings.Fields(line)
		if len(fields) < 6 {
			return false, errUnexpectedPFOutput
		}

		n := len(fields)
		if !validPFToken(fields[0]) || !validPFToken(fields[n-4]) || fields[n-2] != "days" {
			return false, errUnexpectedPFOutput
		}

		if _, err := strconv.ParseUint(fields[n-3], 10, 64); err != nil {
			return false, fmt.Errorf("%w: %w", errUnexpectedPFOutput, err)
		}

		if _, err := time.Parse("15:04:05", fields[n-1]); err != nil {
			return false, fmt.Errorf("%w: %w", errUnexpectedPFOutput, err)
		}

		found = found || fields[n-4] == token
	}

	return found, nil
}

func runPFCommand(ctx context.Context, input, name string, args ...string) ([]byte, error) {
	path, err := service.LookPath(name)
	if err != nil {
		return nil, err
	}

	cmd := service.CommandContext(ctx, path, args...)
	cmd.Stdin = strings.NewReader(input)

	var diagnostics bytes.Buffer

	cmd.Stderr = &diagnostics

	out, err := cmd.Output()
	if err != nil {
		return out, fmt.Errorf("running %s: %w: %s", name, err, diagnostics.String())
	}

	if len(args) == 1 && args[0] == "-E" {
		out = append(out, '\n')
		out = append(out, diagnostics.Bytes()...)
	}

	return out, nil
}

func validPFInterface(name string) bool {
	if name == "" || len(name) > 15 {
		return false
	}

	for _, r := range name {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' && r != '-' && r != '.' {
			return false
		}
	}

	return true
}

func validPFToken(token string) bool {
	if token == "" || strings.Trim(token, "0123456789") != "" {
		return false
	}

	value, err := strconv.ParseUint(token, 10, 64)

	return err == nil && value != 0
}

package openvpn

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/netip"
	"os"
	"strings"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
)

// hostRoute tracks the server host route that openvpn adds on Linux for redirect-gateway
// ("<server> via <gateway> dev <nic>" in the main table). openvpn removes it on a clean exit only: after
// a SIGKILL, an OOM kill or a crash it stays, and later sessions to that server send its address around
// the tunnel. So the route openvpn added is recorded in an owned file, and Stop and Cleanup delete it
// when it is still exactly that route.
type hostRoute struct {
	file string
	goos string
	run  func(context.Context, service.Command) ([]byte, error)
}

func newHostRoute(file, goos string) hostRoute {
	return hostRoute{file: file, goos: goos, run: service.Run}
}

// lookup returns the main-table route for exactly the server's address, or "" when there is none. ok is
// false when route tracking does not apply (not Linux, or the host is not an IP address).
func (r hostRoute) lookup(ctx context.Context, host string) (route string, ok bool, err error) {
	if r.goos != "linux" {
		return "", false, nil
	}

	ip, err := netip.ParseAddr(host)
	if err != nil {
		return "", false, nil //nolint:nilerr // a host name: openvpn resolves it, nothing to track
	}

	family, bits := "-4", 32
	if ip.Is6() {
		family, bits = "-6", 128
	}

	out, err := r.run(ctx, service.Command{
		Args: []string{family, "-o", "route", "show", "exact", fmt.Sprintf("%s/%d", ip.Unmap(), bits), "table", "main"},
		Path: "ip",
	})
	if err != nil {
		return "", true, fmt.Errorf("reading the host route to %s: %w", host, err)
	}

	line, _, _ := strings.Cut(strings.TrimSpace(string(out)), "\n")

	return strings.TrimSpace(line), true, nil
}

// record saves the route openvpn added: one that exists now, did not exist before the start, and goes
// through a gateway.
func (r hostRoute) record(ctx context.Context, host, before string) error {
	after, ok, err := r.lookup(ctx, host)
	if err != nil || !ok {
		return err
	}

	if before != "" || after == "" || !strings.Contains(" "+after+" ", " via ") {
		return nil
	}

	return service.WriteOwned(r.file, []byte(after+"\n"), "#")
}

// clear deletes the recorded route when it is still exactly what openvpn added. A route that changed
// since (another owner) is left alone. The owned file itself goes with the other owned files.
func (r hostRoute) clear(ctx context.Context) error {
	recorded, err := r.recorded()
	if err != nil || recorded == "" {
		return err
	}

	host, _, _ := strings.Cut(recorded, " ")

	current, ok, err := r.lookup(ctx, host)
	if err != nil || !ok || current != recorded {
		return err
	}

	family := "-4"
	if strings.Contains(host, ":") {
		family = "-6"
	}

	args := append([]string{family, "route", "del"}, strings.Fields(recorded)...)
	if _, err := r.run(ctx, service.Command{Args: append(args, "table", "main"), Path: "ip"}); err != nil {
		return fmt.Errorf("removing the host route to %s: %w", host, err)
	}

	return nil
}

func (r hostRoute) recorded() (string, error) {
	if r.goos != "linux" {
		return "", nil
	}

	owned, err := service.OwnsFile(r.file)
	if err != nil || !owned {
		return "", err
	}

	f, err := os.Open(r.file) //#nosec G304 -- an owned file in the tool folder
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}

	if err != nil {
		return "", fmt.Errorf("reading %s: %w", r.file, err)
	}

	defer func() { _ = f.Close() }()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			return line, nil
		}
	}

	if err := sc.Err(); err != nil {
		return "", fmt.Errorf("reading %s: %w", r.file, err)
	}

	return "", nil
}

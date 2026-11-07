package service

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
)

const (
	kernelProbeInterface      = "dvpn-probe"
	kernelProbeTimeout        = 5 * time.Second
	netstatColumnsNoAddress   = 10
	netstatColumnsWithAddress = 11
)

var (
	ErrInterfaceAmbiguous   = errors.New("multiple new tunnel interfaces appeared")
	ErrNewInterfaceNotFound = errors.New("new interface not found")
)

var interfaceNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_=+.-]{1,15}$`)

type execFunc func(ctx context.Context, command Command) ([]byte, error)

func DetectKernelSupport(ctx context.Context, kind string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, kernelProbeTimeout)
	defer cancel()

	return probeKernelSupport(ctx, kind, runtime.GOOS, Run)
}

func DetectNewInterface(before map[string]bool) (string, error) {
	after, err := InterfaceNames()
	if err != nil {
		return "", err
	}

	return selectNewInterface(before, after, runtime.GOOS)
}

func InterfaceNames() (map[string]bool, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("listing interfaces: %w", err)
	}

	names := make(map[string]bool, len(ifaces))
	for _, ifi := range ifaces {
		names[ifi.Name] = true
	}

	return names, nil
}

func ValidateInterfaceName(iface string) error {
	if !interfaceNamePattern.MatchString(iface) {
		return fmt.Errorf("%w: interface %q must match %s", validate.ErrInvalidConfig, iface, interfaceNamePattern)
	}

	return nil
}

func isUtun(iface string) bool {
	suffix, ok := strings.CutPrefix(iface, "utun")
	if !ok || suffix == "" {
		return false
	}

	for _, char := range suffix {
		if char < '0' || char > '9' {
			return false
		}
	}

	return true
}

func parseNetstat(out []byte, iface string) (PeerStats, error) {
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < netstatColumnsNoAddress || fields[0] != iface || !strings.HasPrefix(fields[2], "<Link") {
			continue
		}

		rxIndex, txIndex := 5, 8
		if len(fields) >= netstatColumnsWithAddress {
			rxIndex, txIndex = 6, 9
		}

		rxBytes, err := strconv.ParseUint(fields[rxIndex], 10, 64)
		if err != nil {
			return PeerStats{}, fmt.Errorf("parsing %s rx bytes: %w", iface, err)
		}

		txBytes, err := strconv.ParseUint(fields[txIndex], 10, 64)
		if err != nil {
			return PeerStats{}, fmt.Errorf("parsing %s tx bytes: %w", iface, err)
		}

		// Packet columns sit two before their byte columns (Ipkts Ierrs Ibytes).
		rxPackets, err := strconv.ParseUint(fields[rxIndex-2], 10, 64)
		if err != nil {
			return PeerStats{}, fmt.Errorf("parsing %s rx packets: %w", iface, err)
		}

		txPackets, err := strconv.ParseUint(fields[txIndex-2], 10, 64)
		if err != nil {
			return PeerStats{}, fmt.Errorf("parsing %s tx packets: %w", iface, err)
		}

		return PeerStats{
			RxBytes:   rxBytes,
			RxPackets: rxPackets,
			TxBytes:   txBytes,
			TxPackets: txPackets,
		}, nil
	}

	if err := scanner.Err(); err != nil {
		return PeerStats{}, fmt.Errorf("scanning netstat output: %w", err)
	}

	return PeerStats{}, fmt.Errorf("finding %s: %w", iface, ErrPeerNotFound)
}

func probeKernelSupport(ctx context.Context, kind, goos string, exec execFunc) (bool, error) {
	if goos != "linux" {
		return false, nil
	}

	_, err := exec(ctx, Command{
		Args: []string{"--net", "ip", "link", "add", "dev", kernelProbeInterface, "type", kind},
		Path: "unshare",
	})
	if err == nil {
		return true, nil
	}

	msg := err.Error()
	if strings.Contains(msg, "Unknown device type") || strings.Contains(msg, "Operation not supported") {
		return false, nil
	}

	return false, fmt.Errorf("probing %s kernel support: %w", kind, err)
}

func selectNewInterface(before, after map[string]bool, goos string) (string, error) {
	var added []string

	for name := range after {
		if !before[name] && (goos != "darwin" || isUtun(name)) {
			added = append(added, name)
		}
	}

	if len(added) == 0 {
		return "", ErrNewInterfaceNotFound
	}

	if goos == "darwin" && len(added) > 1 {
		return "", ErrInterfaceAmbiguous
	}

	slices.Sort(added)

	return added[0], nil
}

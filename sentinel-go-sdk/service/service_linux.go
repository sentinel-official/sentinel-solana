package service

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/vishvananda/netlink"
)

//nolint:gochecknoinits
func init() {
	readProcessArgs = processArgsForPID
}

func LinkStats(iface string) (PeerStats, error) {
	link, err := netlink.LinkByName(iface)
	if err != nil {
		return PeerStats{}, fmt.Errorf("finding %s: %w", iface, err)
	}

	stats := link.Attrs().Statistics
	if stats == nil {
		return PeerStats{}, fmt.Errorf("reading %s statistics: %w", iface, ErrPeerNotFound)
	}

	return PeerStats{
		RxBytes:   stats.RxBytes,
		RxPackets: stats.RxPackets,
		TxBytes:   stats.TxBytes,
		TxPackets: stats.TxPackets,
	}, nil
}

func processArgsForPID(ctx context.Context, pid int) (string, []string, error) {
	if err := ctx.Err(); err != nil {
		return "", nil, fmt.Errorf("checking process context: %w", err)
	}

	root, err := os.OpenRoot("/proc")
	if err != nil {
		return "", nil, fmt.Errorf("opening proc filesystem: %w", err)
	}
	defer func() { _ = root.Close() }()

	dir := strconv.Itoa(pid)

	executable, err := root.Readlink(dir + "/exe")
	if err != nil {
		return "", nil, fmt.Errorf("reading process executable: %w", err)
	}

	raw, err := root.ReadFile(dir + "/cmdline")
	if err != nil {
		return "", nil, fmt.Errorf("reading process command: %w", err)
	}

	if len(raw) == 0 {
		return "", nil, os.ErrNotExist
	}

	if raw[len(raw)-1] != 0 {
		return "", nil, os.ErrInvalid
	}

	return executable, strings.Split(string(raw[:len(raw)-1]), "\x00"), nil
}

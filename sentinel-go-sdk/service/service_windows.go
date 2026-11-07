package service

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
)

//nolint:gochecknoinits
func init() {
	killOwnedImage = windowsKillOwnedImage
}

func LinkStats(iface string) (PeerStats, error) {
	ifi, err := net.InterfaceByName(iface)
	if err != nil {
		return PeerStats{}, fmt.Errorf("finding %s: %w", iface, err)
	}

	row := windows.MibIfRow2{InterfaceIndex: uint32(ifi.Index)}
	if err := windows.GetIfEntry2Ex(windows.MibIfEntryNormal, &row); err != nil {
		return PeerStats{}, fmt.Errorf("reading %s statistics: %w", iface, err)
	}

	return PeerStats{
		RxBytes:   row.InOctets,
		RxPackets: row.InUcastPkts + row.InNUcastPkts,
		TxBytes:   row.OutOctets,
		TxPackets: row.OutUcastPkts + row.OutNUcastPkts,
	}, nil
}

func configure(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: windows.CREATE_NEW_PROCESS_GROUP,
		HideWindow:    true,
	}
}

func terminate(cmd *exec.Cmd) error {
	if err := cmd.Process.Kill(); err != nil {
		return fmt.Errorf("killing process: %w", err)
	}

	return nil
}

func windowsKillOwnedImage(ctx context.Context, pid int, owns func(string, time.Time) bool) error {
	const access = windows.PROCESS_QUERY_LIMITED_INFORMATION | windows.PROCESS_TERMINATE | windows.SYNCHRONIZE

	handle, err := windows.OpenProcess(access, false, uint32(pid))
	if errors.Is(err, windows.ERROR_INVALID_PARAMETER) || errors.Is(err, windows.ERROR_ACCESS_DENIED) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("opening process: %w", err)
	}

	defer func() { _ = windows.CloseHandle(handle) }()

	if event, err := windows.WaitForSingleObject(handle, 0); err == nil && event == windows.WAIT_OBJECT_0 {
		return nil
	}

	image := make([]uint16, windows.MAX_LONG_PATH)
	size := uint32(len(image))

	if err := windows.QueryFullProcessImageName(handle, 0, &image[0], &size); err != nil {
		return fmt.Errorf("reading process image: %w", err)
	}

	var created, exited, kernel, user windows.Filetime
	if err := windows.GetProcessTimes(handle, &created, &exited, &kernel, &user); err != nil {
		return fmt.Errorf("reading process times: %w", err)
	}

	if !owns(windows.UTF16ToString(image[:size]), time.Unix(0, created.Nanoseconds())) {
		return nil
	}

	if err := windows.TerminateProcess(handle, 1); err != nil {
		return fmt.Errorf("terminating process: %w", err)
	}

	wait := stopGrace
	if deadline, ok := ctx.Deadline(); ok {
		wait = min(wait, max(time.Until(deadline), 0))
	}

	event, err := windows.WaitForSingleObject(handle, uint32(wait.Milliseconds()))
	if err != nil {
		return fmt.Errorf("waiting for process: %w", err)
	}

	if event != windows.WAIT_OBJECT_0 {
		return fmt.Errorf("waiting for process: %w", windows.WAIT_TIMEOUT)
	}

	return nil
}

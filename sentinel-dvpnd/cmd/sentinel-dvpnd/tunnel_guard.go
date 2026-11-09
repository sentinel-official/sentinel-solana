package main

import (
	"github.com/spf13/cobra"
)

// ─── tunnel guard ───

const tunnelGuardUse = "tunnel-guard"

type tunnelGuardOptions struct {
	ifaces []string
	parent int
}

// newTunnelGuardCmd is the hidden child a client daemon starts on Linux; see leftover.Guard.
func newTunnelGuardCmd() *cobra.Command {
	var opts tunnelGuardOptions

	cmd := &cobra.Command{
		Args:   cobra.NoArgs,
		Hidden: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runTunnelGuard(cmd.Context(), opts, cmd.ErrOrStderr())
		},
		Short: "remove a killed daemon's tunnel interfaces (started by the daemon)",
		Use:   tunnelGuardUse,
	}
	cmd.Flags().StringSliceVar(&opts.ifaces, "iface", nil, "tunnel interface to remove")
	cmd.Flags().IntVar(&opts.parent, "parent", 0, "the daemon's pid")

	return cmd
}

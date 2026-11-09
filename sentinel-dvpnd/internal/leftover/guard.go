package leftover

import (
	"context"
	"os"
	"slices"
	"time"

	"github.com/rs/zerolog"
)

// ─── tunnel guard ───

// A kernel tunnel interface (WireGuard, AmneziaWG on Linux) outlives a daemon that is killed: traffic keeps
// flowing through it to the node while status reads no session, until the next start's recovery removes it.
// The guard is a small child process that removes those interfaces the moment the daemon dies.

const guardRemoveTimeout = 10 * time.Second

// Guard removes the named interfaces when its daemon dies.
type Guard struct {
	Log     zerolog.Logger
	Names   func() []string
	Remover InterfaceRemover
}

// Watch blocks until died or release arrives on sigs, or ctx ends. On died it removes every named interface
// and reports true; release (the daemon stopped cleanly and tore its tunnel down itself) and ctx return false.
func (g Guard) Watch(ctx context.Context, sigs <-chan os.Signal, died, release os.Signal) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case sig := <-sigs:
			switch sig {
			case release:
				return false
			case died:
				g.Remove(ctx)

				return true
			}
		}
	}
}

// Remove removes every named interface, logging each failure and carrying on with the rest.
func (g Guard) Remove(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), guardRemoveTimeout)
	defer cancel()

	names := slices.Compact(slices.Sorted(slices.Values(g.Names())))
	for _, name := range names {
		if name == "" {
			continue
		}

		if err := g.Remover.RemoveInterface(ctx, name); err != nil {
			g.Log.Error().Err(err).Str("iface", name).Msg("tunnel guard: removing a dead daemon's tunnel")

			continue
		}

		g.Log.Info().Str("iface", name).Msg("tunnel guard: the daemon died; removed its tunnel")
	}
}

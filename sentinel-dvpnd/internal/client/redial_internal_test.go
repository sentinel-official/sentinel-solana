package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeRedial runs the policy on a fake clock: every sleep returns at once and moves the clock on.
func fakeRedial(attempts uint) (redialPolicy, *time.Time) {
	clock := time.Unix(0, 0)
	p := newRedialPolicy(attempts, 2*time.Second)
	p.now = func() time.Time { return clock }
	p.sleep = func(_ context.Context, d time.Duration, _ <-chan struct{}) bool {
		clock = clock.Add(d)

		return true
	}

	return p, &clock
}

// The dial error Go gives with no route at all (airplane mode), as it reaches the redial.
func noNetwork() error {
	return fmt.Errorf("fetching node account: %w", &net.OpError{
		Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ENETUNREACH),
	})
}

// Android cb33f S9 (DIR 11-08): airplane mode for ~15 s; all redials failed with "network is
// unreachable" before the network came back, the attempts ran out and the session ended. Failures
// with no network now wait for it instead of spending attempts.
func TestRedial_NoNetworkDoesNotSpendAttempts(t *testing.T) {
	t.Parallel()

	p, clock := fakeRedial(3)
	tries, downFor := 0, 10 // 10 tries with no network (~20 s), then the network is back

	ok := p.run(context.Background(), nil, func(uint, bool) error {
		tries++
		if tries <= downFor {
			return noNetwork()
		}

		return nil
	})

	require.True(t, ok, "the redial after the network came back connects")
	require.Equal(t, downFor+1, tries)
	require.Less(t, clock.Sub(time.Unix(0, 0)), networkWait)
}

func TestRedial_NodeFailuresStillSpendAttempts(t *testing.T) {
	t.Parallel()

	p, _ := fakeRedial(3)
	tries := 0

	ok := p.run(context.Background(), nil, func(uint, bool) error {
		tries++

		return errors.New("dial tcp 35.238.250.188:18711: i/o timeout")
	})

	require.False(t, ok)
	require.Equal(t, 3, tries, "a node that doesn't answer is given the usual attempts")
}

// The wait for the network is capped: then failures count again, so a host that never gets its
// network back still gives up.
func TestRedial_NetworkWaitIsCapped(t *testing.T) {
	t.Parallel()

	p, clock := fakeRedial(3)
	start := *clock

	ok := p.run(context.Background(), nil, func(uint, bool) error { return noNetwork() })

	require.False(t, ok)
	elapsed := clock.Sub(start)
	require.GreaterOrEqual(t, elapsed, networkWait, "waited for the network the whole cap")
	require.Less(t, elapsed, networkWait+2*time.Minute, "then gave up after the usual attempts")
}

// A platform that hears the network come back wakes the waiting redial at once.
func TestRedial_WakeEndsTheNetworkWait(t *testing.T) {
	t.Parallel()

	wake := make(chan struct{}, 1)
	p := newRedialPolicy(3, 10*time.Millisecond)
	p.netPoll = time.Hour // only the wake can end the wait
	tries := 0

	done := make(chan bool, 1)
	go func() {
		done <- p.run(context.Background(), wake, func(uint, bool) error {
			tries++
			if tries == 1 {
				return noNetwork()
			}

			return nil
		})
	}()

	time.Sleep(50 * time.Millisecond)
	wake <- struct{}{}

	select {
	case ok := <-done:
		require.True(t, ok)
	case <-time.After(5 * time.Second):
		t.Fatal("the wake did not end the network wait")
	}
}

func TestNetworkDown(t *testing.T) { //nolint:paralleltest // replaces hasRouteOut

	require.True(t, networkDown(noNetwork()))
	require.True(t, networkDown(errors.New(`Post "https://api.devnet.solana.com": dial tcp 208.115.212.49:443: connect: network is unreachable`)))
	require.True(t, networkDown(errors.New("dial tcp: wsasend: A socket operation was attempted to an unreachable network.")))
	require.False(t, networkDown(errors.New("dial tcp 10.0.0.1:443: connect: no route to host")))
	require.False(t, networkDown(errors.New("dial tcp 10.0.0.1:443: connect: connection refused")))
	require.False(t, networkDown(nil))
}

// FWX review: the wait is per outage. The network came back (a try failed for a node reason), then
// dropped again: the second outage waits too, not only what is left of the first one's 120 s.
func TestRedial_EachOutageGetsItsOwnWait(t *testing.T) {
	t.Parallel()

	p, _ := fakeRedial(3)
	tries := 0

	ok := p.run(context.Background(), nil, func(uint, bool) error {
		tries++

		switch {
		case tries <= 50: // first outage, 100 s
			return noNetwork()
		case tries == 51: // the network is back; the node did not answer
			return errors.New("dial tcp 35.238.250.188:18711: i/o timeout")
		case tries <= 101: // second outage, 100 s more
			return noNetwork()
		default:
			return nil
		}
	})

	require.True(t, ok, "the second outage was waited for as well")
	require.Equal(t, 102, tries)
}

// FWX review: a desktop's local resolver stays up when the Wi-Fi goes, so the redial sees a lookup
// error, not ENETUNREACH. It counts as no network only while the host has no route out.
func TestNetworkDown_LookupFailsWithoutARoute(t *testing.T) { //nolint:paralleltest // replaces hasRouteOut
	route := true
	prev := hasRouteOut
	hasRouteOut = func() bool { return route }
	t.Cleanup(func() { hasRouteOut = prev })

	lookups := []error{
		fmt.Errorf("fetching node account: %w", &net.DNSError{Err: "server misbehaving", Name: "api.devnet.solana.com", Server: "127.0.0.53:53", IsTemporary: true}),
		&net.DNSError{Err: "no such host", Name: "api.devnet.solana.com", IsNotFound: true},
		errors.New(`Post "https://api.devnet.solana.com": dial tcp: lookup api.devnet.solana.com: Temporary failure in name resolution`),
	}

	for _, err := range lookups {
		route = false
		require.True(t, networkDown(err), "no route: %v", err)

		route = true
		require.False(t, networkDown(err), "a lookup failure with a route out still counts: %v", err)
	}
}

// A wake left from before the redial does not end its first network wait.
func TestRedial_StaleWakeIsDropped(t *testing.T) {
	t.Parallel()

	wake := make(chan struct{}, 1)
	wake <- struct{}{} // the network changed while the session was still up

	p, _ := fakeRedial(3)
	waited := 0
	p.sleep = func(_ context.Context, _ time.Duration, w <-chan struct{}) bool {
		if w != nil {
			select {
			case <-w:
				t.Fatal("a stale wake ended the network wait")
			default:
			}
		}
		waited++

		return true
	}

	tries := 0
	ok := p.run(context.Background(), wake, func(uint, bool) error {
		tries++
		if tries < 3 {
			return noNetwork()
		}

		return nil
	})

	require.True(t, ok)
	require.Equal(t, 3, waited)
}

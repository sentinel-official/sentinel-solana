package service_test

import (
	"net/netip"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sentinel-official/sentinel-go-sdk/v3/service"
	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
)

func TestNewPool_RejectsOverlappingPrefixes(t *testing.T) {
	t.Parallel()

	pool, err := service.NewPool(
		netip.MustParsePrefix("10.0.0.1/24"),
		netip.MustParsePrefix("10.0.0.2/25"),
	)
	require.ErrorIs(t, err, validate.ErrInvalidConfig)
	require.Nil(t, pool)
}

func TestParsePrefixes(t *testing.T) {
	t.Parallel()

	values := []string{"10.0.0.1/24", "fd00::1/64"}

	got, err := service.ParsePrefixes(values)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, "10.0.0.1/24", got[0].String())

	bad := []string{"10.0.0.1"}

	_, err = service.ParsePrefixes(bad)
	require.ErrorIs(t, err, validate.ErrInvalidConfig)
	require.Contains(t, err.Error(), `address "10.0.0.1"`)
}

func TestPool_Acquire(t *testing.T) {
	t.Parallel()

	prefixV4 := netip.MustParsePrefix("10.0.0.1/29")
	prefixV6 := netip.MustParsePrefix("fd00::1/125")

	pool, err := service.NewPool(prefixV4, prefixV6)
	require.NoError(t, err)

	acquired := make([][]netip.Prefix, 0, 5)

	for range 5 {
		prefixes, err := pool.Acquire()
		require.NoError(t, err)

		acquired = append(acquired, prefixes)
	}

	require.Equal(t, "10.0.0.2/32", acquired[0][0].String())
	require.Equal(t, "fd00::2/128", acquired[0][1].String())
	require.Equal(t, "10.0.0.3/32", acquired[1][0].String())

	_, err = pool.Acquire()
	require.ErrorIs(t, err, service.ErrPoolExhausted)

	pool.Release(acquired[0])

	reused, err := pool.Acquire()
	require.NoError(t, err)
	require.Equal(t, "10.0.0.2/32", reused[0].String(), "IPv4: the released address is the only one free")
	require.Equal(t, "fd00::7/128", reused[1].String(), "IPv6: a never-used address before the released one")
}

// a lowest-free pool handed every new peer the address the last one just released, and NAT
// flows still open for the old peer reached the new one. While others are free, a released address
// is not handed out again; after a full lap it is.
func TestPool_Acquire_DoesNotReuseAReleasedAddressWhileOthersAreFree(t *testing.T) {
	t.Parallel()

	pool, err := service.NewPool(netip.MustParsePrefix("10.212.1.1/24"))
	require.NoError(t, err)

	first, err := pool.Acquire()
	require.NoError(t, err)
	require.Equal(t, "10.212.1.2/32", first[0].String())
	pool.Release(first)

	seen := map[string]bool{}
	for range 252 { // every other host address once, each released straight away
		p, err := pool.Acquire()
		require.NoError(t, err)
		require.NotEqual(t, "10.212.1.2/32", p[0].String(), "released address reused while others are free")
		require.False(t, seen[p[0].String()], "address %s handed out twice in one lap", p[0])
		seen[p[0].String()] = true
		pool.Release(p)
	}

	wrapped, err := pool.Acquire()
	require.NoError(t, err)
	require.Equal(t, "10.212.1.2/32", wrapped[0].String(), "after a full lap the pool wraps to the first host")
}

func TestPool_Acquire_SkipsUsedAndReservedOnTheLap(t *testing.T) {
	t.Parallel()

	pool, err := service.NewPool(netip.MustParsePrefix("10.0.0.1/29")) // hosts .2-.6
	require.NoError(t, err)

	a, err := pool.Acquire() // .2
	require.NoError(t, err)
	require.NoError(t, pool.Reserve(netip.MustParseAddr("10.0.0.3")))

	b, err := pool.Acquire()
	require.NoError(t, err)
	require.Equal(t, "10.0.0.4/32", b[0].String(), "skips the reserved .3")
	pool.Release(a)

	got := []string{}
	for range 2 {
		p, err := pool.Acquire()
		require.NoError(t, err)
		got = append(got, p[0].String())
	}
	require.Equal(t, []string{"10.0.0.5/32", "10.0.0.6/32"}, got)

	last, err := pool.Acquire() // wraps: .2 is free again, .3 reserved, .4-.6 used
	require.NoError(t, err)
	require.Equal(t, "10.0.0.2/32", last[0].String())

	_, err = pool.Acquire()
	require.ErrorIs(t, err, service.ErrPoolExhausted)
}

func TestPool_Reserve(t *testing.T) {
	t.Parallel()

	pool, err := service.NewPool(netip.MustParsePrefix("10.0.0.1/24"), netip.MustParsePrefix("fd00::1/64"))
	require.NoError(t, err)
	require.NoError(t, pool.Reserve(netip.MustParseAddr("10.0.0.3")))
	require.NoError(t, pool.Reserve(netip.MustParseAddr("fd00::3")))

	first, err := pool.Acquire()
	require.NoError(t, err)
	require.Equal(t, "10.0.0.2/32", first[0].String())
	require.Equal(t, "fd00::2/128", first[1].String())

	second, err := pool.Acquire()
	require.NoError(t, err)
	require.Equal(t, "10.0.0.4/32", second[0].String())
	require.Equal(t, "fd00::4/128", second[1].String())
	require.ErrorIs(t, pool.Reserve(netip.MustParseAddr("10.0.0.3")), service.ErrAddressInUse)
	require.ErrorIs(t, pool.Reserve(netip.MustParseAddr("10.0.0.1")), service.ErrAddressInUse)
	require.ErrorIs(t, pool.Reserve(netip.MustParseAddr("10.1.0.3")), validate.ErrInvalidConfig)
}

package service

import (
	"errors"
	"fmt"
	"net/netip"
	"sync"

	"github.com/sentinel-official/sentinel-go-sdk/v3/validate"
)

var (
	ErrAddressInUse  = errors.New("address already in use")
	ErrPoolExhausted = errors.New("address pool exhausted")
)

type Pool struct {
	families []family
	mu       sync.Mutex
}

func NewPool(prefixes ...netip.Prefix) (*Pool, error) {
	if len(prefixes) == 0 {
		return nil, fmt.Errorf("%w: pool needs at least one prefix", validate.ErrInvalidConfig)
	}

	pool := &Pool{}

	for _, pfx := range prefixes {
		if !pfx.IsValid() {
			return nil, fmt.Errorf("%w: prefix %s", validate.ErrInvalidConfig, pfx)
		}

		masked := pfx.Masked()
		for _, f := range pool.families {
			if f.prefix.Contains(masked.Addr()) || masked.Contains(f.prefix.Addr()) {
				return nil, fmt.Errorf("%w: overlapping prefixes %s and %s", validate.ErrInvalidConfig, f.prefix, masked)
			}
		}

		pool.families = append(pool.families, family{
			prefix: masked,
			self:   pfx.Addr(),
			used:   map[netip.Addr]bool{},
		})
	}

	return pool, nil
}

func (p *Pool) Acquire() ([]netip.Prefix, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]netip.Prefix, 0, len(p.families))
	for i := range p.families {
		addr, ok := p.families[i].next()
		if !ok {
			for j, addr := range out {
				delete(p.families[j].used, addr.Addr())
			}

			return nil, ErrPoolExhausted
		}

		p.families[i].used[addr] = true
		out = append(out, netip.PrefixFrom(addr, addr.BitLen()))
	}

	return out, nil
}

func (p *Pool) Release(prefixes []netip.Prefix) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, prefix := range prefixes {
		for i := range p.families {
			delete(p.families[i].used, prefix.Addr())
		}
	}
}

func (p *Pool) Reserve(addr netip.Addr) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := range p.families {
		f := &p.families[i]
		if !f.prefix.Contains(addr) {
			continue
		}

		if addr == f.self || f.used[addr] || addr == f.prefix.Addr() ||
			(addr.Is4() && !f.prefix.Contains(addr.Next())) {
			return fmt.Errorf("%w: %s", ErrAddressInUse, addr)
		}

		f.used[addr] = true

		return nil
	}

	return fmt.Errorf("%w: address %s outside pool", validate.ErrInvalidConfig, addr)
}

// family hands out addresses round-robin: next starts after the last address it gave out, so an
// address released a moment ago is not handed to the next peer while others are free. A
// lowest-free pool gave every new WireGuard peer the address of the one that just left, and NAT
// flows still open for the old peer were delivered to, and billed against, the new one.
type family struct {
	prefix netip.Prefix
	self   netip.Addr
	used   map[netip.Addr]bool
	cursor netip.Addr // last address handed out; zero before the first
}

func (f *family) next() (netip.Addr, bool) {
	first := f.prefix.Addr().Next()
	start := first
	if f.cursor.IsValid() && f.prefix.Contains(f.cursor.Next()) {
		start = f.cursor.Next()
	}

	// One full lap from start, wrapping to the first host address at the end of the prefix.
	for addr, wrapped := start, false; ; {
		if !f.prefix.Contains(addr) || (addr.Is4() && !f.prefix.Contains(addr.Next())) {
			if wrapped || start == first {
				return netip.Addr{}, false
			}

			addr, wrapped = first, true

			continue
		}

		if wrapped && addr == start {
			return netip.Addr{}, false
		}

		if addr != f.self && !f.used[addr] {
			f.cursor = addr

			return addr, true
		}

		addr = addr.Next()
	}
}

func ParsePrefixes(values []string) ([]netip.Prefix, error) {
	out := make([]netip.Prefix, 0, len(values))
	for _, value := range values {
		prefix, err := netip.ParsePrefix(value)
		if err != nil {
			return nil, fmt.Errorf("%w: address %q", validate.ErrInvalidConfig, value)
		}

		out = append(out, prefix)
	}

	return out, nil
}

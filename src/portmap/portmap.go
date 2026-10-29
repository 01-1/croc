// Package portmap makes a sender's local relay reachable from the internet by
// mapping its TCP ports on the home gateway (PCP, NAT-PMP, or UPnP IGD) or by
// using a global IPv6 address. Tailscale's net/portmapper only maps UDP, so it
// cannot serve croc's TCP relay.
package portmap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"time"

	"tailscale.com/net/netmon"
)

// DefaultLifetime is the mapping lifetime requested from the gateway. Mappings
// are renewed at half their granted lifetime, so a crashed sender's mappings
// expire within this period.
const DefaultLifetime = 10 * time.Minute

var (
	// ErrNoGateway means no default IPv4 gateway was found.
	ErrNoGateway = errors.New("portmap: no default gateway")
	// ErrNotPublic means the gateway mapped the ports, but its external
	// address is not publicly routable (for example double NAT or CGNAT), so
	// the mappings cannot help a receiver on the internet.
	ErrNotPublic = errors.New("portmap: gateway external address is not public")
)

var cgnat = netip.MustParsePrefix("100.64.0.0/10")

// IsPublic reports whether addr is routable on the public internet.
func IsPublic(addr netip.Addr) bool {
	addr = addr.Unmap()
	return addr.IsGlobalUnicast() && !addr.IsPrivate() && !cgnat.Contains(addr)
}

// attempt is one gateway protocol with its own time budget, so a gateway that
// ignores one protocol cannot use up the time the next protocol needs.
type attempt struct {
	budget time.Duration
	new    func(context.Context) (mapper, error)
}

type mapper interface {
	name() string
	// add maps external port == internal port and returns the external
	// address and the lifetime the gateway granted.
	add(ctx context.Context, port uint16, lifetime time.Duration) (netip.AddrPort, time.Duration, error)
	remove(ctx context.Context, port uint16) error
}

// Set is a group of active TCP mappings for the same external address. It
// renews them until Close.
type Set struct {
	mapper   mapper
	external netip.Addr
	ports    []uint16
	lifetime time.Duration
	logf     func(string, ...any)
	stop     context.CancelFunc
	done     chan struct{}
	close    sync.Once
}

// Config controls Map.
type Config struct {
	// Ports are the TCP ports to map; each is mapped to the same external
	// port, because croc's relay advertises its data ports by number.
	Ports []uint16
	// Lifetime is the requested mapping lifetime; zero uses DefaultLifetime.
	Lifetime time.Duration
	// Logf receives debug messages; nil discards them.
	Logf func(string, ...any)
}

// Map maps every port in cfg on the default gateway, trying PCP, NAT-PMP, and
// then UPnP IGD. It returns ErrNotPublic when the gateway works but its
// external address is not public.
func Map(ctx context.Context, cfg Config) (*Set, error) {
	gateway, self, ok := netmon.LikelyHomeRouterIP()
	if !ok || !gateway.Is4() {
		return nil, ErrNoGateway
	}
	gw := netip.AddrPortFrom(gateway, gatewayPort)
	attempts := []attempt{
		{budget: 1500 * time.Millisecond, new: func(context.Context) (mapper, error) { return newPCP(gw, self), nil }},
		{budget: 1500 * time.Millisecond, new: func(context.Context) (mapper, error) { return natpmp{gateway: gw}, nil }},
		// Discovery takes about two seconds; the rest covers the mappings.
		{budget: 4 * time.Second, new: func(ctx context.Context) (mapper, error) { return discoverUPnP(ctx, self) }},
	}
	return mapWith(ctx, cfg, attempts)
}

func mapWith(ctx context.Context, cfg Config, attempts []attempt) (*Set, error) {
	if len(cfg.Ports) == 0 {
		return nil, errors.New("portmap: no ports to map")
	}
	if cfg.Lifetime <= 0 {
		cfg.Lifetime = DefaultLifetime
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}
	var errs []error
	for _, try := range attempts {
		if ctx.Err() != nil {
			break
		}
		attemptCtx, cancel := ctx, context.CancelFunc(func() {})
		if try.budget > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, try.budget)
		}
		m, err := try.new(attemptCtx)
		if err != nil {
			cancel()
			errs = append(errs, err)
			continue
		}
		set, err := mapAll(attemptCtx, m, cfg)
		cancel()
		if err == nil {
			return set, nil
		}
		cfg.Logf("portmap: %s failed: %v", m.name(), err)
		if errors.Is(err, ErrNotPublic) {
			// Every protocol reports the same gateway external address.
			return nil, err
		}
		errs = append(errs, err)
	}
	if err := ctx.Err(); err != nil {
		errs = append(errs, err)
	}
	return nil, fmt.Errorf("portmap: no gateway protocol mapped the relay ports: %w", errors.Join(errs...))
}

func mapAll(ctx context.Context, m mapper, cfg Config) (*Set, error) {
	set := &Set{mapper: m, lifetime: cfg.Lifetime, logf: cfg.Logf}
	renewEvery := cfg.Lifetime
	for _, port := range cfg.Ports {
		external, granted, err := m.add(ctx, port, cfg.Lifetime)
		if err != nil {
			set.removeAll()
			return nil, err
		}
		set.ports = append(set.ports, port)
		if external.Port() != port {
			set.removeAll()
			return nil, fmt.Errorf("%s: gateway assigned external port %d for internal port %d", m.name(), external.Port(), port)
		}
		if !set.external.IsValid() {
			set.external = external.Addr()
		} else if external.Addr() != set.external {
			set.removeAll()
			return nil, fmt.Errorf("%s: gateway reported different external addresses %v and %v", m.name(), set.external, external.Addr())
		}
		if granted <= 0 {
			set.removeAll()
			return nil, fmt.Errorf("%s: gateway granted no lifetime for port %d", m.name(), port)
		}
		renewEvery = min(renewEvery, granted)
	}
	if !IsPublic(set.external) {
		set.removeAll()
		return nil, fmt.Errorf("%w: %s reported %v", ErrNotPublic, m.name(), set.external)
	}
	renewCtx, stop := context.WithCancel(context.Background())
	set.stop = stop
	set.done = make(chan struct{})
	go set.renew(renewCtx, renewEvery/2)
	cfg.Logf("portmap: %s mapped TCP %v on %v", m.name(), set.ports, set.external)
	return set, nil
}

// Protocol names the gateway protocol that created the mappings.
func (s *Set) Protocol() string { return s.mapper.name() }

// Address returns the public address that reaches the mapped ports.
func (s *Set) Address() netip.Addr { return s.external }

func (s *Set) renew(ctx context.Context, every time.Duration) {
	defer close(s.done)
	ticker := time.NewTicker(max(every, time.Nanosecond))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nextEvery := s.lifetime / 2
			for _, port := range s.ports {
				addCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				_, granted, err := s.mapper.add(addCtx, port, s.lifetime)
				cancel()
				if err == nil && granted <= 0 {
					err = errors.New("gateway granted no lifetime")
				}
				if err != nil {
					// A failed renewal must not lengthen the retry interval
					// while an earlier, shorter grant may still be active.
					nextEvery = min(nextEvery, every)
					if ctx.Err() == nil {
						s.logf("portmap: renewing %s mapping for TCP %d: %v", s.mapper.name(), port, err)
					}
					continue
				}
				nextEvery = min(nextEvery, granted/2)
			}
			every = max(nextEvery, time.Nanosecond)
			ticker.Reset(every)
		}
	}
}

// Close stops renewal and removes every mapping. It is safe to call more than
// once.
func (s *Set) Close() error {
	if s == nil {
		return nil
	}
	var err error
	s.close.Do(func() {
		if s.stop != nil {
			s.stop()
			<-s.done
		}
		err = s.removeAll()
	})
	return err
}

func (s *Set) removeAll() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var errs []error
	for _, port := range s.ports {
		if err := s.mapper.remove(ctx, port); err != nil {
			errs = append(errs, fmt.Errorf("remove %s mapping for TCP %d: %w", s.mapper.name(), port, err))
		}
	}
	s.ports = nil
	return errors.Join(errs...)
}

// GlobalIPv6 returns this host's preferred global IPv6 source address, if the
// routing table has one. It sends no packets: connecting a UDP socket only
// selects a route. A global address does not prove that inbound connections
// pass the network's firewall; receivers fall back when they do not.
func GlobalIPv6() (netip.Addr, bool) {
	conn, err := net.Dial("udp6", "[2001:4860:4860::8888]:53")
	if err != nil {
		return netip.Addr{}, false
	}
	defer conn.Close()
	local, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return netip.Addr{}, false
	}
	addr, ok := netip.AddrFromSlice(local.IP)
	if !ok || !addr.Is6() || addr.Is4In6() || !IsPublic(addr) {
		return netip.Addr{}, false
	}
	return addr, true
}

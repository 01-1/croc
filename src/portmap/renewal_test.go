package portmap

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type leaseMapper struct {
	mu     sync.Mutex
	grants map[uint16][]time.Duration
	calls  map[uint16]int
}

func (*leaseMapper) name() string { return "test-leases" }

func (m *leaseMapper) add(_ context.Context, port uint16, _ time.Duration) (netip.AddrPort, time.Duration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	grants := m.grants[port]
	granted := grants[min(m.calls[port], len(grants)-1)]
	m.calls[port]++
	if granted < 0 {
		return netip.AddrPort{}, 0, errors.New("transient renewal failure")
	}
	return netip.AddrPortFrom(publicAddr, port), granted, nil
}

func (*leaseMapper) remove(context.Context, uint16) error { return nil }

func (m *leaseMapper) callCount(port uint16) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[port]
}

func TestRenewalTracksGrantedLifetimes(t *testing.T) {
	type checkpoint struct {
		at    time.Duration
		calls int
	}
	for _, tt := range []struct {
		name   string
		grants map[uint16][]time.Duration
		checks []checkpoint
	}{
		{
			name:   "shorter then longer grant",
			grants: map[uint16][]time.Duration{9009: {6 * time.Second, 2 * time.Second, 6 * time.Second}},
			checks: []checkpoint{{3 * time.Second, 2}, {4 * time.Second, 3}, {6 * time.Second, 3}, {7 * time.Second, 4}},
		},
		{
			name: "shortest grant on first port",
			grants: map[uint16][]time.Duration{
				9009: {6 * time.Second, 2 * time.Second},
				9010: {6 * time.Second, 4 * time.Second},
			},
			checks: []checkpoint{{3 * time.Second, 2}, {4 * time.Second, 3}},
		},
		{
			name: "shortest grant on last port",
			grants: map[uint16][]time.Duration{
				9009: {6 * time.Second, 4 * time.Second},
				9010: {6 * time.Second, 2 * time.Second},
			},
			checks: []checkpoint{{3 * time.Second, 2}, {4 * time.Second, 3}},
		},
		{
			name: "failed renewal keeps the shorter retry interval",
			grants: map[uint16][]time.Duration{
				9009: {6 * time.Second, 2 * time.Second, -1, 6 * time.Second},
				9010: {6 * time.Second, 2 * time.Second, 6 * time.Second},
			},
			checks: []checkpoint{{3 * time.Second, 2}, {4 * time.Second, 3}, {5 * time.Second, 4}},
		},
		{
			name:   "zero grant keeps the shorter retry interval",
			grants: map[uint16][]time.Duration{9009: {6 * time.Second, 2 * time.Second, 0, 6 * time.Second}},
			checks: []checkpoint{{3 * time.Second, 2}, {4 * time.Second, 3}, {5 * time.Second, 4}},
		},
		{
			name:   "one second grant renews before expiry",
			grants: map[uint16][]time.Duration{9009: {6 * time.Second, time.Second}},
			checks: []checkpoint{{3 * time.Second, 2}, {3500 * time.Millisecond, 3}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				m := &leaseMapper{grants: tt.grants, calls: make(map[uint16]int)}
				ports := []uint16{9009}
				if _, ok := tt.grants[9010]; ok {
					ports = append(ports, 9010)
				}
				set, err := mapWith(context.Background(), Config{Ports: ports, Lifetime: 6 * time.Second},
					attempts(func(context.Context) (mapper, error) { return m, nil }))
				if err != nil {
					t.Fatal(err)
				}
				defer set.Close()
				synctest.Wait()
				var elapsed time.Duration
				for _, check := range tt.checks {
					time.Sleep(check.at - elapsed)
					synctest.Wait()
					for _, port := range ports {
						if got := m.callCount(port); got != check.calls {
							t.Fatalf("at %v: port %d has %d mapping calls, want %d", check.at, port, got, check.calls)
						}
					}
					elapsed = check.at
				}
			})
		})
	}
}

//go:build portmap_live

package portmap

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"testing"
	"time"

	"tailscale.com/net/netmon"
)

// TestLiveGateway maps and removes one TCP port with each protocol on the real
// default gateway, then runs Map. Mappings request a one-minute lifetime, so
// anything left behind expires quickly. Run it on a network you control:
//
//	go test -tags portmap_live -run TestLiveGateway -v ./src/portmap
func TestLiveGateway(t *testing.T) {
	gateway, self, ok := netmon.LikelyHomeRouterIP()
	if !ok || !gateway.Is4() {
		t.Skip("no default IPv4 gateway")
	}
	t.Logf("gateway %v, this host %v", gateway, self)
	listener, err := net.Listen("tcp4", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := uint16(listener.Addr().(*net.TCPAddr).Port)
	gw := netip.AddrPortFrom(gateway, gatewayPort)

	mappers := []func(context.Context) (mapper, error){
		func(context.Context) (mapper, error) { return newPCP(gw, self), nil },
		func(context.Context) (mapper, error) { return natpmp{gateway: gw}, nil },
		func(ctx context.Context) (mapper, error) { return discoverUPnP(ctx, self) },
	}
	for _, newMapper := range mappers {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		m, err := newMapper(ctx)
		if err != nil {
			t.Logf("discovery failed: %v", err)
			cancel()
			continue
		}
		external, granted, err := m.add(ctx, port, time.Minute)
		if err != nil {
			t.Logf("%s: add TCP %d failed: %v", m.name(), port, err)
			cancel()
			continue
		}
		t.Logf("%s: mapped TCP %d to %v for %v (public: %t)", m.name(), port, external, granted, IsPublic(external.Addr()))
		if err := m.remove(ctx, port); err != nil {
			t.Errorf("%s: remove TCP %d: %v", m.name(), port, err)
		} else {
			t.Logf("%s: removed TCP %d", m.name(), port)
		}
		cancel()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	set, err := Map(ctx, Config{Ports: []uint16{port}, Lifetime: time.Minute, Logf: t.Logf})
	switch {
	case err == nil:
		t.Logf("Map: %s mapped TCP %d on public address %v", set.Protocol(), port, set.Address())
		if err := set.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	case errors.Is(err, ErrNotPublic):
		t.Logf("Map: gateway works but is not publicly reachable: %v", err)
	default:
		t.Logf("Map failed: %v", err)
	}
	if ipv6, ok := GlobalIPv6(); ok {
		t.Logf("global IPv6 address: %v", ipv6)
	} else {
		t.Log("no global IPv6 route")
	}
}

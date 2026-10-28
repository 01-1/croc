package portmap

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/huin/goupnp/soap"
)

// fakeGateway answers NAT-PMP and PCP requests on a loopback UDP port and
// records the active TCP mappings by internal port.
type fakeGateway struct {
	t        *testing.T
	conn     net.PacketConn
	external netip.Addr
	// remap, if set, is the external port assigned instead of the suggested one.
	remap    uint16
	mu       sync.Mutex
	mappings map[uint16]uint32
	requests int
}

func newFakeGateway(t *testing.T, external netip.Addr) *fakeGateway {
	t.Helper()
	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := &fakeGateway{t: t, conn: conn, external: external, mappings: make(map[uint16]uint32)}
	t.Cleanup(func() { conn.Close() })
	go g.serve()
	return g
}

func (g *fakeGateway) addr() netip.AddrPort {
	return g.conn.LocalAddr().(*net.UDPAddr).AddrPort()
}

func (g *fakeGateway) active() map[uint16]uint32 {
	g.mu.Lock()
	defer g.mu.Unlock()
	active := make(map[uint16]uint32, len(g.mappings))
	for port, lifetime := range g.mappings {
		active[port] = lifetime
	}
	return active
}

func (g *fakeGateway) serve() {
	buf := make([]byte, 1100)
	for {
		n, from, err := g.conn.ReadFrom(buf)
		if err != nil {
			return
		}
		g.mu.Lock()
		g.requests++
		g.mu.Unlock()
		var response []byte
		switch {
		case n == 2 && buf[0] == 0 && buf[1] == 0:
			response = make([]byte, 12)
			response[1] = 128
			ip := g.external.As4()
			copy(response[8:], ip[:])
		case n == 12 && buf[0] == 0 && buf[1] == 2:
			internal := binary.BigEndian.Uint16(buf[4:])
			lifetime := binary.BigEndian.Uint32(buf[8:])
			external := g.record(internal, binary.BigEndian.Uint16(buf[6:]), lifetime)
			response = make([]byte, 16)
			response[1] = 130
			binary.BigEndian.PutUint16(response[8:], internal)
			binary.BigEndian.PutUint16(response[10:], external)
			binary.BigEndian.PutUint32(response[12:], lifetime)
		case n == pcpMapSize && buf[0] == pcpVersion && buf[1] == pcpOpcodeMap:
			internal := binary.BigEndian.Uint16(buf[40:])
			lifetime := binary.BigEndian.Uint32(buf[4:])
			external := g.record(internal, binary.BigEndian.Uint16(buf[42:]), lifetime)
			response = make([]byte, pcpMapSize)
			response[0] = pcpVersion
			response[1] = pcpOpcodeMap | pcpResponseBit
			binary.BigEndian.PutUint32(response[4:], lifetime)
			copy(response[24:44], buf[24:44])
			binary.BigEndian.PutUint16(response[42:], external)
			ip := netip.AddrFrom16(g.external.As16()).As16()
			copy(response[44:60], ip[:])
		default:
			continue
		}
		g.conn.WriteTo(response, from)
	}
}

func (g *fakeGateway) record(internal, suggested uint16, lifetime uint32) uint16 {
	g.mu.Lock()
	defer g.mu.Unlock()
	if lifetime == 0 {
		delete(g.mappings, internal)
		return 0
	}
	g.mappings[internal] = lifetime
	if g.remap != 0 {
		return g.remap
	}
	return suggested
}

var publicAddr = netip.MustParseAddr("203.0.113.7")

func TestPCPAndNATPMPMapRenewAndRemove(t *testing.T) {
	for _, protocol := range []string{"pcp", "nat-pmp"} {
		t.Run(protocol, func(t *testing.T) {
			gateway := newFakeGateway(t, publicAddr)
			var m mapper = natpmp{gateway: gateway.addr()}
			if protocol == "pcp" {
				m = newPCP(gateway.addr(), netip.MustParseAddr("192.168.1.20"))
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			set, err := mapWith(ctx, Config{Ports: []uint16{9009, 9010}, Lifetime: 2 * time.Second},
				[]func(context.Context) (mapper, error){func(context.Context) (mapper, error) { return m, nil }})
			if err != nil {
				t.Fatal(err)
			}
			if set.Address() != publicAddr || set.Protocol() != protocol {
				t.Fatalf("set = %v via %s", set.Address(), set.Protocol())
			}
			if active := gateway.active(); len(active) != 2 || active[9009] != 2 || active[9010] != 2 {
				t.Fatalf("active mappings = %v", active)
			}
			gateway.mu.Lock()
			before := gateway.requests
			gateway.mu.Unlock()
			// Renewal runs at half the granted lifetime.
			time.Sleep(1500 * time.Millisecond)
			gateway.mu.Lock()
			after := gateway.requests
			gateway.mu.Unlock()
			if after <= before {
				t.Fatal("mappings were not renewed")
			}
			if err := set.Close(); err != nil {
				t.Fatal(err)
			}
			if active := gateway.active(); len(active) != 0 {
				t.Fatalf("mappings left after Close: %v", active)
			}
			if err := set.Close(); err != nil {
				t.Fatalf("second Close: %v", err)
			}
		})
	}
}

func TestMapRejectsNonPublicGatewayAndRemovesMappings(t *testing.T) {
	gateway := newFakeGateway(t, netip.MustParseAddr("192.168.1.177"))
	tried := 0
	next := func(context.Context) (mapper, error) {
		tried++
		return natpmp{gateway: gateway.addr()}, nil
	}
	_, err := mapWith(context.Background(), Config{Ports: []uint16{9009}},
		[]func(context.Context) (mapper, error){
			func(context.Context) (mapper, error) {
				return newPCP(gateway.addr(), netip.MustParseAddr("192.168.86.59")), nil
			},
			next,
		})
	if !errors.Is(err, ErrNotPublic) {
		t.Fatalf("error = %v; want ErrNotPublic", err)
	}
	if tried != 0 {
		t.Fatal("other protocols were tried although the gateway address is not public")
	}
	if active := gateway.active(); len(active) != 0 {
		t.Fatalf("mappings left behind: %v", active)
	}
}

func TestMapRejectsChangedExternalPortAndFallsBack(t *testing.T) {
	remapping := newFakeGateway(t, publicAddr)
	remapping.mu.Lock()
	remapping.remap = 40000
	remapping.mu.Unlock()
	good := newFakeGateway(t, publicAddr)
	set, err := mapWith(context.Background(), Config{Ports: []uint16{9009}},
		[]func(context.Context) (mapper, error){
			func(context.Context) (mapper, error) { return natpmp{gateway: remapping.addr()}, nil },
			func(context.Context) (mapper, error) { return natpmp{gateway: good.addr()}, nil },
		})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	if active := remapping.active(); len(active) != 0 {
		t.Fatalf("remapped gateway kept mappings: %v", active)
	}
	if active := good.active(); len(active) != 1 {
		t.Fatalf("fallback gateway mappings = %v", active)
	}
}

func TestMapFailsWhenNoProtocolResponds(t *testing.T) {
	silent, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	addr := silent.LocalAddr().(*net.UDPAddr).AddrPort()
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_, err = mapWith(ctx, Config{Ports: []uint16{9009}},
		[]func(context.Context) (mapper, error){
			func(context.Context) (mapper, error) { return natpmp{gateway: addr}, nil },
		})
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v; want a deadline failure", err)
	}
}

type fakeIGD struct {
	mu        sync.Mutex
	permanent bool
	leases    map[uint16]uint32
	external  string
}

func (f *fakeIGD) AddPortMappingCtx(_ context.Context, _ string, externalPort uint16, protocol string, internalPort uint16, _ string, _ bool, _ string, lease uint32) error {
	if protocol != "TCP" || externalPort != internalPort {
		return errors.New("unexpected mapping request")
	}
	if f.permanent && lease != 0 {
		fault := &soap.SOAPFaultError{}
		fault.Detail.UPnPError.Errorcode = 725
		return fault
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leases[externalPort] = lease
	return nil
}

func (f *fakeIGD) DeletePortMappingCtx(_ context.Context, _ string, externalPort uint16, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.leases, externalPort)
	return nil
}

func (f *fakeIGD) GetExternalIPAddressCtx(context.Context) (string, error) { return f.external, nil }

func TestUPnPFallsBackToPermanentLeaseAndRemoves(t *testing.T) {
	igd := &fakeIGD{permanent: true, leases: map[uint16]uint32{}, external: publicAddr.String()}
	set, err := mapWith(context.Background(), Config{Ports: []uint16{9009, 9010}},
		[]func(context.Context) (mapper, error){
			func(context.Context) (mapper, error) {
				return &upnp{client: igd, self: netip.MustParseAddr("192.168.1.20")}, nil
			},
		})
	if err != nil {
		t.Fatal(err)
	}
	if lease, ok := igd.leases[9009]; !ok || lease != 0 || len(igd.leases) != 2 {
		t.Fatalf("UPnP leases = %v; want two permanent mappings", igd.leases)
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	if len(igd.leases) != 0 {
		t.Fatalf("UPnP mappings left after Close: %v", igd.leases)
	}
}

func TestIsPublic(t *testing.T) {
	for addr, want := range map[string]bool{
		"203.0.113.7":        true,
		"2620:9b::1924:6b10": true,
		"192.168.1.177":      false,
		"10.0.0.1":           false,
		"100.64.0.1":         false,
		"127.0.0.1":          false,
		"169.254.1.1":        false,
		"fd7a:115c:a1e0::1":  false,
		"fe80::1":            false,
		"::ffff:192.168.1.1": false,
		"::ffff:203.0.113.7": true,
	} {
		if got := IsPublic(netip.MustParseAddr(addr)); got != want {
			t.Errorf("IsPublic(%s) = %t; want %t", addr, got, want)
		}
	}
}

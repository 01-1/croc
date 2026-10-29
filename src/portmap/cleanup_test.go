package portmap

import (
	"context"
	"errors"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"github.com/huin/goupnp/soap"
)

type cleanupIGD struct {
	*fakeIGD
	entries   []mappingEntry
	replaced  map[uint16]mappingEntry
	deleted   []uint16
	listErr   error
	deleteErr map[uint16]error
}

func mappingFault(code int) error {
	fault := &soap.SOAPFaultError{}
	fault.Detail.UPnPError.Errorcode = code
	return fault
}

func (f *cleanupIGD) GetGenericPortMappingEntryCtx(_ context.Context, index uint16) (string, uint16, string, uint16, string, bool, string, uint32, error) {
	if f.listErr != nil && index > 0 {
		return "", 0, "", 0, "", false, "", 0, f.listErr
	}
	if int(index) >= len(f.entries) {
		return "", 0, "", 0, "", false, "", 0, mappingFault(713)
	}
	m := f.entries[index]
	return m.remoteHost, m.externalPort, m.protocol, m.internalPort, m.internalClient, true, m.description, m.lease, nil
}

func (f *cleanupIGD) GetSpecificPortMappingEntryCtx(_ context.Context, _ string, port uint16, _ string) (uint16, string, bool, string, uint32, error) {
	if m, ok := f.replaced[port]; ok {
		return m.internalPort, m.internalClient, true, m.description, m.lease, nil
	}
	for _, m := range f.entries {
		if m.externalPort == port {
			return m.internalPort, m.internalClient, true, m.description, m.lease, nil
		}
	}
	return 0, "", false, "", 0, mappingFault(714)
}

func (f *cleanupIGD) DeletePortMappingCtx(_ context.Context, _ string, port uint16, _ string) error {
	if err := f.deleteErr[port]; err != nil {
		return err
	}
	for i, m := range f.entries {
		if m.externalPort == port {
			f.entries = append(f.entries[:i], f.entries[i+1:]...)
			f.deleted = append(f.deleted, port)
			return nil
		}
	}
	return mappingFault(714)
}

func cleanupEntry(port uint16) mappingEntry {
	return mappingEntry{externalPort: port, protocol: "TCP", internalPort: port, internalClient: "192.168.1.20", description: mappingDescription}
}

func TestCleanupRemovesOnlyUnlimitedCrocForwardsForThisHost(t *testing.T) {
	f := &cleanupIGD{entries: []mappingEntry{cleanupEntry(9009), cleanupEntry(9010)}}
	for _, change := range []func(*mappingEntry){
		func(m *mappingEntry) { m.description = "another application" },
		func(m *mappingEntry) { m.internalClient = "192.168.1.21" },
		func(m *mappingEntry) { m.protocol = "UDP" },
		func(m *mappingEntry) { m.lease = 600 },
		func(m *mappingEntry) { m.internalPort++ },
		func(m *mappingEntry) { m.remoteHost = "203.0.113.1" },
	} {
		m := cleanupEntry(uint16(9010 + len(f.entries)))
		change(&m)
		f.entries = append(f.entries, m)
	}
	var reserved, released []uint16
	reserve := func(port uint16) (func(), error) {
		reserved = append(reserved, port)
		return func() { released = append(released, port) }, nil
	}
	result, err := cleanupUPnP(context.Background(), f, netip.MustParseAddr("192.168.1.20"), reserve)
	if err != nil || !reflect.DeepEqual(result.Removed, []uint16{9009, 9010}) || len(result.Skipped) != 0 {
		t.Fatalf("cleanup = %+v, %v", result, err)
	}
	if len(f.entries) != 6 || !reflect.DeepEqual(f.deleted, result.Removed) || !reflect.DeepEqual(reserved, released) {
		t.Fatalf("entries=%v deleted=%v reserved=%v released=%v", f.entries, f.deleted, reserved, released)
	}
	result, err = cleanupUPnP(context.Background(), f, netip.MustParseAddr("192.168.1.20"), reserve)
	if err != nil || len(result.Removed) != 0 {
		t.Fatalf("repeated cleanup = %+v, %v", result, err)
	}
}

func TestCleanupKeepsActivePortsAndReplacedRules(t *testing.T) {
	replacement := cleanupEntry(9010)
	replacement.description = "new owner"
	f := &cleanupIGD{entries: []mappingEntry{cleanupEntry(9009), cleanupEntry(9010)}, replaced: map[uint16]mappingEntry{9010: replacement}}
	released := false
	result, err := cleanupUPnP(context.Background(), f, netip.MustParseAddr("192.168.1.20"), func(port uint16) (func(), error) {
		if port == 9009 {
			return nil, errors.New("port in use")
		}
		return func() { released = true }, nil
	})
	if err != nil || len(result.Removed) != 0 || !reflect.DeepEqual(result.Skipped, []uint16{9009, 9010}) || len(f.deleted) != 0 || !released {
		t.Fatalf("cleanup = %+v, %v; deleted=%v released=%t", result, err, f.deleted, released)
	}
}

func TestCleanupDoesNotDeleteAfterIncompleteListing(t *testing.T) {
	listErr := errors.New("router stopped responding")
	f := &cleanupIGD{entries: []mappingEntry{cleanupEntry(9009)}, listErr: listErr}
	_, err := cleanupUPnP(context.Background(), f, netip.MustParseAddr("192.168.1.20"), func(uint16) (func(), error) {
		t.Fatal("reserved a port after an incomplete listing")
		return nil, nil
	})
	if !errors.Is(err, listErr) || len(f.deleted) != 0 {
		t.Fatalf("error=%v deleted=%v", err, f.deleted)
	}
}

func TestCleanupReportsPartialFailureAndReleasesPorts(t *testing.T) {
	deleteErr := errors.New("router rejected deletion")
	f := &cleanupIGD{entries: []mappingEntry{cleanupEntry(9009), cleanupEntry(9010)}, deleteErr: map[uint16]error{9009: deleteErr}}
	releases := 0
	result, err := cleanupUPnP(context.Background(), f, netip.MustParseAddr("192.168.1.20"), func(uint16) (func(), error) {
		return func() { releases++ }, nil
	})
	if !errors.Is(err, deleteErr) || !reflect.DeepEqual(result.Removed, []uint16{9010}) || releases != 2 {
		t.Fatalf("cleanup=%+v error=%v releases=%d", result, err, releases)
	}
}

type noticeIGD struct {
	*fakeIGD
	notices *int
}

func (f noticeIGD) AddPortMappingCtx(ctx context.Context, remote string, external uint16, protocol string, internal uint16, host string, enabled bool, description string, lease uint32) error {
	if lease == 0 && *f.notices != 1 {
		return errors.New("unlimited forward requested before notification")
	}
	return f.fakeIGD.AddPortMappingCtx(ctx, remote, external, protocol, internal, host, enabled, description, lease)
}

func TestUnlimitedForwardNoticePrecedesRequestAndRunsOnce(t *testing.T) {
	for _, permanent := range []bool{false, true} {
		notices := 0
		f := noticeIGD{fakeIGD: &fakeIGD{permanent: permanent, leases: make(map[uint16]uint32), external: publicAddr.String()}, notices: &notices}
		set, err := mapWith(context.Background(), Config{Ports: []uint16{9009, 9010}, Lifetime: time.Minute, OnPermanentMapping: func() { notices++ }},
			attempts(func(context.Context) (mapper, error) {
				return &upnp{client: f, self: netip.MustParseAddr("192.168.1.20")}, nil
			}))
		if err != nil {
			t.Fatal(err)
		}
		if err := set.Close(); err != nil {
			t.Fatal(err)
		}
		want := 0
		if permanent {
			want = 1
		}
		if notices != want {
			t.Fatalf("permanent=%t: got %d notices, want %d", permanent, notices, want)
		}
	}
}

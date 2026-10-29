package portmap

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sync"
	"time"

	"github.com/huin/goupnp/dcps/internetgateway2"
	"github.com/huin/goupnp/soap"
)

// igdClient is the subset of the UPnP IGD WANIPConnection and
// WANPPPConnection services used for TCP port mappings.
type igdClient interface {
	AddPortMappingCtx(ctx context.Context, remoteHost string, externalPort uint16, protocol string, internalPort uint16, internalClient string, enabled bool, description string, leaseSeconds uint32) error
	DeletePortMappingCtx(ctx context.Context, remoteHost string, externalPort uint16, protocol string) error
	GetExternalIPAddressCtx(ctx context.Context) (string, error)
}

// upnp maps TCP ports with a UPnP Internet Gateway Device.
type upnp struct {
	client igdClient
	self   netip.Addr
	// permanent is set when the gateway only accepts unlimited leases
	// (UPnP error 725); those mappings rely on remove at exit.
	permanent bool
}

// upnpSearches find IGD connection services, in order of preference.
var upnpSearches = []func(context.Context) ([]igdClient, error){
	func(ctx context.Context) ([]igdClient, error) {
		clients, _, err := internetgateway2.NewWANIPConnection2ClientsCtx(ctx)
		return asIGD(clients), err
	},
	func(ctx context.Context) ([]igdClient, error) {
		clients, _, err := internetgateway2.NewWANIPConnection1ClientsCtx(ctx)
		return asIGD(clients), err
	},
	func(ctx context.Context) ([]igdClient, error) {
		clients, _, err := internetgateway2.NewWANPPPConnection1ClientsCtx(ctx)
		return asIGD(clients), err
	},
}

// discoverUPnP returns the most preferred IGD connection service found on the
// LAN. The searches run at once: each SSDP search takes about two seconds, and
// goupnp refuses to start one with less than a second left, so running them in
// turn would starve the older WANIPConnection:1 and WANPPPConnection:1
// services.
func discoverUPnP(ctx context.Context, self netip.Addr) (*upnp, error) {
	return discoverUPnPWith(ctx, self, upnpSearches)
}

func discoverUPnPWith(ctx context.Context, self netip.Addr, searches []func(context.Context) ([]igdClient, error)) (*upnp, error) {
	found := make([][]igdClient, len(searches))
	var wg sync.WaitGroup
	for i, search := range searches {
		wg.Go(func() {
			if clients, err := search(ctx); err == nil {
				found[i] = clients
			}
		})
	}
	wg.Wait()
	for _, clients := range found {
		if len(clients) > 0 {
			return &upnp{client: clients[0], self: self}, nil
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, errors.New("upnp: no internet gateway device found")
}

func asIGD[T igdClient](clients []T) []igdClient {
	result := make([]igdClient, len(clients))
	for i, client := range clients {
		result[i] = client
	}
	return result
}

func (*upnp) name() string { return "upnp" }

func (m *upnp) add(ctx context.Context, port uint16, lifetime time.Duration) (netip.AddrPort, time.Duration, error) {
	// Look up the external address before touching the mapping, so a failed
	// lookup can neither leave a new mapping behind nor, during renewal,
	// disturb one that is working.
	external, err := m.client.GetExternalIPAddressCtx(ctx)
	if err != nil {
		return netip.AddrPort{}, 0, fmt.Errorf("upnp: external address: %w", err)
	}
	addr, err := netip.ParseAddr(external)
	if err != nil {
		return netip.AddrPort{}, 0, fmt.Errorf("upnp: external address %q: %w", external, err)
	}
	lease := uint32(lifetime / time.Second)
	if m.permanent {
		lease = 0
	}
	err = m.client.AddPortMappingCtx(ctx, "", port, "TCP", port, m.self.String(), true, "croc relay", lease)
	if err != nil && lease != 0 && isOnlyPermanentLease(err) {
		m.permanent = true
		lease = 0
		err = m.client.AddPortMappingCtx(ctx, "", port, "TCP", port, m.self.String(), true, "croc relay", 0)
	}
	if err != nil {
		return netip.AddrPort{}, 0, fmt.Errorf("upnp: add TCP port %d: %w", port, err)
	}
	granted := time.Duration(lease) * time.Second
	if lease == 0 {
		// An unlimited lease never needs renewing, but renewing at the
		// requested interval is harmless and keeps the scheduler simple.
		granted = lifetime
	}
	return netip.AddrPortFrom(addr, port), granted, nil
}

func (m *upnp) remove(ctx context.Context, port uint16) error {
	return m.client.DeletePortMappingCtx(ctx, "", port, "TCP")
}

// isOnlyPermanentLease reports UPnP error 725, OnlyPermanentLeasesSupported.
func isOnlyPermanentLease(err error) bool {
	fault, ok := errors.AsType[*soap.SOAPFaultError](err)
	return ok && fault.Detail.UPnPError.Errorcode == 725
}

package portmap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"

	"github.com/huin/goupnp/soap"
	"tailscale.com/net/netmon"
)

// CleanupResult lists removed rules and ports left alone because they are in
// use or their mapping changed during cleanup.
type CleanupResult struct {
	Removed []uint16
	Skipped []uint16
}

type igdMappingReader interface {
	GetGenericPortMappingEntryCtx(context.Context, uint16) (string, uint16, string, uint16, string, bool, string, uint32, error)
	GetSpecificPortMappingEntryCtx(context.Context, string, uint16, string) (uint16, string, bool, string, uint32, error)
}

type mappingEntry struct {
	remoteHost     string
	externalPort   uint16
	protocol       string
	internalPort   uint16
	internalClient string
	description    string
	lease          uint32
}

func (m mappingEntry) ownedUnlimited(self netip.Addr) bool {
	internal, err := netip.ParseAddr(m.internalClient)
	return err == nil && internal.Unmap() == self.Unmap() && m.remoteHost == "" &&
		strings.EqualFold(m.protocol, "TCP") && m.internalPort == m.externalPort && m.internalPort != 0 &&
		m.description == mappingDescription && m.lease == 0
}

// Cleanup removes unlimited UPnP TCP forwards labelled "croc relay" for this
// computer. It never creates a mapping and leaves listening ports alone.
func Cleanup(ctx context.Context) (CleanupResult, error) {
	_, self, ok := netmon.LikelyHomeRouterIP()
	if !ok || !self.Is4() {
		return CleanupResult{}, ErrNoGateway
	}
	m, err := discoverUPnP(ctx, self)
	if err != nil {
		return CleanupResult{}, err
	}
	return cleanupUPnP(ctx, m.client, self, func(port uint16) (func(), error) {
		listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IP(self.AsSlice()), Port: int(port)})
		if err != nil {
			return nil, err
		}
		return func() { listener.Close() }, nil
	})
}

func cleanupUPnP(ctx context.Context, client igdClient, self netip.Addr, reserve func(uint16) (func(), error)) (result CleanupResult, err error) {
	reader, ok := client.(igdMappingReader)
	if !ok {
		return result, errors.New("upnp: router cannot list its port forwards")
	}
	// Take a snapshot before deleting: removing a rule shifts later indices.
	var entries []mappingEntry
	for index := 0; ; index++ {
		if index > 65535 {
			return result, errors.New("upnp: too many port forwards")
		}
		var entry mappingEntry
		entry.remoteHost, entry.externalPort, entry.protocol, entry.internalPort, entry.internalClient,
			_, entry.description, entry.lease, err = reader.GetGenericPortMappingEntryCtx(ctx, uint16(index))
		if mappingMissing(err) {
			break
		}
		if err != nil {
			return result, fmt.Errorf("upnp: list port forwards: %w", err)
		}
		if entry.ownedUnlimited(self) {
			entries = append(entries, entry)
		}
	}
	var errs []error
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return result, errors.Join(append(errs, err)...)
		}
		release, err := reserve(entry.internalPort)
		if err != nil {
			result.Skipped = append(result.Skipped, entry.externalPort)
			continue
		}
		func() {
			defer release()
			current := entry
			current.internalPort, current.internalClient, _, current.description, current.lease, err =
				reader.GetSpecificPortMappingEntryCtx(ctx, entry.remoteHost, entry.externalPort, entry.protocol)
			if mappingMissing(err) {
				return
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("upnp: inspect TCP port %d: %w", entry.externalPort, err))
				return
			}
			if !current.ownedUnlimited(self) {
				result.Skipped = append(result.Skipped, entry.externalPort)
				return
			}
			if err := client.DeletePortMappingCtx(ctx, entry.remoteHost, entry.externalPort, entry.protocol); err != nil && !mappingMissing(err) {
				errs = append(errs, fmt.Errorf("upnp: remove TCP port %d: %w", entry.externalPort, err))
				return
			}
			result.Removed = append(result.Removed, entry.externalPort)
		}()
	}
	return result, errors.Join(errs...)
}

func mappingMissing(err error) bool {
	fault, ok := errors.AsType[*soap.SOAPFaultError](err)
	return ok && (fault.Detail.UPnPError.Errorcode == 713 || fault.Detail.UPnPError.Errorcode == 714)
}

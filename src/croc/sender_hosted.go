package croc

import (
	"context"
	"fmt"
	"net/netip"
	"strconv"
	"time"

	log "github.com/schollz/croc/v11/src/logger"
	"github.com/schollz/croc/v11/src/portmap"
)

const (
	// senderRelayExposeTimeout bounds gateway port mapping before the code is
	// shown. It covers each protocol's budget in src/portmap: 1.5 s for PCP and
	// NAT-PMP each, and 3 s for UPnP discovery.
	senderRelayExposeTimeout = 6 * time.Second
	// senderHostedDialTimeout bounds the receiver's attempt to reach the
	// sender's relay before it falls back to the public relay.
	senderHostedDialTimeout = 5 * time.Second
)

// exposeSenderRelay makes the sender's local relay reachable from the internet
// for --transport direct, so a receiver can meet the sender with no public
// relay, DERP, or STUN. It starts the local relay early and returns the address
// to put in the code. When the relay cannot be exposed it returns an invalid
// address and the transfer falls back to the public relay and Tailcat's direct
// path.
func (c *Client) exposeSenderRelay() (netip.AddrPort, func(), error) {
	noop := func() {}
	if c.Options.Transport != TransportDirect || c.Options.DisableLocal || c.Options.OnlyLocal {
		return netip.AddrPort{}, noop, nil
	}
	if err := c.setupLocalRelay(); err != nil {
		return netip.AddrPort{}, noop, fmt.Errorf("start local relay: %w", err)
	}
	ports := make([]uint16, 0, len(c.Options.RelayPorts))
	for _, port := range c.Options.RelayPorts {
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			return netip.AddrPort{}, noop, fmt.Errorf("invalid local relay port %q: %w", port, err)
		}
		ports = append(ports, uint16(n))
	}
	expose := c.exposeRelayPorts
	if expose == nil {
		expose = exposeRelayPorts
	}
	addr, closeExposure, err := expose(c.stop.ctx, ports)
	if err != nil {
		log.Debugf("sender-hosted relay unavailable; the receiver will meet through the public relay: %v", err)
		return netip.AddrPort{}, noop, nil
	}
	log.Debugf("sender-hosted relay reachable at %v", addr)
	return addr, closeExposure, nil
}

// exposeRelayPorts maps ports on the gateway (PCP, NAT-PMP, or UPnP), or falls
// back to a global IPv6 address, and returns the address that reaches the
// first port.
func exposeRelayPorts(ctx context.Context, ports []uint16) (netip.AddrPort, func(), error) {
	mapCtx, cancel := context.WithTimeout(ctx, senderRelayExposeTimeout)
	defer cancel()
	set, err := portmap.Map(mapCtx, portmap.Config{Ports: ports, Logf: log.Debugf})
	if err == nil {
		return netip.AddrPortFrom(set.Address(), ports[0]), func() {
			if closeErr := set.Close(); closeErr != nil {
				log.Debugf("removing relay port mappings: %v", closeErr)
			}
		}, nil
	}
	if ipv6, ok := portmap.GlobalIPv6(); ok {
		log.Debugf("no gateway port mapping (%v); using global IPv6 address %v", err, ipv6)
		return netip.AddrPortFrom(ipv6, ports[0]), func() {}, nil
	}
	return netip.AddrPort{}, nil, err
}

// connectSenderHostedRelay dials the relay address embedded in the code.
func (c *Client) connectSenderHostedRelay() (relayControlResult, error) {
	ctx, cancel := context.WithTimeout(c.stop.ctx, senderHostedDialTimeout)
	defer cancel()
	return c.connectRelayControlContext(ctx, c.senderRelayAddress.String())
}

// directViaSenderRelay reports whether a --transport direct transfer reached
// its peer through the sender's own relay, over the LAN or a sender-hosted
// public address. Relay data channels then carry no third party, so Tailcat is
// not needed.
func (c *Client) directViaSenderRelay() bool {
	if c.Options.Transport != TransportDirect {
		return false
	}
	address, _ := c.currentRelayControlRoute()
	return c.relayControlRouteIsPeerToPeer(address)
}

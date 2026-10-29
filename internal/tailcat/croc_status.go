// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package tailcat

import "tailscale.com/ipn/ipnstate"

// Status returns the client's current WireGuard and magicsock status.
// It returns nil until the client has initialized its networking stack.
func (c *Client) Status() *ipnstate.Status {
	c.startMu.Lock()
	defer c.startMu.Unlock()
	if c.lb == nil {
		return nil
	}
	return c.lb.Status()
}

// PeerStatus is like Status but includes per-peer path details such as
// CurAddr and Relay. Status omits peers because its builder does not set
// WantPeers.
func (c *Client) PeerStatus() *ipnstate.Status {
	c.startMu.Lock()
	defer c.startMu.Unlock()
	if c.lb == nil {
		return nil
	}
	return c.lb.peerStatus()
}

// PeerStatus is the server-side equivalent of [Client.PeerStatus].
func (s *Server) PeerStatus() *ipnstate.Status {
	if s.lb == nil {
		return nil
	}
	return s.lb.peerStatus()
}

func (b *locoBackend) peerStatus() *ipnstate.Status {
	sb := ipnstate.StatusBuilder{WantPeers: true}
	b.sys.MagicSock.Get().UpdateStatus(&sb)
	b.sys.Engine.Get().UpdateStatus(&sb)
	return sb.Status()
}

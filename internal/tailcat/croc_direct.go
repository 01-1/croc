// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause

package tailcat

import "errors"

// DisableDERP closes the server's DERP connections and stops magicsock from
// using DERP again. Once a peer has a direct UDP path, croc calls this so no
// WireGuard packet can fall back to a relay: if the direct path is lost,
// packets are dropped instead of relayed. Tailcat sets its DERP map only at
// startup, so nothing restores it afterwards.
func (s *Server) DisableDERP() error {
	if s.lb == nil {
		return errors.New("tailcat: server not started")
	}
	s.lb.disableDERP()
	return nil
}

// DisableDERP is the client-side equivalent of [Server.DisableDERP].
func (c *Client) DisableDERP() error {
	c.startMu.Lock()
	defer c.startMu.Unlock()
	if c.lb == nil {
		return errors.New("tailcat: client not started")
	}
	c.lb.disableDERP()
	return nil
}

func (b *locoBackend) disableDERP() {
	b.sys.MagicSock.Get().SetDERPMap(nil)
}

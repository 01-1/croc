//go:build !croc_no_tailcat && (linux || windows || darwin || freebsd || openbsd) && !ts_omit_portmapper

package tailcattransport

import (
	"testing"

	"tailscale.com/net/portmapper/portmappertype"
)

// internal/tailcat's own tests link the portmapper through their test-only
// imports, so this guard lives in a package that only reaches it through
// the production Tailcat import.
func TestPortMapperLinked(t *testing.T) {
	if !portmappertype.HookNewPortMapper.IsSet() {
		t.Fatal("portmapper hook is not registered; magicsock will never request NAT-PMP, PCP, or UPnP mappings")
	}
}

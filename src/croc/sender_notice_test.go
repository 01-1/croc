package croc

import (
	"net/netip"
	"strings"
	"testing"
)

func TestSenderNoticesPersistInTerminalOutput(t *testing.T) {
	notice := formatUnlimitedPortForwardNotice([]uint16{9009, 9010})
	for _, required := range []string{"unlimited", "9009", "9010", "Normal shutdown removes them", "crash", "indefinitely", "croc cleanup-ports"} {
		if !strings.Contains(notice, required) {
			t.Fatalf("notice is missing %q: %q", required, notice)
		}
	}
	address := netip.MustParseAddrPort("203.0.113.7:9009")
	privacy := formatHostedInvitationNotice(address)
	if !strings.Contains(privacy, "anyone who sees this code") || !strings.Contains(privacy, address.Addr().String()) {
		t.Fatalf("hosted invitation notice = %q", privacy)
	}
	for _, text := range []string{notice, privacy} {
		if strings.ContainsAny(text, "\r\x1b\a") || !strings.HasSuffix(text, "\n\n") {
			t.Fatalf("notice can be cleared or overwritten: %q", text)
		}
	}
	if got := formatHostedInvitationNotice(netip.AddrPort{}); got != "" {
		t.Fatalf("non-hosted invitation has a public-IP notice: %q", got)
	}
}

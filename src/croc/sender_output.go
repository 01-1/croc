package croc

import (
	"fmt"
	"net/netip"
	"strings"

	"github.com/schollz/croc/v11/src/termui"
)

func formatUnlimitedPortForwardNotice(ports []uint16) string {
	return fmt.Sprintf("\nRouter port forwarding: croc is requesting unlimited TCP forwards for ports %v.\n"+
		"Normal shutdown removes them. A crash can leave them behind indefinitely.\n"+
		"After croc has stopped, run: croc cleanup-ports\n\n", ports)
}

func formatHostedInvitationNotice(address netip.AddrPort) string {
	if !address.IsValid() {
		return ""
	}
	return fmt.Sprintf("Hosted invitation: anyone who sees this code can learn your public IP address (%s).\n\n", address.Addr())
}

const (
	secretColorPrefix = termui.Yellow
	colorReset        = termui.Reset
)

func colorSecret(secret string, enabled bool) string {
	if !enabled {
		return secret
	}
	return termui.Secret(secret, true)
}

func formatSendInstructions(secret, flags, webURL, clipboardNotice string, colorEnabled bool) string {
	if clipboardNotice != "" {
		clipboardNotice = " (" + clipboardNotice + ")"
	}
	instructions := fmt.Sprintf(`On the other computer, run:
  croc %s%s%s
`, flags, colorSecret(secret, colorEnabled), clipboardNotice)
	if webURL != "" {
		instructions += fmt.Sprintf(`
Or open:
  %s
`, webURL)
	}
	return instructions
}

func formatClipboardText(secret, flags string, extended bool) string {
	if !extended {
		return secret
	}
	return fmt.Sprintf("CROC_SECRET=%q croc %s", secret, strings.TrimSpace(flags))
}

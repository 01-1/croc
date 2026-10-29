package codephrase

import (
	"net/netip"
	"strings"
	"testing"
)

func TestSenderAddressRoundTrip(t *testing.T) {
	tests := []struct {
		addr  string
		words int
	}{
		{addr: "203.0.113.7:9009", words: 3},
		{addr: "203.0.113.7:40123", words: 5},
		{addr: "[2001:db8::7]:9009", words: 12},
		{addr: "[2001:db8::7]:9010", words: 14},
	}
	for _, test := range tests {
		t.Run(test.addr, func(t *testing.T) {
			addr := netip.MustParseAddrPort(test.addr)
			code := WithSenderAddress("bacon-pickle-trout", addr)
			plain, suffix, _ := strings.Cut(code, "@")
			if plain != "bacon-pickle-trout" || len(strings.Split(suffix, "-")) != test.words {
				t.Fatalf("WithSenderAddress = %q; want %d address words", code, test.words)
			}
			gotPlain, gotAddr, ok, err := SplitSenderAddress(code)
			if err != nil || !ok || gotPlain != "bacon-pickle-trout" || gotAddr != addr {
				t.Fatalf("SplitSenderAddress(%q) = (%q, %v, %t, %v)", code, gotPlain, gotAddr, ok, err)
			}
			components, err := Parse(gotPlain)
			if err != nil || components.Format != FormatThreeWord {
				t.Fatalf("plain part parsed as %+v, %v", components, err)
			}
		})
	}
}

func TestSenderAddressRoundTripWithCustomCode(t *testing.T) {
	addr := netip.MustParseAddrPort("203.0.113.7:9009")
	code := WithSenderAddress("me@example-dot-com", addr)
	plain, gotAddr, ok, err := SplitSenderAddress(code)
	if err != nil || !ok || plain != "me@example-dot-com" || gotAddr != addr {
		t.Fatalf("SplitSenderAddress(%q) = (%q, %v, %t, %v)", code, plain, gotAddr, ok, err)
	}
}

func TestSplitSenderAddressLeavesOtherCodesUnchanged(t *testing.T) {
	for _, code := range []string{
		"bacon-pickle-trout",
		"1234-my-custom-code",
		"me@example-dot-com",
		"@leading-separator",
		"trailing-separator@",
	} {
		plain, addr, ok, err := SplitSenderAddress(code)
		if err != nil || ok || plain != code || addr.IsValid() {
			t.Fatalf("SplitSenderAddress(%q) = (%q, %v, %t, %v); want the code unchanged", code, plain, addr, ok, err)
		}
	}
}

func TestSplitSenderAddressRejectsCorruptAddresses(t *testing.T) {
	valid := WithSenderAddress("bacon-pickle-trout", netip.MustParseAddrPort("203.0.113.7:9009"))
	words := strings.Split(strings.SplitN(valid, "@", 2)[1], "-")
	tests := map[string]string{
		"wrong length": "bacon-pickle-trout@" + strings.Join(words[:2], "-"),
		"unspecified":  WithSenderAddress("bacon-pickle-trout", netip.MustParseAddrPort("0.0.0.0:9009")),
		"multicast":    WithSenderAddress("bacon-pickle-trout", netip.MustParseAddrPort("224.0.0.1:9009")),
		"port zero":    WithSenderAddress("bacon-pickle-trout", netip.MustParseAddrPort("203.0.113.7:0")),
	}
	for name, code := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, _, err := SplitSenderAddress(code); err == nil {
				t.Fatalf("SplitSenderAddress(%q) accepted a corrupt address", code)
			}
		})
	}
}

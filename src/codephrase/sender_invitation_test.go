package codephrase

import (
	"encoding/binary"
	"math/rand/v2"
	"net/netip"
	"strings"
	"testing"
)

func TestNumberWordInvitationPreservesSecretAndAddress(t *testing.T) {
	for _, secret := range []string{strings.Join(effWords[:3], "-"), strings.Join([]string{effWords[effWordCount-1], effWords[effWordCount-1], effWords[effWordCount-1]}, "-"), "yo-yo-acid-acorn", "My-Custom@Secret", "123456", "me@example-dot-com"} {
		for _, value := range []string{"203.0.113.7:9009", "203.0.113.7:40123", "[2001:db8::7]:9009", "[2001:db8::7]:40123", "[2620:9b:1234:5678:9012:3456:789a:bcde]:9009"} {
			address := netip.MustParseAddrPort(value)
			invitation := WithSenderAddress(secret, address)
			parts := strings.FieldsFunc(invitation, func(c rune) bool { return c == '-' || c == '@' })
			for i, part := range parts {
				if i%2 == 0 {
					if len(part) != 2 || !strings.ContainsRune(invitationDigits, rune(part[0])) || !strings.ContainsRune(invitationDigits, rune(part[1])) {
						t.Fatalf("invalid number %q in %q", part, invitation)
					}
				} else if _, ok := invitationWordIndices[part]; !ok {
					t.Fatalf("invalid word %q in %q", part, invitation)
				}
			}
			for _, text := range []string{invitation, strings.ToUpper(invitation)} {
				plain, got, ok, err := SplitSenderAddress(text)
				if err != nil || !ok || plain != secret || got != address {
					t.Fatalf("SplitSenderAddress(%q) = (%q, %v, %t, %v), want (%q, %v)", text, plain, got, ok, err, secret, address)
				}
			}
		}
	}
}

func TestNumberWordInvitationCompactsGeneratedSecret(t *testing.T) {
	secret := strings.Join(effWords[:3], "-")
	invitation := WithSenderAddress(secret, netip.MustParseAddrPort("203.0.113.7:9009"))
	parts := strings.FieldsFunc(invitation, func(c rune) bool { return c == '-' || c == '@' })
	if len(parts) != 8 {
		t.Fatalf("generated secret plus IPv4 needs four number-word pairs: %q", invitation)
	}
	zeroFilled := encodeInvitationAddress(netip.MustParseAddrPort("[2001:db8::7]:9009"))
	if strings.Count(zeroFilled, "-") >= 15 {
		t.Fatalf("IPv6 zero groups were not compressed: %q", zeroFilled)
	}
}

func TestNumberWordInvitationManyAddresses(t *testing.T) {
	random := rand.New(rand.NewPCG(1, 2))
	secret := strings.Join(effWords[:3], "-")
	for range 256 {
		var data [16]byte
		for i := range data {
			data[i] = byte(random.Uint32())
		}
		data[0] = 0x20
		for _, port := range []uint16{DefaultSenderRelayPort, uint16(random.Uint32()%65535 + 1)} {
			for _, address := range []netip.Addr{netip.AddrFrom16(data), netip.AddrFrom4([4]byte{203, data[1], data[2], data[3]})} {
				want := netip.AddrPortFrom(address, port)
				_, got, ok, err := SplitSenderAddress(WithSenderAddress(secret, want))
				if err != nil || !ok || got != want {
					t.Fatalf("round trip %v = %v, %t, %v", want, got, ok, err)
				}
			}
		}
	}
}

func TestNumberWordIPv6EveryZeroGroupPattern(t *testing.T) {
	secret := strings.Join(effWords[:3], "-")
	for mask := 0x80; mask <= 0xff; mask++ {
		var data [16]byte
		binary.BigEndian.PutUint16(data[:], 0x2001)
		for group := 1; group < 8; group++ {
			if mask&(1<<(7-group)) != 0 {
				binary.BigEndian.PutUint16(data[group*2:], uint16(group+16))
			}
		}
		for _, port := range []uint16{DefaultSenderRelayPort, 40123} {
			want := netip.AddrPortFrom(netip.AddrFrom16(data), port)
			_, got, ok, err := SplitSenderAddress(WithSenderAddress(secret, want))
			if err != nil || !ok || got != want {
				t.Fatalf("mask %x: round trip %v = %v, %t, %v", mask, want, got, ok, err)
			}
		}
	}
}

func TestNumberWordInvitationRejectsCorruption(t *testing.T) {
	valid := WithSenderAddress(strings.Join(effWords[:3], "-"), netip.MustParseAddrPort("203.0.113.7:9009"))
	secret, address, _ := strings.Cut(valid, "@")
	parts := strings.Split(address, "-")
	for _, number := range []string{"00", "11", "2", "222", "OO", "I2"} {
		parts[0] = number
		if _, err := decodeInvitationAddress(strings.Join(parts, "-")); err == nil {
			t.Fatalf("accepted invalid number %q", number)
		}
	}
	for _, corrupt := range []string{secret + "@22-notaword-22-notaword", "23-notaword-23-notaword@" + address, secret + "@" + address + "-22-notaword"} {
		if _, _, _, err := SplitSenderAddress(corrupt); err == nil {
			t.Fatalf("accepted corrupt invitation %q", corrupt)
		}
	}
}

func TestNumberWordReceiverAcceptsExistingWordInvitations(t *testing.T) {
	plain, address, ok, err := SplitSenderAddress("bacon-pickle-trout@KILO-JAGUAR-ASIA")
	if err != nil || !ok || plain != "bacon-pickle-trout" || address != netip.MustParseAddrPort("203.0.113.7:9009") {
		t.Fatalf("legacy invitation = %q, %v, %t, %v", plain, address, ok, err)
	}
}

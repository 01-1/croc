package codephrase

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"strings"

	"github.com/schollz/croc/v11/src/mnemonicode"
)

// senderAddressSeparator joins a croc code to the sender's relay address.
// "@" is not part of generated codes and is safe in shells.
const senderAddressSeparator = "@"

// DefaultSenderRelayPort is omitted from sender addresses to keep codes short.
const DefaultSenderRelayPort = 9009

// WithSenderAddress encodes the secret and public address as alternating
// two-digit numbers and words. Decoding restores the original secret exactly.
func WithSenderAddress(code string, addr netip.AddrPort) string {
	return encodeInvitationSecret(code) + senderAddressSeparator + encodeInvitationAddress(addr)
}

// WithSenderAddressWords retains the spoken-word address format. IPv4 adds
// three words, or five with a non-default port; IPv6 adds twelve or fourteen.
func WithSenderAddressWords(code string, addr netip.AddrPort) string {
	ip := addr.Addr().Unmap().AsSlice()
	if addr.Port() != DefaultSenderRelayPort {
		ip = binary.BigEndian.AppendUint16(ip, addr.Port())
	}
	return code + senderAddressSeparator + strings.Join(mnemonicode.EncodeWordList(nil, ip), "-")
}

// SplitSenderAddress separates a code produced by WithSenderAddress into the
// plain code and the sender's relay address. It accepts compact and word-based
// addresses and splits at the last "@", because a custom code may contain "@"
// itself. Codes without a sender address, including custom codes that contain
// "@" followed by non-mnemonicode words, are returned unchanged with ok false.
func SplitSenderAddress(code string) (plain string, addr netip.AddrPort, ok bool, err error) {
	separator := strings.LastIndex(code, senderAddressSeparator)
	if separator <= 0 || separator == len(code)-len(senderAddressSeparator) {
		return code, netip.AddrPort{}, false, nil
	}
	plain, suffix := code[:separator], code[separator+len(senderAddressSeparator):]
	var data []byte
	first, _, _ := strings.Cut(suffix, "-")
	if looksNumberWordSecret(plain) && len(first) == 2 && first[0] >= '0' && first[0] <= '9' && first[1] >= '0' && first[1] <= '9' {
		data, err = decodeInvitationAddress(suffix)
		if err == nil {
			plain, err = decodeInvitationSecret(plain)
		}
	} else {
		data, err = mnemonicode.DecodeWordList(nil, strings.Split(suffix, "-"))
	}
	if err != nil {
		if errors.Is(err, mnemonicode.ErrUnknownWord) {
			return code, netip.AddrPort{}, false, nil
		}
		return "", netip.AddrPort{}, false, fmt.Errorf("invalid sender address in code: %w", err)
	}
	port := uint16(DefaultSenderRelayPort)
	switch len(data) {
	case 4, 16:
	case 6, 18:
		port = binary.BigEndian.Uint16(data[len(data)-2:])
		data = data[:len(data)-2]
	default:
		return "", netip.AddrPort{}, false, fmt.Errorf("invalid sender address in code: %d bytes", len(data))
	}
	ip, _ := netip.AddrFromSlice(data)
	if port == 0 || !ip.Unmap().IsGlobalUnicast() {
		return "", netip.AddrPort{}, false, fmt.Errorf("invalid sender address in code: %v", netip.AddrPortFrom(ip, port))
	}
	return plain, netip.AddrPortFrom(ip.Unmap(), port), true, nil
}

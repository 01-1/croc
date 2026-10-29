package codephrase

import (
	"encoding/binary"
	"errors"
	"math/big"
	"math/bits"
	"net/netip"
	"slices"
	"strings"

	"github.com/schollz/croc/v11/src/mnemonicode"
)

const (
	// Each number has two digits from this alphabet, avoiding 0/O and 1/I/L.
	invitationDigits   = "23456789"
	invitationWordBase = 1024
	invitationPairBase = 64 * invitationWordBase
)

var errInvitation = errors.New("invalid number-and-word invitation")

var invitationWords = func() []string {
	words := append([]string(nil), mnemonicode.WordList[:1626]...)
	slices.SortFunc(words, func(a, b string) int {
		if len(a) != len(b) {
			return len(a) - len(b)
		}
		return strings.Compare(a, b)
	})
	return words[:invitationWordBase]
}()

var invitationWordIndices = func() map[string]int {
	indices := make(map[string]int, invitationWordBase)
	for index, word := range invitationWords {
		indices[word] = index
	}
	return indices
}()

func encodeNumberWords(value *big.Int, minimumPairs int) string {
	number := new(big.Int).Set(value)
	radix := big.NewInt(invitationPairBase)
	var pairs []string
	for number.Sign() > 0 || len(pairs) < minimumPairs {
		var remainder big.Int
		number.QuoRem(number, radix, &remainder)
		index := remainder.Int64()
		digits := index / invitationWordBase
		pair := string([]byte{invitationDigits[digits/8], invitationDigits[digits%8]}) + "-" + invitationWords[index%invitationWordBase]
		pairs = append(pairs, pair)
	}
	slices.Reverse(pairs)
	return strings.Join(pairs, "-")
}

func decodeNumberWords(text string, maxPairs int) (*big.Int, int, error) {
	if maxPairs > 0 && strings.Count(text, "-") >= 2*maxPairs {
		return nil, 0, errInvitation
	}
	parts := strings.Split(text, "-")
	if len(parts) == 0 || len(parts)%2 != 0 {
		return nil, 0, errInvitation
	}
	value := new(big.Int)
	radix := big.NewInt(invitationPairBase)
	for i := 0; i < len(parts); i += 2 {
		if len(parts[i]) != 2 {
			return nil, 0, errInvitation
		}
		tens := strings.IndexByte(invitationDigits, parts[i][0])
		ones := strings.IndexByte(invitationDigits, parts[i][1])
		if tens < 0 || ones < 0 {
			return nil, 0, errInvitation
		}
		for _, c := range []byte(parts[i+1]) {
			if c > 127 {
				return nil, 0, errInvitation
			}
		}
		word, ok := invitationWordIndices[strings.ToLower(parts[i+1])]
		if !ok {
			return nil, 0, errInvitation
		}
		value.Mul(value, radix)
		value.Add(value, big.NewInt(int64((tens*8+ones)*invitationWordBase+word)))
	}
	return value, len(parts) / 2, nil
}

func encodeInvitationSecret(secret string) string {
	if words, ok := threeEFFWords(secret); ok {
		value := int64(0)
		for _, word := range words {
			index := slices.Index(effWords, word)
			value = value*effWordCount + int64(index)
		}
		return encodeNumberWords(big.NewInt(value), 2)
	}
	// A leading byte preserves arbitrary custom codes, including their case.
	data := append([]byte{1}, []byte(secret)...)
	return encodeNumberWords(new(big.Int).SetBytes(data), 3)
}

func decodeInvitationSecret(text string) (string, error) {
	value, pairs, err := decodeNumberWords(text, 0)
	if err != nil || pairs < 2 {
		return "", errInvitation
	}
	if pairs == 2 {
		if value.Cmp(big.NewInt(effWordCount*effWordCount*effWordCount)) >= 0 {
			return "", errInvitation
		}
		words := make([]string, 3)
		for i := 2; i >= 0; i-- {
			var remainder big.Int
			value.QuoRem(value, big.NewInt(effWordCount), &remainder)
			words[i] = effWords[remainder.Int64()]
		}
		return strings.Join(words, "-"), nil
	}
	data := value.Bytes()
	if len(data) < 7 || data[0] != 1 {
		return "", errInvitation
	}
	secret := string(data[1:])
	if encodeInvitationSecret(secret) != strings.ToLower(text) {
		return "", errInvitation
	}
	return secret, nil
}

func encodeInvitationAddress(address netip.AddrPort) string {
	ip := address.Addr().Unmap()
	data := ip.AsSlice()
	if address.Port() != DefaultSenderRelayPort {
		data = binary.BigEndian.AppendUint16(data, address.Port())
	}
	value := new(big.Int).SetBytes(data)
	text := encodeNumberWords(value, len(data)/2)
	if ip.Is6() {
		packed := []byte{0}
		bytes := ip.As16()
		for group := range 8 {
			groupValue := binary.BigEndian.Uint16(bytes[group*2:])
			if groupValue != 0 {
				packed[0] |= 1 << (7 - group)
				packed = binary.BigEndian.AppendUint16(packed, groupValue)
			}
		}
		if address.Port() != DefaultSenderRelayPort {
			packed = binary.BigEndian.AppendUint16(packed, address.Port())
		}
		compressed := new(big.Int).SetBytes(packed)
		compressedText := encodeNumberWords(compressed, 4)
		// Two, three, eight and nine pairs identify uncompressed addresses.
		// Four through seven pairs identify the compressed IPv6 form.
		if strings.Count(compressedText, "-") < 15 && len(compressedText) < len(text) {
			text = compressedText
		}
	}
	return text
}

func decodeInvitationAddress(text string) ([]byte, error) {
	value, pairs, err := decodeNumberWords(text, 9)
	if err != nil {
		return nil, err
	}
	switch pairs {
	case 2:
		return invitationBytes(value, 4)
	case 3:
		return invitationBytes(value, 6)
	case 8:
		return invitationBytes(value, 16)
	case 9:
		return invitationBytes(value, 18)
	case 4, 5, 6, 7:
		if value.BitLen() > 14*8 {
			return nil, errInvitation
		}
		if encodeNumberWords(value, 4) != strings.ToLower(text) {
			return nil, errInvitation
		}
		packed := value.Bytes()
		if len(packed) == 0 || packed[0] == 0 {
			return nil, errInvitation
		}
		length := 1 + 2*bits.OnesCount8(packed[0])
		if len(packed) != length && len(packed) != length+2 {
			return nil, errInvitation
		}
		data := make([]byte, 16)
		offset := 1
		for group := range 8 {
			if packed[0]&(1<<(7-group)) != 0 {
				if binary.BigEndian.Uint16(packed[offset:]) == 0 {
					return nil, errInvitation
				}
				copy(data[group*2:], packed[offset:offset+2])
				offset += 2
			}
		}
		return append(data, packed[length:]...), nil
	}
	return nil, errInvitation
}

func invitationBytes(value *big.Int, size int) ([]byte, error) {
	if value.BitLen() > size*8 {
		return nil, errInvitation
	}
	data := make([]byte, size)
	return value.FillBytes(data), nil
}

package mnemonicode

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrUnknownWord is returned when a word is not in WordList.
	ErrUnknownWord = errors.New("mnemonicode: unknown word")
	// ErrInvalidEncoding is returned when known words do not form an encoding
	// that EncodeWordList could have produced.
	ErrInvalidEncoding = errors.New("mnemonicode: invalid encoding")
)

var wordIndex = func() map[string]int {
	index := make(map[string]int, len(WordList))
	for i, word := range WordList {
		index[word] = i
	}
	return index
}()

// DecodeWordList decodes words produced by EncodeWordList and appends the
// bytes to dst. Words are matched case-insensitively. Like mn_decode in the
// original C implementation, it rejects unknown words (ErrUnknownWord), and
// returns ErrInvalidEncoding for a short remainder word
// anywhere but the end of a three-byte remainder, and groups whose value does
// not fit the bytes they encode.
func DecodeWordList(dst []byte, words []string) ([]byte, error) {
	indices := make([]int, len(words))
	for i, word := range words {
		index, ok := wordIndex[strings.ToLower(word)]
		if !ok {
			return nil, fmt.Errorf("%w %q", ErrUnknownWord, word)
		}
		indices[i] = index
	}
	for len(indices) > 0 {
		group := indices[:min(3, len(indices))]
		indices = indices[len(group):]
		var x uint64
		multiplier := uint64(1)
		size := 0
		for i, index := range group {
			short := index >= base
			if short && (i != 2 || len(indices) != 0) {
				return nil, fmt.Errorf("%w: unexpected remainder word", ErrInvalidEncoding)
			}
			if short {
				index -= base
			}
			x += uint64(index) * multiplier
			multiplier *= base
			switch {
			case i == 2 && short:
				size = 3
			case i == 2:
				size = 4
			default:
				size = i + 1
			}
		}
		if x >= 1<<(8*size) {
			return nil, fmt.Errorf("%w: value does not fit in %d bytes", ErrInvalidEncoding, size)
		}
		for range size {
			dst = append(dst, byte(x))
			x >>= 8
		}
	}
	return dst, nil
}

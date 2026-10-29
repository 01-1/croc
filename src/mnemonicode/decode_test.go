package mnemonicode

import (
	"bytes"
	"crypto/rand"
	"errors"
	"strings"
	"testing"
)

func TestDecodeWordListRoundTrips(t *testing.T) {
	for size := range 24 {
		for range 64 {
			src := make([]byte, size)
			if _, err := rand.Read(src); err != nil {
				t.Fatal(err)
			}
			words := EncodeWordList(nil, src)
			got, err := DecodeWordList(nil, words)
			if err != nil {
				t.Fatalf("decode %d bytes from %v: %v", size, words, err)
			}
			if !bytes.Equal(got, src) {
				t.Fatalf("decode(%v) = %x; want %x", words, got, src)
			}
		}
	}
}

func TestDecodeWordListEdgeValues(t *testing.T) {
	for _, src := range [][]byte{
		{0}, {0xff}, {0xff, 0xff}, {0xff, 0xff, 0xff}, {0xff, 0xff, 0xff, 0xff},
		{0, 0, 0, 0}, bytes.Repeat([]byte{0xff}, 7),
	} {
		words := EncodeWordList(nil, src)
		got, err := DecodeWordList(nil, words)
		if err != nil || !bytes.Equal(got, src) {
			t.Fatalf("decode(%v) = (%x, %v); want %x", words, got, err, src)
		}
	}
	upper := EncodeWordList(nil, []byte{1, 2, 3, 4})
	for i := range upper {
		upper[i] = strings.ToUpper(upper[i])
	}
	if got, err := DecodeWordList(nil, upper); err != nil || !bytes.Equal(got, []byte{1, 2, 3, 4}) {
		t.Fatalf("case-insensitive decode = (%x, %v)", got, err)
	}
}

func TestDecodeWordListRejectsInvalidEncodings(t *testing.T) {
	short := WordList[base]
	last := WordList[base-1]
	if _, err := DecodeWordList(nil, []string{"not-a-mnemonicode-word"}); !errors.Is(err, ErrUnknownWord) {
		t.Fatalf("unknown word error = %v; want ErrUnknownWord", err)
	}
	tests := map[string][]string{
		"short word first":         {short},
		"short word not last":      {WordList[0], WordList[0], short, WordList[0]},
		"one word over a byte":     {last},
		"two words over 16 bits":   {last, last},
		"three words over 32 bits": {last, last, last},
	}
	for name, words := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeWordList(nil, words); !errors.Is(err, ErrInvalidEncoding) {
				t.Fatalf("decode(%v) error = %v; want ErrInvalidEncoding", words, err)
			}
		})
	}
}

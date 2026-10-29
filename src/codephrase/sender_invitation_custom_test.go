package codephrase

import "testing"

func TestPlainCustomCodesWithNumericSuffixRemainUnchanged(t *testing.T) {
	for _, code := range []string{"custom@01-password", "custom@23-notaword", "my-normal-code@22-bad", "1234-foo@23-domain"} {
		plain, address, ok, err := SplitSenderAddress(code)
		if err != nil || ok || plain != code || address.IsValid() {
			t.Fatalf("custom code %q changed: plain=%q address=%v ok=%t error=%v", code, plain, address, ok, err)
		}
	}
}

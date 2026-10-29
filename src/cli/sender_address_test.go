package cli

import (
	"testing"

	"github.com/schollz/croc/v11/internal/cli"
	"github.com/schollz/croc/v11/src/croc"
)

func TestAddressWordsRemembersAndAllowsExplicitCompactOverride(t *testing.T) {
	for _, tt := range []struct {
		name  string
		flags []string
		want  bool
	}{
		{"remembered words", nil, true},
		{"explicit compact", []string{"--address-words=false"}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			app := newApp()
			for _, command := range app.Commands {
				if command.Name == "send" {
					command.Action = func(c *cli.Context) error {
						options := croc.Options{HostedAddressWords: c.Bool("address-words")}
						applyRememberedSendOptions(c, &options, croc.Options{HostedAddressWords: true})
						if options.HostedAddressWords != tt.want {
							t.Fatalf("address words = %t, want %t", options.HostedAddressWords, tt.want)
						}
						return nil
					}
				}
			}
			if err := app.Run(append([]string{"croc", "send"}, tt.flags...)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAddressWordsRejectsNonDirectTransport(t *testing.T) {
	t.Setenv("CROC_CONFIG_DIR", t.TempDir())
	t.Setenv("CROC_DO_CHECK", "0")
	err := newApp().Run([]string{"croc", "--ignore-stdin", "send", "--address-words"})
	if err == nil || err.Error() != "--address-words only applies to --transport direct" {
		t.Fatalf("unexpected error: %v", err)
	}
}

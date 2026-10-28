package croc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/codephrase"
	log "github.com/schollz/croc/v11/src/logger"
)

func TestReceiverCodeWithSenderAddress(t *testing.T) {
	addr := netip.MustParseAddrPort("203.0.113.7:9009")
	code := codephrase.WithSenderAddress("bacon-pickle-trout", addr)
	receiver, err := newTestClient(Options{SharedSecret: code, Curve: "p256"})
	if err != nil {
		t.Fatal(err)
	}
	plain, err := newTestClient(Options{SharedSecret: "bacon-pickle-trout", Curve: "p256"})
	if err != nil {
		t.Fatal(err)
	}
	if receiver.senderRelayAddress != addr || receiver.Options.SharedSecret != "bacon-pickle-trout" {
		t.Fatalf("receiver parsed %v and %q", receiver.senderRelayAddress, receiver.Options.SharedSecret)
	}
	if receiver.Options.RoomName != plain.Options.RoomName || receiver.pakePassphrase != plain.pakePassphrase {
		t.Fatal("a code with a sender address must use the same room and PAKE password as its plain code")
	}
	if redacted := receiver.redactError(errors.New("failed with " + code)).Error(); strings.Contains(redacted, "@") {
		t.Fatalf("sender address was not redacted: %q", redacted)
	}
	if _, err := newTestClient(Options{IsSender: true, SharedSecret: code, Curve: "p256"}); err == nil {
		t.Fatal("a sender accepted a code that already includes a sender address")
	}
}

func newExposureTestSender(t *testing.T, mode TransportMode) *Client {
	t.Helper()
	sender, err := newClient(Options{
		IsSender:      true,
		SharedSecret:  fmt.Sprintf("expose-%d", time.Now().UnixNano()),
		Transport:     mode,
		RelayPorts:    freeConsecutiveTestPorts(t, 3),
		RelayPassword: "pass123",
		Curve:         "p256",
	}, &fakeTailcatTransport{available: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sender.stop.Cancel)
	return sender
}

func TestExposeSenderRelayOnlyForDirectTransport(t *testing.T) {
	auto := newExposureTestSender(t, TransportAuto)
	auto.exposeRelayPorts = func(context.Context, []uint16) (netip.AddrPort, func(), error) {
		t.Fatal("auto mode exposed the local relay")
		return netip.AddrPort{}, nil, nil
	}
	if addr, _, err := auto.exposeSenderRelay(); err != nil || addr.IsValid() || auto.localRelayPort != "" {
		t.Fatalf("auto exposure = (%v, %v), local relay %q", addr, err, auto.localRelayPort)
	}

	direct := newExposureTestSender(t, TransportDirect)
	var mapped []uint16
	closed := false
	direct.exposeRelayPorts = func(_ context.Context, ports []uint16) (netip.AddrPort, func(), error) {
		mapped = ports
		return netip.AddrPortFrom(netip.MustParseAddr("203.0.113.7"), ports[0]), func() { closed = true }, nil
	}
	addr, closeExposure, err := direct.exposeSenderRelay()
	if err != nil {
		t.Fatal(err)
	}
	if direct.localRelayPort == "" || len(mapped) != len(direct.Options.RelayPorts) {
		t.Fatalf("mapped %v for local relay ports %v", mapped, direct.Options.RelayPorts)
	}
	for i, port := range direct.Options.RelayPorts {
		if strconv.Itoa(int(mapped[i])) != port {
			t.Fatalf("mapped %v for local relay ports %v", mapped, direct.Options.RelayPorts)
		}
	}
	if addr.Port() != mapped[0] {
		t.Fatalf("exposed %v; want the control port %d", addr, mapped[0])
	}
	closeExposure()
	if !closed {
		t.Fatal("closing the exposure did not remove the mappings")
	}

	unmapped := newExposureTestSender(t, TransportDirect)
	unmapped.exposeRelayPorts = func(context.Context, []uint16) (netip.AddrPort, func(), error) {
		return netip.AddrPort{}, nil, errors.New("gateway external address is not public")
	}
	if addr, closeExposure, err := unmapped.exposeSenderRelay(); err != nil || addr.IsValid() || closeExposure == nil {
		t.Fatalf("failed exposure = (%v, %v); want a plain code and a fallback", addr, err)
	}
}

func TestDirectTransportThroughSenderHostedRelay(t *testing.T) {
	log.SetLevel("warn")
	// Neither side can reach a public relay, so the transfer can only use
	// the sender's own relay.
	const unreachableRelay = "127.0.0.1:1"
	tempFile, cleanup := createTestFile(t, 256*1024)
	defer cleanup()
	receivedFile := filepath.Base(tempFile)
	defer os.Remove(receivedFile)

	secret := fmt.Sprintf("senderhosted-%d", time.Now().UnixNano())
	sender, err := newClient(Options{
		IsSender:         true,
		SharedSecret:     secret,
		RelayAddress:     unreachableRelay,
		RelayPorts:       freeConsecutiveTestPorts(t, 3),
		RelayPassword:    "pass123",
		NoPrompt:         true,
		Transport:        TransportDirect,
		Curve:            "siec",
		Overwrite:        true,
		NoCompress:       true,
		DisableClipboard: true,
	}, &fakeTailcatTransport{available: true})
	if err != nil {
		t.Fatal(err)
	}
	sender.senderRouteWaitOverride = 20 * time.Second
	hosted := make(chan netip.AddrPort, 1)
	sender.exposeRelayPorts = func(_ context.Context, ports []uint16) (netip.AddrPort, func(), error) {
		addr := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), ports[0])
		hosted <- addr
		return addr, func() {}, nil
	}
	receiver, err := newTestClient(Options{
		SharedSecret:  secret,
		RelayAddress:  unreachableRelay,
		RelayPassword: "pass123",
		NoPrompt:      true,
		DisableLocal:  true,
		Curve:         "siec",
		Overwrite:     true,
		NoCompress:    true,
	})
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 2)
	go func() {
		filesInfo, emptyFolders, totalNumberFolders, err := GetFilesInfo([]string{tempFile}, false, false, nil)
		if err != nil {
			results <- err
			return
		}
		results <- sender.Send(filesInfo, emptyFolders, totalNumberFolders)
	}()
	select {
	case receiver.senderRelayAddress = <-hosted:
	case <-time.After(10 * time.Second):
		t.Fatal("sender never exposed its relay")
	}
	go func() { results <- receiver.Receive() }()
	waitForTransferResults(t, results, 2, 30*time.Second, sender, receiver)

	for name, client := range map[string]*Client{"sender": sender, "receiver": receiver} {
		if client.selectedDataTransport.Load() != selectedTransportRelay {
			t.Fatalf("%s data transport = %d; want the sender's relay", name, client.selectedDataTransport.Load())
		}
	}
	want, _ := os.ReadFile(tempFile)
	got, err := os.ReadFile(receivedFile)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("received %d bytes (%v); want %d", len(got), err, len(want))
	}
}

func TestReceiverFallsBackWhenSenderHostedRelayIsUnreachable(t *testing.T) {
	log.SetLevel("warn")
	relay := startTestRelay(t, 4)
	tempFile, cleanup := createTestFile(t, 64*1024)
	defer cleanup()
	receivedFile := filepath.Base(tempFile)
	defer os.Remove(receivedFile)

	secret := fmt.Sprintf("senderhosted-fallback-%d", time.Now().UnixNano())
	sender, err := newTestClient(Options{
		IsSender:         true,
		SharedSecret:     secret,
		RelayAddress:     relay.address,
		RelayPorts:       append([]string(nil), relay.allPorts...),
		RelayPassword:    "pass123",
		NoPrompt:         true,
		DisableLocal:     true,
		Curve:            "siec",
		Overwrite:        true,
		NoCompress:       true,
		DisableClipboard: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := newTestClient(Options{
		SharedSecret:  secret,
		RelayAddress:  relay.address,
		RelayPassword: "pass123",
		NoPrompt:      true,
		DisableLocal:  true,
		Curve:         "siec",
		Overwrite:     true,
		NoCompress:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// A sender-hosted address that refuses connections.
	receiver.senderRelayAddress = netip.MustParseAddrPort("127.0.0.1:1")

	results := make(chan error, 2)
	go func() {
		filesInfo, emptyFolders, totalNumberFolders, err := GetFilesInfo([]string{tempFile}, false, false, nil)
		if err != nil {
			results <- err
			return
		}
		results <- sender.Send(filesInfo, emptyFolders, totalNumberFolders)
	}()
	go func() { results <- receiver.Receive() }()
	waitForTransferResults(t, results, 2, 30*time.Second, sender, receiver)

	if address, _ := receiver.currentRelayControlRoute(); receiver.relayControlRouteIsPeerToPeer(address) {
		t.Fatalf("receiver route %s is marked peer-to-peer after falling back to the relay", address)
	}
	want, _ := os.ReadFile(tempFile)
	got, err := os.ReadFile(receivedFile)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("received %d bytes (%v); want %d", len(got), err, len(want))
	}
}

package croc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/codephrase"
	log "github.com/schollz/croc/v11/src/logger"
	"github.com/schollz/croc/v11/src/tcp"
)

func TestReceiverCodeWithSenderAddress(t *testing.T) {
	addr := netip.MustParseAddrPort("203.0.113.7:9009")
	for _, plainCode := range []string{"bacon-pickle-trout", "me@example-dot-com"} {
		t.Run(plainCode, func(t *testing.T) {
			code := codephrase.WithSenderAddress(plainCode, addr)
			receiver, err := newTestClient(Options{SharedSecret: code, Curve: "p256"})
			if err != nil {
				t.Fatal(err)
			}
			plain, err := newTestClient(Options{SharedSecret: plainCode, Curve: "p256"})
			if err != nil {
				t.Fatal(err)
			}
			if receiver.senderRelayAddress != addr || receiver.Options.SharedSecret != plainCode {
				t.Fatalf("receiver parsed %v and %q", receiver.senderRelayAddress, receiver.Options.SharedSecret)
			}
			if receiver.Options.RoomName != plain.Options.RoomName || receiver.pakePassphrase != plain.pakePassphrase {
				t.Fatal("a code with a sender address must use the same room and PAKE password as its plain code")
			}
			if redacted := receiver.redactError(errors.New("failed with " + code)).Error(); strings.Contains(redacted, strings.TrimPrefix(code, plainCode)) {
				t.Fatalf("sender address was not redacted: %q", redacted)
			}
			if _, err := newTestClient(Options{IsSender: true, SharedSecret: code, Curve: "p256"}); err == nil {
				t.Fatal("a sender accepted a code that already includes a sender address")
			}
		})
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

func TestSenderHostedRelayAdmitsOnlyTransferRooms(t *testing.T) {
	sender := newExposureTestSender(t, TransportDirect)
	sender.exposeRelayPorts = func(_ context.Context, ports []uint16) (netip.AddrPort, func(), error) {
		return netip.AddrPortFrom(netip.MustParseAddr("203.0.113.7"), ports[0]), func() {}, nil
	}
	if _, _, err := sender.exposeSenderRelay(); err != nil {
		t.Fatal(err)
	}
	address := net.JoinHostPort("127.0.0.1", sender.localRelayPort)
	// Anyone who learns the exposed address knows the default relay
	// password, but not the transfer code the rooms are derived from.
	if conn, _, _, err := tcp.ConnectToTCPServer(address, "pass123", "unrelated-room"); err == nil {
		conn.Close()
		t.Fatal("the exposed relay admitted a room that does not belong to the transfer")
	}
	reconnectRoom, err := generateReconnectRoom()
	if err != nil {
		t.Fatal(err)
	}
	sender.setNextReconnectRoom(reconnectRoom)
	for _, room := range []string{
		sender.Options.RoomName,
		sender.Options.RoomName + "-1",
		reconnectRoom,
		reconnectRoom + "-0",
	} {
		conn, _, _, err := tcp.ConnectToTCPServer(address, "pass123", room)
		if err != nil {
			t.Fatalf("the exposed relay rejected transfer room %q: %v", room, err)
		}
		conn.Close()
	}
	for _, room := range []string{sender.Options.RoomName + "-x", sender.Options.RoomName + "x"} {
		if conn, _, _, err := tcp.ConnectToTCPServer(address, "pass123", room); err == nil {
			conn.Close()
			t.Fatalf("the exposed relay admitted %q", room)
		}
	}
}

func TestSenderHostedRelayPairsOnlyWithSender(t *testing.T) {
	sender := newExposureTestSender(t, TransportDirect)
	room := sender.Options.RoomName
	sender1 := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40001}
	sender6 := &net.TCPAddr{IP: net.IPv6loopback, Port: 40002}
	strangerA := &net.TCPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 40003}
	strangerB := &net.TCPAddr{IP: net.ParseIP("2001:db8::2"), Port: 40004}
	tests := []struct {
		name          string
		room          string
		peer, waiting net.Addr
		want          bool
	}{
		{name: "receiver opens a transfer room", room: room, peer: strangerA, want: true},
		{name: "receiver joins the sender", room: room + "-0", peer: strangerA, waiting: sender1, want: true},
		{name: "sender joins the receiver", room: room + "-1", peer: sender6, waiting: strangerB, want: true},
		{name: "strangers pair in a guessed room", room: room + "-0", peer: strangerB, waiting: strangerA},
		{name: "stranger opens an unrelated room", room: "unrelated-room", peer: strangerA},
		{name: "sender opens an unrelated room", room: "unrelated-room", peer: sender1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := sender.admitToSenderRelay(test.room, test.peer, test.waiting); got != test.want {
				t.Fatalf("admitToSenderRelay = %t; want %t", got, test.want)
			}
		})
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

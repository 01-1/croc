package croc

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/comm"
	"github.com/schollz/croc/v11/src/message"
	"github.com/schollz/croc/v11/src/tailcattransport"
)

type directTestBundle struct {
	bundle       *tailcatDataBundle
	peer         *tailcatDataBundle
	path         atomic.Value
	disabledDERP atomic.Int32
	disabledOn   atomic.Value
}

func newDirectTestBundle(streams int, path string) *directTestBundle {
	b := &directTestBundle{}
	b.bundle, b.peer = newPipeDataBundles(streams)
	b.path.Store(path)
	b.bundle.stats = func() tailcattransport.BundleStats {
		return tailcattransport.BundleStats{Path: b.path.Load().(string), StreamCount: streams}
	}
	b.bundle.directPath = func() bool { return b.path.Load().(string) == "direct" }
	b.bundle.disableDERP = func() error {
		b.disabledOn.Store(b.path.Load().(string))
		b.disabledDERP.Add(1)
		return nil
	}
	return b
}

// newDirectTestSender returns a --transport direct sender wired to a fake
// Tailcat listener, plus the peer side of its control channel.
func newDirectTestSender(t *testing.T, key []byte, bundle *directTestBundle) (*Client, *comm.Comm, *transferAttemptState) {
	t.Helper()
	listener := &fakeTailcatListener{
		offer: "authenticated-tailcat-offer",
		accept: func(context.Context) (*tailcatDataBundle, error) {
			return bundle.bundle, nil
		},
	}
	provider := &fakeTailcatTransport{available: true}
	provider.listen = func(context.Context, []byte, tailcattransport.PathEvent) (tailcatDataListener, error) {
		return listener, nil
	}
	provider.validate = func(string, []byte) error { return nil }
	controlLocal, controlPeer := net.Pipe()
	t.Cleanup(func() {
		_ = controlLocal.Close()
		_ = controlPeer.Close()
	})
	client := &Client{
		Options:                Options{IsSender: true, Transport: TransportDirect},
		Key:                    key,
		conn:                   make([]*comm.Comm, len(bundle.bundle.connections)+1),
		stop:                   newStop(context.Background()),
		tailcat:                tailcatClientState{transport: provider, peerCapable: true},
		directPathPollOverride: 5 * time.Millisecond,
	}
	client.setConnection(0, comm.New(controlLocal))
	return client, comm.New(controlPeer), newTailcatTestAttempt(client.connection(0))
}

// runDirectTestReceiver answers the offer with a ready status and returns the
// sender's next message.
func runDirectTestReceiver(peer *comm.Comm, key []byte) <-chan message.Message {
	result := make(chan message.Message, 1)
	go func() {
		receive := func() (message.Message, error) {
			payload, err := peer.Receive()
			if err != nil {
				return message.Message{}, err
			}
			return message.Decode(key, payload)
		}
		offer, err := receive()
		if err != nil || offer.Type != message.TypeTailcatOffer {
			result <- message.Message{Type: "unexpected", Message: string(offer.Type)}
			return
		}
		if err := message.Send(peer, key, message.Message{Type: message.TypeTailcatStatus, Message: tailcatStatusReady}); err != nil {
			result <- message.Message{Type: "unexpected", Message: err.Error()}
			return
		}
		next, err := receive()
		if err != nil {
			next = message.Message{Type: "unexpected", Message: err.Error()}
		}
		result <- next
	}()
	return result
}

func TestDirectTransportWaitsForDirectPathBeforeSelecting(t *testing.T) {
	key := bytes.Repeat([]byte{0x51}, 32)
	bundle := newDirectTestBundle(2, "derp")
	defer bundle.peer.Close()
	client, peer, attempt := newDirectTestSender(t, key, bundle)
	next := runDirectTestReceiver(peer, key)
	go func() {
		time.Sleep(50 * time.Millisecond)
		bundle.path.Store("direct")
	}()

	if err := client.activateSecureChannel(attempt); err != nil {
		t.Fatal(err)
	}
	selection := <-next
	if selection.Type != message.TypeTransportSelect || selection.Message != string(TransportDERP) {
		t.Fatalf("selection = %+v; want Tailcat", selection)
	}
	if got := bundle.disabledDERP.Load(); got != 1 {
		t.Fatalf("DERP disabled %d times; want once", got)
	}
	if got := bundle.disabledOn.Load(); got != "direct" {
		t.Fatalf("DERP disabled while path was %v; want direct", got)
	}
	if client.selectedDataTransport.Load() != selectedTransportTailcat {
		t.Fatalf("selected transport = %d", client.selectedDataTransport.Load())
	}
	client.closeTailcatBundle()
}

func TestDirectTransportFailsInsteadOfRelaying(t *testing.T) {
	key := bytes.Repeat([]byte{0x52}, 32)
	bundle := newDirectTestBundle(2, "derp")
	defer bundle.peer.Close()
	client, peer, attempt := newDirectTestSender(t, key, bundle)
	// Shorten the coordinated setup window: the sender decides two seconds
	// before the setup deadline.
	attempt.tailcat.setupContext, attempt.tailcat.setupCancel = context.WithCancel(context.Background())
	attempt.tailcat.setupDeadline = time.Now().Add(2*time.Second + 200*time.Millisecond)
	next := runDirectTestReceiver(peer, key)

	err := client.activateSecureChannel(attempt)
	if !errors.Is(err, ErrDirectPath) {
		t.Fatalf("activation error = %v; want ErrDirectPath", err)
	}
	reported := <-next
	if reported.Type != message.TypeError || !strings.Contains(reported.Message, ErrDirectPath.Error()) {
		t.Fatalf("receiver got %+v; want a direct path error instead of a relay selection", reported)
	}
	if bundle.disabledDERP.Load() != 0 {
		t.Fatal("DERP was disabled although no direct path was found")
	}
	if client.selectedDataTransport.Load() != selectedTransportUnset {
		t.Fatalf("a data transport was selected: %d", client.selectedDataTransport.Load())
	}
}

func TestDirectPathGuardFailsWhenPathIsLost(t *testing.T) {
	key := bytes.Repeat([]byte{0x53}, 32)
	bundle := newDirectTestBundle(1, "direct")
	defer bundle.peer.Close()
	client, peer, attempt := newDirectTestSender(t, key, bundle)
	reported := make(chan message.Message, 1)
	go func() {
		payload, err := peer.Receive()
		if err != nil {
			reported <- message.Message{Type: "unexpected", Message: err.Error()}
			return
		}
		m, _ := message.Decode(key, payload)
		reported <- m
	}()

	client.directPathGraceOverride = 20 * time.Millisecond
	client.sendingFileData.Store(true)
	client.guardDirectPath(bundle.bundle, attempt)
	defer bundle.bundle.Close()
	bundle.path.Store("derp")
	select {
	case err := <-attempt.errc:
		if !errors.Is(err, ErrDirectPath) {
			t.Fatalf("guard error = %v; want ErrDirectPath", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("guard did not fail the transfer after the direct path was lost")
	}
	if m := <-reported; m.Type != message.TypeError || !strings.Contains(m.Message, "was lost during the transfer") {
		t.Fatalf("receiver got %+v; want the lost-path error", m)
	}
}

func TestDirectPathGuardIgnoresPathAfterTransferCompletes(t *testing.T) {
	bundle := newDirectTestBundle(1, "direct")
	defer bundle.peer.Close()
	client, _, attempt := newDirectTestSender(t, bytes.Repeat([]byte{0x54}, 32), bundle)
	client.directPathGraceOverride = 10 * time.Millisecond
	client.sendingFileData.Store(true)
	client.tailcat.terminal.Store(true)
	client.guardDirectPath(bundle.bundle, attempt)
	bundle.path.Store("derp")
	time.Sleep(100 * time.Millisecond)
	_ = bundle.bundle.Close()
	select {
	case err := <-attempt.errc:
		t.Fatalf("guard reported %v after the transfer completed", err)
	default:
	}
}

func TestDirectTransportIgnoresAutoThresholdAndRequiresTailcat(t *testing.T) {
	client := &Client{
		Options: Options{IsSender: true, Transport: TransportDirect},
		tailcat: tailcatClientState{transport: &fakeTailcatTransport{available: true}},
		stop:    newStop(context.Background()),
	}
	client.tailcat.transferBytes.Store(1)
	if !client.localTailcatSupported() {
		t.Fatal("--transport direct did not use Tailcat below the auto threshold")
	}
	if features := client.pakeFeatures(); !supportsFeature(features, tailcatFeature) || !supportsFeature(features, tailcatRequiredFeature) {
		t.Fatalf("--transport direct features = %v; want Tailcat required", features)
	}
	if err := client.activateSecureChannel(newTailcatTestAttempt(nil)); err == nil || !strings.Contains(err.Error(), "--transport direct requires a Tailcat-capable peer") {
		t.Fatalf("direct transport with an incapable peer: %v", err)
	}
}

func TestDirectTransportOptionValidation(t *testing.T) {
	if mode, err := ParseTransportMode(" Direct "); err != nil || mode != TransportDirect {
		t.Fatalf("ParseTransportMode(direct) = (%q, %v)", mode, err)
	}
	provider := &fakeTailcatTransport{available: true}
	if _, err := newClient(Options{SharedSecret: "direct-qr", Curve: "p256", IsSender: true, Transport: TransportDirect, ShowQrCode: true}, provider); err == nil || !strings.Contains(err.Error(), "--qrcode") {
		t.Fatalf("direct transport with --qrcode: %v", err)
	}
	if _, err := newClient(Options{SharedSecret: "direct-receiver", Curve: "p256", Transport: TransportDirect}, provider); err == nil || err.Error() != "transport selection is sender-only" {
		t.Fatalf("receiver direct transport: %v", err)
	}
	if _, err := newClient(Options{SharedSecret: "direct-unavailable", Curve: "p256", IsSender: true, Transport: TransportDirect}, &unavailableTestDataTransport{}); !errors.Is(err, errDirectUnavailable) {
		t.Fatalf("direct transport without Tailcat: %v", err)
	}
}

func TestDirectPathGuardIgnoresIdlePathBeforeTransferStarts(t *testing.T) {
	bundle := newDirectTestBundle(1, "direct")
	defer bundle.peer.Close()
	client, _, attempt := newDirectTestSender(t, bytes.Repeat([]byte{0x55}, 32), bundle)
	client.directPathGraceOverride = 10 * time.Millisecond
	client.guardDirectPath(bundle.bundle, attempt)
	// Magicsock stops confirming an idle path, for example while the
	// receiver is still answering its accept prompt.
	bundle.path.Store("derp")
	time.Sleep(100 * time.Millisecond)
	_ = bundle.bundle.Close()
	select {
	case err := <-attempt.errc:
		t.Fatalf("guard reported %v before file data started", err)
	default:
	}
}

func TestDirectPathGuardIgnoresSenderApprovalPrompt(t *testing.T) {
	bundle := newDirectTestBundle(1, "direct")
	defer bundle.peer.Close()
	client, _, attempt := newDirectTestSender(t, bytes.Repeat([]byte{0x56}, 32), bundle)
	client.directPathGraceOverride = 10 * time.Millisecond
	// The receiver has requested a file, but --ask is still waiting for the
	// sender's approval, so no file data flows yet.
	client.markTransferStarted()
	client.guardDirectPath(bundle.bundle, attempt)
	bundle.path.Store("derp")
	time.Sleep(100 * time.Millisecond)
	_ = bundle.bundle.Close()
	select {
	case err := <-attempt.errc:
		t.Fatalf("guard reported %v while the sender was still approving the transfer", err)
	default:
	}
}

package croc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/comm"
	log "github.com/schollz/croc/v11/src/logger"
	"github.com/schollz/croc/v11/src/message"
)

// runWebRTCTestTransfer sends one file between two CLI clients that share a
// test relay but not Tailcat, so the data transport is chosen by the WebRTC
// exchange.
func runWebRTCTestTransfer(t *testing.T, size int, configure func(sender, receiver *Client)) (sender, receiver *Client) {
	t.Helper()
	log.SetLevel("warn")
	relay := startTestRelay(t, 4)
	tempFile, cleanup := createTestFile(t, size)
	defer cleanup()
	receivedFile := filepath.Base(tempFile)
	defer os.Remove(receivedFile)

	secret := fmt.Sprintf("webrtc-%d", time.Now().UnixNano())
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
	receiver, err = newTestClient(Options{
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
	// Host candidates only: the test must not contact public STUN servers.
	sender.Options.STUNServers = []string{}
	receiver.Options.STUNServers = []string{}
	configure(sender, receiver)

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
	waitForTransferResults(t, results, 2, 60*time.Second, sender, receiver)

	want, _ := os.ReadFile(tempFile)
	got, err := os.ReadFile(receivedFile)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("received %d bytes (%v); want %d", len(got), err, len(want))
	}
	return sender, receiver
}

func TestWebRTCDataTransportBetweenClients(t *testing.T) {
	sender, receiver := runWebRTCTestTransfer(t, 4<<20, func(sender, _ *Client) {
		sender.webrtcAnySize = true
	})
	for name, client := range map[string]*Client{"sender": sender, "receiver": receiver} {
		if client.selectedDataTransport.Load() != selectedTransportTailcat {
			t.Fatalf("%s data transport = %d; want the WebRTC bundle", name, client.selectedDataTransport.Load())
		}
	}
}

func TestWebRTCFallsBackToRelayWhenPeerCannotAnswer(t *testing.T) {
	sender, receiver := runWebRTCTestTransfer(t, 256*1024, func(sender, receiver *Client) {
		sender.webrtcAnySize = true
		// An unusable STUN URL makes the receiver's peer connection fail,
		// so it answers empty and the sender must select the relay.
		receiver.Options.STUNServers = []string{"not-a-stun-url"}
	})
	for name, client := range map[string]*Client{"sender": sender, "receiver": receiver} {
		if client.selectedDataTransport.Load() != selectedTransportRelay {
			t.Fatalf("%s data transport = %d; want the relay", name, client.selectedDataTransport.Load())
		}
	}
}

func TestWebRTCSelectsRelayBelowThreshold(t *testing.T) {
	sender, receiver := runWebRTCTestTransfer(t, 64*1024, func(*Client, *Client) {})
	for name, client := range map[string]*Client{"sender": sender, "receiver": receiver} {
		if client.selectedDataTransport.Load() != selectedTransportRelay {
			t.Fatalf("%s data transport = %d; want the relay below the threshold", name, client.selectedDataTransport.Load())
		}
	}
}

func TestWebRTCFeatureAdvertisement(t *testing.T) {
	for _, tt := range []struct {
		name    string
		options Options
		want    bool
	}{
		{name: "receiver", options: Options{}, want: true},
		{name: "auto sender", options: Options{IsSender: true, Transport: TransportAuto}, want: true},
		{name: "relay sender", options: Options{IsSender: true, Transport: TransportRelay}},
		{name: "local only", options: Options{OnlyLocal: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{Options: tt.options, tailcat: tailcatClientState{transport: &unavailableTestDataTransport{}}}
			if got := supportsFeature(client.pakeFeatures(), webrtcFeature); got != tt.want {
				t.Fatalf("advertises WebRTC = %t; want %t", got, tt.want)
			}
		})
	}
}

func TestWebRTCAnswerDeadlineKeepsControlFramesAligned(t *testing.T) {
	key := bytes.Repeat([]byte{0x61}, 32)
	local, peer := net.Pipe()
	defer local.Close()
	defer peer.Close()
	client := &Client{Key: key, conn: make([]*comm.Comm, 1)}
	client.setConnection(0, comm.New(local))

	answer, err := message.Encode(key, message.Message{Type: message.TypeWebRTCAnswer, Message: "late answer"})
	if err != nil {
		t.Fatal(err)
	}
	frame := binary.LittleEndian.AppendUint32(append([]byte(nil), comm.MAGIC_BYTES...), uint32(len(answer)))
	frame = append(frame, answer...)
	written := make(chan error, 1)
	go func() {
		// Part of the answer arrives before the sender's deadline and the
		// rest after it, followed by the receiver's next message.
		if _, err := peer.Write(frame[:3]); err != nil {
			written <- err
			return
		}
		time.Sleep(300 * time.Millisecond)
		if _, err := peer.Write(frame[3:]); err != nil {
			written <- err
			return
		}
		written <- message.Send(comm.New(peer), key, message.Message{Type: message.TypeError, Message: "next"})
	}()

	if got, err := client.receiveWebRTCAnswer(time.Now().Add(100 * time.Millisecond)); err != nil || got != "late answer" {
		t.Errorf("receiveWebRTCAnswer = (%q, %v); want the answer whose frame started before the deadline", got, err)
	}
	payload, err := client.connection(0).ReceiveWithDeadline(time.Now().Add(2 * time.Second))
	if err != nil {
		t.Fatalf("next control frame: %v", err)
	}
	if next, err := message.Decode(key, payload); err != nil || next.Message != "next" {
		t.Fatalf("next control message = (%+v, %v)", next, err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
}

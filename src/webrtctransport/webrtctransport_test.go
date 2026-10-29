package webrtctransport

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/comm"
)

// connectLoopback runs a full offer/answer exchange in-process with host
// candidates only.
func connectLoopback(t *testing.T, channels int) (offerer, answerer *Session, a, b []net.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := Config{STUNServers: []string{}, Channels: channels}
	offerer, offer, err := Offer(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { offerer.Close() })
	answerer, answer, err := Answer(ctx, cfg, offer, cfg.ChannelCount())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { answerer.Close() })
	if err := offerer.Accept(answer); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var errA, errB error
	wg.Add(2)
	go func() { defer wg.Done(); a, errA = offerer.Wait(ctx) }()
	go func() { defer wg.Done(); b, errB = answerer.Wait(ctx) }()
	wg.Wait()
	if errA != nil || errB != nil {
		t.Fatalf("wait: offerer %v, answerer %v", errA, errB)
	}
	return offerer, answerer, a, b
}

func TestLoopbackCarriesCrocFramesOnEveryChannel(t *testing.T) {
	_, _, a, b := connectLoopback(t, 3)
	if len(a) != 3 || len(b) != 3 {
		t.Fatalf("got %d and %d channels; want 3", len(a), len(b))
	}
	payload := make([]byte, 200*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	for i := range a {
		sender, receiver := comm.New(a[i]), comm.New(b[i])
		errc := make(chan error, 1)
		go func() { errc <- sender.Send(payload) }()
		got, err := receiver.Receive()
		if err != nil {
			t.Fatalf("channel %d receive: %v", i, err)
		}
		if err := <-errc; err != nil {
			t.Fatalf("channel %d send: %v", i, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("channel %d corrupted a %d-byte frame", i, len(payload))
		}
		// And back, so both directions are covered.
		go func() { errc <- receiver.Send(payload[:1000]) }()
		if got, err := sender.Receive(); err != nil || !bytes.Equal(got, payload[:1000]) {
			t.Fatalf("channel %d reverse frame: %v", i, err)
		}
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
}

func TestLoopbackStreamsLargeTransfersWithBackpressure(t *testing.T) {
	_, _, a, b := connectLoopback(t, 1)
	const total = 32 << 20
	chunk := make([]byte, 1<<20)
	if _, err := rand.Read(chunk); err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() {
		for sent := 0; sent < total; sent += len(chunk) {
			if _, err := a[0].Write(chunk); err != nil {
				errc <- err
				return
			}
		}
		errc <- nil
	}()
	buf := make([]byte, 64*1024)
	received := 0
	for received < total {
		if err := b[0].SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatal(err)
		}
		n, err := b[0].Read(buf)
		if err != nil {
			t.Fatalf("read after %d bytes: %v", received, err)
		}
		if !bytes.Equal(buf[:n], chunk[received%len(chunk):received%len(chunk)+n]) {
			t.Fatalf("stream corrupted at byte %d", received)
		}
		received += n
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestClosedConnReportsNetErrClosed(t *testing.T) {
	_, _, a, b := connectLoopback(t, 1)
	if err := a[0].Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := a[0].Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("read after local close = %v; want net.ErrClosed", err)
	}
	if err := b[0].SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(b[0], make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("read after remote close = %v; want net.ErrClosed", err)
	}
}

func TestAnswerRejectsInvalidChannelCounts(t *testing.T) {
	for _, channels := range []int{0, -1, MaxChannels + 1} {
		if _, _, err := Answer(context.Background(), Config{STUNServers: []string{}}, "v=0", channels); err == nil {
			t.Fatalf("Answer accepted %d channels", channels)
		}
	}
	if _, _, err := Offer(context.Background(), Config{STUNServers: []string{}, Channels: MaxChannels + 1}); err == nil {
		t.Fatal("Offer accepted too many channels")
	}
}

func TestWaitFailsWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	offerer, _, err := Offer(ctx, Config{STUNServers: []string{}, Channels: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer offerer.Close()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer waitCancel()
	if _, err := offerer.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait without an answer = %v; want deadline exceeded", err)
	}
}

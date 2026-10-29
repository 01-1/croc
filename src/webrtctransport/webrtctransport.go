// Package webrtctransport carries croc's data streams over WebRTC data
// channels, the only peer-to-peer path a browser can open. The offer and
// answer travel over croc's PAKE-encrypted control channel, so the DTLS
// fingerprints in them are authenticated, and file data keeps croc's own
// encryption on top of DTLS. Only STUN is configured: when no direct path
// exists the setup fails and croc falls back to its relay.
package webrtctransport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/pion/datachannel"
	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
)

// DefaultSTUNServers are used when Config.STUNServers is empty. STUN servers
// only learn each peer's public address; they never carry file data.
var DefaultSTUNServers = []string{"stun:stun.l.google.com:19302", "stun:stun1.l.google.com:19302"}

const (
	// DefaultChannels is the number of data channels the offerer creates,
	// matching the parallelism of croc's relay data ports.
	DefaultChannels = 4
	// MaxMessageSize is the largest data channel message either side sends.
	// 16 KiB is accepted by every browser's SCTP implementation.
	MaxMessageSize = 16 * 1024
	// readBufferSize leaves room for peers that send larger messages.
	readBufferSize = 256 * 1024
	labelPrefix    = "croc-"
)

// Config controls a peer connection.
type Config struct {
	// STUNServers are STUN URLs; nil uses DefaultSTUNServers. An empty,
	// non-nil slice disables STUN (host candidates only).
	STUNServers []string
	// Channels is the number of data channels an offer creates.
	Channels int
}

// ChannelCount returns the data channel count an offer creates.
func (c Config) ChannelCount() int { return c.channels() }

func (c Config) channels() int {
	if c.Channels > 0 {
		return c.Channels
	}
	return DefaultChannels
}

func newPeerConnection(cfg Config) (*webrtc.PeerConnection, error) {
	var settings webrtc.SettingEngine
	settings.DetachDataChannels()
	// Block writes while SCTP buffers are full instead of queueing without
	// bound.
	settings.EnableDataChannelBlockWrite(true)
	// Browsers hide LAN addresses behind mDNS names; resolve them so peers
	// on the same network can connect directly.
	settings.SetICEMulticastDNSMode(ice.MulticastDNSModeQueryOnly)
	servers := cfg.STUNServers
	if servers == nil {
		servers = DefaultSTUNServers
	}
	var iceServers []webrtc.ICEServer
	if len(servers) > 0 {
		iceServers = []webrtc.ICEServer{{URLs: servers}}
	}
	api := webrtc.NewAPI(webrtc.WithSettingEngine(settings))
	return api.NewPeerConnection(webrtc.Configuration{ICEServers: iceServers})
}

// gather sets desc as the local description and waits for ICE gathering, so
// the returned SDP carries every candidate (croc does not trickle).
func gather(ctx context.Context, pc *webrtc.PeerConnection, desc webrtc.SessionDescription) (string, error) {
	complete := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(desc); err != nil {
		return "", err
	}
	select {
	case <-complete:
	case <-ctx.Done():
		return "", fmt.Errorf("gather ICE candidates: %w", ctx.Err())
	}
	local := pc.LocalDescription()
	if local == nil {
		return "", errors.New("no local description")
	}
	return local.SDP, nil
}

// Session is one side of a peer connection and owns its data channels.
type Session struct {
	pc        *webrtc.PeerConnection
	want      int
	mu        sync.Mutex
	opened    map[int]net.Conn
	ready     chan struct{}
	readyOnce sync.Once
	failed    chan struct{}
	failOnce  sync.Once
	closeOnce sync.Once
}

func newSession(pc *webrtc.PeerConnection, want int) *Session {
	s := &Session{pc: pc, want: want, opened: make(map[int]net.Conn), ready: make(chan struct{}), failed: make(chan struct{})}
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			s.failOnce.Do(func() { close(s.failed) })
		}
	})
	return s
}

func (s *Session) track(dc *webrtc.DataChannel) {
	index, ok := channelIndex(dc.Label())
	if !ok {
		_ = dc.Close()
		return
	}
	dc.OnOpen(func() {
		raw, err := dc.DetachWithDeadline()
		if err != nil {
			s.failOnce.Do(func() { close(s.failed) })
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, dup := s.opened[index]; dup || (s.want > 0 && index >= s.want) {
			_ = raw.Close()
			return
		}
		s.opened[index] = newConn(raw)
		if s.want > 0 && len(s.opened) == s.want {
			s.readyOnce.Do(func() { close(s.ready) })
		}
	})
}

func channelIndex(label string) (int, bool) {
	var index int
	if !strings.HasPrefix(label, labelPrefix) {
		return 0, false
	}
	if _, err := fmt.Sscanf(label[len(labelPrefix):], "%d", &index); err != nil || index < 0 || index >= MaxChannels {
		return 0, false
	}
	return index, true
}

// Offer creates a peer connection with cfg.Channels data channels and returns
// its complete SDP offer. Send Channels with it so the answerer knows how many
// to wait for.
func Offer(ctx context.Context, cfg Config) (*Session, string, error) {
	if cfg.channels() > MaxChannels {
		return nil, "", fmt.Errorf("invalid WebRTC channel count %d", cfg.channels())
	}
	pc, err := newPeerConnection(cfg)
	if err != nil {
		return nil, "", err
	}
	s := newSession(pc, cfg.channels())
	ordered := true
	for i := range cfg.channels() {
		dc, err := pc.CreateDataChannel(fmt.Sprintf("%s%d", labelPrefix, i), &webrtc.DataChannelInit{Ordered: &ordered})
		if err != nil {
			_ = s.Close()
			return nil, "", err
		}
		s.track(dc)
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		_ = s.Close()
		return nil, "", err
	}
	sdp, err := gather(ctx, pc, offer)
	if err != nil {
		_ = s.Close()
		return nil, "", err
	}
	return s, sdp, nil
}

// Accept applies the peer's answer to an offering session.
func (s *Session) Accept(answer string) error {
	return s.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer})
}

// MaxChannels bounds the channel count an offerer may request.
const MaxChannels = 16

// Answer applies a peer's offer and returns the answering session and its
// complete SDP answer. channels is the count the offerer stated alongside the
// offer; data channel labels are not part of SDP.
func Answer(ctx context.Context, cfg Config, offer string, channels int) (*Session, string, error) {
	if channels < 1 || channels > MaxChannels {
		return nil, "", fmt.Errorf("invalid WebRTC channel count %d", channels)
	}
	pc, err := newPeerConnection(cfg)
	if err != nil {
		return nil, "", err
	}
	s := newSession(pc, channels)
	pc.OnDataChannel(s.track)
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer}); err != nil {
		_ = s.Close()
		return nil, "", err
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		_ = s.Close()
		return nil, "", err
	}
	sdp, err := gather(ctx, pc, answer)
	if err != nil {
		_ = s.Close()
		return nil, "", err
	}
	return s, sdp, nil
}

// Wait blocks until every data channel is open and returns them in label
// order. It fails when ctx ends or the peer connection fails.
func (s *Session) Wait(ctx context.Context) ([]net.Conn, error) {
	select {
	case <-s.ready:
	case <-s.failed:
		return nil, errors.New("WebRTC peer connection failed")
	case <-ctx.Done():
		return nil, fmt.Errorf("open WebRTC data channels: %w", ctx.Err())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	conns := make([]net.Conn, s.want)
	for i := range conns {
		conns[i] = s.opened[i]
	}
	return conns, nil
}

// Path describes the selected ICE candidate pair, for logging.
func (s *Session) Path() string {
	sctp := s.pc.SCTP()
	if sctp == nil || sctp.Transport() == nil || sctp.Transport().ICETransport() == nil {
		return ""
	}
	pair, err := sctp.Transport().ICETransport().GetSelectedCandidatePair()
	if err != nil || pair == nil {
		return ""
	}
	return fmt.Sprintf("%s %s <-> %s %s", pair.Local.Typ, pair.Local.Address, pair.Remote.Typ, pair.Remote.Address)
}

// Close closes every data channel and the peer connection.
func (s *Session) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.mu.Lock()
		for _, conn := range s.opened {
			_ = conn.Close()
		}
		s.mu.Unlock()
		err = s.pc.Close()
	})
	return err
}

// conn adapts a detached data channel to a byte stream: writes are split into
// MaxMessageSize messages and reads return message bytes in order.
type conn struct {
	raw     datachannel.ReadWriteCloserDeadliner
	readMu  sync.Mutex
	buf     []byte
	pending []byte
	closed  chan struct{}
	once    sync.Once
}

func newConn(raw datachannel.ReadWriteCloserDeadliner) *conn {
	return &conn{raw: raw, buf: make([]byte, readBufferSize), closed: make(chan struct{})}
}

func (c *conn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for len(c.pending) == 0 {
		select {
		case <-c.closed:
			return 0, net.ErrClosed
		default:
		}
		n, err := c.raw.Read(c.buf)
		if err != nil {
			return 0, c.mapErr(err)
		}
		c.pending = c.buf[:n]
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *conn) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		chunk := p[:min(len(p), MaxMessageSize)]
		n, err := c.raw.Write(chunk)
		written += n
		if err != nil {
			return written, c.mapErr(err)
		}
		p = p[len(chunk):]
	}
	return written, nil
}

// mapErr reports reads and writes after a local or remote close as net.ErrClosed,
// which croc treats as an expected end of a peer-to-peer stream.
func (c *conn) mapErr(err error) error {
	select {
	case <-c.closed:
		return net.ErrClosed
	default:
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
		return net.ErrClosed
	}
	return err
}

// Close closes the data channel. Closing a detached channel does not wake a
// blocked read, so it first expires the read deadline.
func (c *conn) Close() error {
	var err error
	c.once.Do(func() {
		close(c.closed)
		_ = c.raw.SetReadDeadline(time.Now())
		err = c.raw.Close()
	})
	return err
}

func (c *conn) LocalAddr() net.Addr  { return addr{} }
func (c *conn) RemoteAddr() net.Addr { return addr{} }

func (c *conn) SetDeadline(t time.Time) error {
	if err := c.raw.SetReadDeadline(t); err != nil {
		return err
	}
	return c.raw.SetWriteDeadline(t)
}

func (c *conn) SetReadDeadline(t time.Time) error  { return c.raw.SetReadDeadline(t) }
func (c *conn) SetWriteDeadline(t time.Time) error { return c.raw.SetWriteDeadline(t) }

type addr struct{}

func (addr) Network() string { return "webrtc" }
func (addr) String() string  { return "webrtc" }

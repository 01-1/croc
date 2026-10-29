package croc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	log "github.com/schollz/croc/v11/src/logger"
	"github.com/schollz/croc/v11/src/message"
	"github.com/schollz/croc/v11/src/tailcattransport"
	"github.com/schollz/croc/v11/src/webrtctransport"
)

// WebRTC gives browser peers, which cannot use Tailcat, a direct data path.
// When both sides advertise webrtcFeature and do not share Tailcat, the sender
// always ends negotiation with a transport selection: "webrtc" after an
// offer/answer exchange on the encrypted control channel, or "relay". The
// receiver therefore waits for that selection instead of opening relay data
// channels on its own.
const (
	webrtcFeature         = "webrtc-v1"
	webrtcSetupTimeout    = 12 * time.Second
	transportSelectWebRTC = "webrtc"
)

type webrtcState struct {
	peerCapable bool
	mu          sync.Mutex
	session     *webrtctransport.Session
	offered     bool
	selected    chan struct{}
	selectOnce  sync.Once
}

// reset clears per-attempt state before a reconnect.
func (s *webrtcState) reset() {
	s.closePending()
	s.mu.Lock()
	s.peerCapable = false
	s.offered = false
	s.selected = nil
	s.selectOnce = sync.Once{}
	s.mu.Unlock()
}

// closePending closes an answered session that was never selected.
func (s *webrtcState) closePending() {
	s.mu.Lock()
	session := s.session
	s.session = nil
	s.mu.Unlock()
	if session != nil {
		_ = session.Close()
	}
}

func (c *Client) webrtcConfig() webrtctransport.Config {
	return webrtctransport.Config{STUNServers: c.Options.STUNServers, Channels: webrtctransport.DefaultChannels}
}

// localWebRTCSupported reports whether this client advertises WebRTC. Relay,
// strict Tailcat, and local-only transfers never use it.
func (c *Client) localWebRTCSupported() bool {
	if c.Options.OnlyLocal || c.disableWebRTC {
		return false
	}
	return !c.Options.IsSender || c.Options.Transport == TransportAuto
}

// webrtcNegotiated reports whether the data transport is chosen by the
// WebRTC exchange: both sides support it and they do not share Tailcat.
func (c *Client) webrtcNegotiated() bool {
	return c.localWebRTCSupported() && c.webrtc.peerCapable && !(c.tailcat.peerCapable && c.localTailcatSupported())
}

// webrtcSenderEligible reports whether the sender attempts WebRTC rather than
// selecting the relay outright. Like Tailcat it follows auto mode's size
// threshold, and a transfer already meeting on the sender's own relay over the
// LAN gains nothing from it.
func (c *Client) webrtcSenderEligible() bool {
	address, _ := c.currentRelayControlRoute()
	return (c.webrtcAnySize || c.autoTailcatEligible()) && !c.relayControlRouteIsPeerToPeer(address)
}

// activateWebRTCSender runs the sender's half of the exchange and always ends
// it with a transport selection.
func (c *Client) activateWebRTCSender(attempt *transferAttemptState) error {
	if !c.webrtcSenderEligible() {
		return c.selectRelayForWebRTCPeer(attempt, "eligibility", errors.New("transfer below the direct threshold"))
	}
	ctx, cancel := context.WithTimeout(c.stop.ctx, webrtcSetupTimeout)
	defer cancel()
	cfg := c.webrtcConfig()
	session, offer, err := webrtctransport.Offer(ctx, cfg)
	if err != nil {
		return c.selectRelayForWebRTCPeer(attempt, "offer", err)
	}
	if err := message.Send(c.connection(0), c.Key, message.Message{
		Type:    message.TypeWebRTCOffer,
		Message: offer,
		Num:     cfg.ChannelCount(),
	}); err != nil {
		_ = session.Close()
		return fmt.Errorf("send WebRTC offer: %w", err)
	}
	deadline, _ := ctx.Deadline()
	answer, err := c.receiveWebRTCAnswer(deadline)
	if err != nil {
		_ = session.Close()
		if _, ok := errors.AsType[tailcatProtocolError](err); ok {
			return err
		}
		return c.selectRelayForWebRTCPeer(attempt, "answer", err)
	}
	if answer == "" {
		_ = session.Close()
		return c.selectRelayForWebRTCPeer(attempt, "answer", errors.New("peer could not create a WebRTC answer"))
	}
	if err := session.Accept(answer); err != nil {
		_ = session.Close()
		return c.selectRelayForWebRTCPeer(attempt, "answer", err)
	}
	conns, err := session.Wait(ctx)
	if err != nil {
		_ = session.Close()
		return c.selectRelayForWebRTCPeer(attempt, "connection", err)
	}
	if err := message.Send(c.connection(0), c.Key, message.Message{
		Type:    message.TypeTransportSelect,
		Message: transportSelectWebRTC,
	}); err != nil {
		_ = session.Close()
		return fmt.Errorf("select WebRTC transport: %w", err)
	}
	log.Debugf("selected WebRTC data transport: %s", session.Path())
	if err := c.installTailcatBundle(newWebRTCBundle(session, conns), nil, attempt); err != nil {
		return err
	}
	return c.finishDataTransportActivation()
}

func (c *Client) receiveWebRTCAnswer(deadline time.Time) (string, error) {
	payload, err := c.connection(0).ReceiveWithDeadline(deadline)
	if err != nil {
		return "", err
	}
	m, err := message.Decode(c.Key, payload)
	if err != nil {
		return "", tailcatProtocolError{err: fmt.Errorf("invalid encrypted WebRTC answer: %w", err)}
	}
	if m.Type == message.TypeError {
		return "", tailcatProtocolError{err: fmt.Errorf("peer error: %s", m.Message)}
	}
	if m.Type != message.TypeWebRTCAnswer {
		return "", tailcatProtocolError{err: fmt.Errorf("expected WebRTC answer, got %s", m.Type)}
	}
	return m.Message, nil
}

func (c *Client) selectRelayForWebRTCPeer(attempt *transferAttemptState, stage string, setupErr error) error {
	log.Debugf("WebRTC %s did not produce a direct path; using the croc relay: %v", stage, setupErr)
	if err := message.Send(c.connection(0), c.Key, message.Message{
		Type:    message.TypeTransportSelect,
		Message: string(TransportRelay),
	}); err != nil {
		return fmt.Errorf("select relay transport: %w", err)
	}
	return c.activateRelayDataChannels(attempt)
}

// waitForWebRTCSelection arms the receiver's watchdog: a sender that
// advertised WebRTC must select a transport within the setup window.
func (c *Client) waitForWebRTCSelection(attempt *transferAttemptState) {
	c.webrtc.mu.Lock()
	selected := make(chan struct{})
	c.webrtc.selected = selected
	c.webrtc.mu.Unlock()
	go func() {
		timer := time.NewTimer(2 * webrtcSetupTimeout)
		defer timer.Stop()
		select {
		case <-selected:
		case <-timer.C:
			attempt.report(errors.New("sender did not select a data transport"))
		case <-c.stop.ctx.Done():
		}
	}()
}

func (c *Client) markWebRTCSelected() {
	c.webrtc.mu.Lock()
	selected := c.webrtc.selected
	c.webrtc.mu.Unlock()
	if selected != nil {
		c.webrtc.selectOnce.Do(func() { close(selected) })
	}
}

// processWebRTCOffer answers the sender's offer. An answer it cannot create is
// sent empty, so the sender falls back to the relay instead of waiting.
func (c *Client) processWebRTCOffer(m message.Message) error {
	if c.Options.IsSender || !c.webrtcNegotiated() {
		return errors.New("unexpected WebRTC offer")
	}
	c.webrtc.mu.Lock()
	if c.webrtc.offered {
		c.webrtc.mu.Unlock()
		return errors.New("duplicate WebRTC offer rejected")
	}
	c.webrtc.offered = true
	c.webrtc.mu.Unlock()

	ctx, cancel := context.WithTimeout(c.stop.ctx, webrtcSetupTimeout)
	defer cancel()
	session, answer, err := webrtctransport.Answer(ctx, c.webrtcConfig(), m.Message, m.Num)
	if err != nil {
		log.Debugf("could not answer WebRTC offer: %v", err)
		answer = ""
	} else {
		c.webrtc.mu.Lock()
		c.webrtc.session = session
		c.webrtc.mu.Unlock()
	}
	return message.Send(c.connection(0), c.Key, message.Message{Type: message.TypeWebRTCAnswer, Message: answer})
}

// processWebRTCSelect applies the sender's transport selection.
func (c *Client) processWebRTCSelect(m message.Message, attempt *transferAttemptState) error {
	if c.Options.IsSender {
		return errors.New("sender rejected a transport selection from the receiver")
	}
	if c.transportSelectionReceived {
		return errors.New("duplicate transport selection rejected")
	}
	c.transportSelectionReceived = true
	c.markWebRTCSelected()
	c.webrtc.mu.Lock()
	session := c.webrtc.session
	c.webrtc.session = nil
	c.webrtc.mu.Unlock()

	switch m.Message {
	case transportSelectWebRTC:
		if session == nil {
			return errors.New("WebRTC selected without an answered offer")
		}
		ctx, cancel := context.WithTimeout(c.stop.ctx, webrtcSetupTimeout)
		defer cancel()
		conns, err := session.Wait(ctx)
		if err != nil {
			_ = session.Close()
			return fmt.Errorf("WebRTC selected but its data channels did not open: %w", err)
		}
		log.Debugf("selected WebRTC data transport: %s", session.Path())
		if err := c.installTailcatBundle(newWebRTCBundle(session, conns), nil, attempt); err != nil {
			return err
		}
		return c.finishDataTransportActivation()
	case string(TransportRelay):
		if session != nil {
			_ = session.Close()
		}
		log.Debug("selected croc relay data transport after WebRTC negotiation")
		return c.activateRelayDataChannels(attempt)
	default:
		if session != nil {
			_ = session.Close()
		}
		return fmt.Errorf("invalid transport selection %q", m.Message)
	}
}

// newWebRTCBundle installs WebRTC data channels through the same path as a
// Tailcat bundle: an ordered set of streams that carry croc's data frames.
func newWebRTCBundle(session *webrtctransport.Session, conns []net.Conn) *tailcatDataBundle {
	return &tailcatDataBundle{
		connections: conns,
		stats: func() tailcattransport.BundleStats {
			return tailcattransport.BundleStats{Path: "webrtc", StreamCount: len(conns)}
		},
		cleanup: session.Close,
	}
}

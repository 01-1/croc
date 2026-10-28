package croc

import (
	"fmt"
	"time"

	log "github.com/schollz/croc/v11/src/logger"
	"github.com/schollz/croc/v11/src/message"
)

const (
	directPathPollInterval = 250 * time.Millisecond
	// directPathLossGrace is how long the live direct path may stay
	// unconfirmed during a transfer before it is treated as lost. DERP is
	// already disabled, so a short gap only stalls packets.
	directPathLossGrace = 5 * time.Second
)

func (c *Client) directPathPollInterval() time.Duration {
	if c.directPathPollOverride > 0 {
		return c.directPathPollOverride
	}
	return directPathPollInterval
}

func (c *Client) directPathLossGrace() time.Duration {
	if c.directPathGraceOverride > 0 {
		return c.directPathGraceOverride
	}
	return directPathLossGrace
}

// bundleIsDirect reports whether croc's Tailcat path tracking says "direct"
// and magicsock confirms it is sending over UDP alone.
func bundleIsDirect(bundle *tailcatDataBundle) bool {
	return bundle.stats().Path == "direct" && bundle.directPath != nil && bundle.directPath()
}

// requireDirectPath holds --transport direct file data until the Tailcat
// bundle reports a direct UDP path, then disables DERP on this side so no later
// packet can be relayed. Magicsock always bootstraps through DERP, so the
// bundle normally starts on "derp" and is promoted once NAT traversal works.
func (c *Client) requireDirectPath(bundle *tailcatDataBundle, deadline time.Time) error {
	if bundle == nil || bundle.stats == nil {
		return fmt.Errorf("%w: Tailcat bundle reports no path", ErrDirectPath)
	}
	started := time.Now()
	ticker := time.NewTicker(c.directPathPollInterval())
	defer ticker.Stop()
	timer := time.NewTimer(max(time.Until(deadline), 0))
	defer timer.Stop()
	for !bundleIsDirect(bundle) {
		select {
		case <-ticker.C:
		case <-timer.C:
			return fmt.Errorf("%w: path was still %q after %s; --transport direct never sends file data through DERP or the croc relay",
				ErrDirectPath, bundle.stats().Path, time.Since(started).Round(time.Second))
		case <-c.stop.ctx.Done():
			return c.stop.ctx.Err()
		}
	}
	if bundle.disableDERP == nil {
		return fmt.Errorf("%w: Tailcat bundle cannot disable DERP", ErrDirectPath)
	}
	if err := bundle.disableDERP(); err != nil {
		return fmt.Errorf("%w: disable DERP: %v", ErrDirectPath, err)
	}
	log.Debugf("direct path confirmed after %s; DERP disabled", time.Since(started).Round(time.Millisecond))
	return nil
}

// sendPeerError tells the receiver why a --transport direct transfer stopped,
// so both sides report the same reason instead of the receiver timing out.
func (c *Client) sendPeerError(err error) {
	if c.connection(0) == nil {
		return
	}
	if sendErr := message.Send(c.connection(0), c.Key, message.Message{Type: message.TypeError, Message: err.Error()}); sendErr != nil {
		log.Debugf("could not report direct path failure to peer: %v", sendErr)
	}
}

// guardDirectPath fails the transfer if the direct path is lost while file
// data is flowing. DERP is already disabled, so packets are dropped rather than
// relayed; the guard turns that stall into a prompt, explicit error. It only
// checks while the sender streams an approved file, because magicsock stops
// confirming paths that carry no traffic, for example during the receiver's
// accept prompt or the sender's --ask prompt. It must be called before the
// bundle is installed so its stop hook is part of the bundle's cleanup.
func (c *Client) guardDirectPath(bundle *tailcatDataBundle, attempt *transferAttemptState) {
	done := make(chan struct{})
	bundle.addCleanup(func() { close(done) })
	go func() {
		ticker := time.NewTicker(c.directPathPollInterval())
		defer ticker.Stop()
		var lostSince time.Time
		for {
			select {
			case <-ticker.C:
				if !c.sendingFileData.Load() || c.tailcat.terminal.Load() || bundle.directPath() {
					lostSince = time.Time{}
					continue
				}
				if lostSince.IsZero() {
					lostSince = time.Now()
					continue
				}
				if time.Since(lostSince) < c.directPathLossGrace() {
					continue
				}
				err := fmt.Errorf("%w: direct path to the receiver was lost during the transfer; --transport direct does not fall back to DERP or the croc relay",
					ErrDirectPath)
				c.sendPeerError(err)
				attempt.report(err)
				return
			case <-done:
				return
			case <-c.stop.ctx.Done():
				return
			}
		}
	}()
}

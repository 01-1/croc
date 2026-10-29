package tailcat

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"tailscale.com/tstest/integration"
	"tailscale.com/types/key"
)

func TestDisableDERPKeepsDirectPath(t *testing.T) {
	t.Parallel()

	dm := integration.RunDERPAndSTUN(t, mkLogger(t, "derpstun"), "127.0.0.1")
	s := &Server{Key: key.NewNode(), Logf: mkLogger(t, "server"), Region: dm.Regions[1]}
	t.Cleanup(func() { s.Close() })
	s.OnTCP = func(port uint16) func(net.Conn) {
		if port != 80 {
			return nil
		}
		return func(c net.Conn) {
			io.WriteString(c, "hello\n")
			c.Close()
		}
	}
	if err := s.Start(); err != nil {
		t.Fatalf("server Start: %v", err)
	}
	c := &Client{Server: s.ConnBlob(), Logf: mkLogger(t, "client")}
	t.Cleanup(func() { c.Close() })
	s.AddAllowedClient(c.PublicKey())
	PingForTest(t, s, c)

	dial := func() string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err := c.DialTCPPort(ctx, 80)
		if err != nil {
			t.Fatalf("DialTCPPort: %v", err)
		}
		defer conn.Close()
		got, err := io.ReadAll(conn)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		return string(got)
	}

	deadline := time.Now().Add(20 * time.Second)
	for !clientHasDirectPath(c) {
		if time.Now().After(deadline) {
			t.Fatal("client never reported a direct path")
		}
		dial()
		time.Sleep(50 * time.Millisecond)
	}

	if err := s.DisableDERP(); err != nil {
		t.Fatalf("server DisableDERP: %v", err)
	}
	if err := c.DisableDERP(); err != nil {
		t.Fatalf("client DisableDERP: %v", err)
	}
	for name, lb := range map[string]*locoBackend{"server": s.lb, "client": c.lb} {
		if n := lb.sys.MagicSock.Get().DERPs(); n != 0 {
			t.Fatalf("%s still has %d DERP connections", name, n)
		}
	}
	if got := dial(); got != "hello\n" {
		t.Fatalf("dial over the direct path after disabling DERP = %q", got)
	}
	if !clientHasDirectPath(c) {
		t.Fatal("direct path was lost after disabling DERP")
	}
}

func TestDisableDERPBeforeStartFails(t *testing.T) {
	if err := (&Server{}).DisableDERP(); err == nil {
		t.Fatal("DisableDERP on an unstarted server succeeded")
	}
	if err := (&Client{}).DisableDERP(); err == nil {
		t.Fatal("DisableDERP on an unstarted client succeeded")
	}
}

func clientHasDirectPath(c *Client) bool {
	st := c.PeerStatus()
	if st == nil {
		return false
	}
	for _, peer := range st.Peer {
		if peer != nil && peer.CurAddr != "" {
			return true
		}
	}
	return false
}

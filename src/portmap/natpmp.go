package portmap

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"time"
)

// Port 5351 serves both NAT-PMP (RFC 6886) and PCP (RFC 6887).
const gatewayPort = 5351

// natpmp maps TCP ports with NAT-PMP (RFC 6886).
type natpmp struct {
	gateway netip.AddrPort
}

func (natpmp) name() string { return "nat-pmp" }

func (m natpmp) add(ctx context.Context, port uint16, lifetime time.Duration) (netip.AddrPort, time.Duration, error) {
	external, err := m.externalAddress(ctx)
	if err != nil {
		return netip.AddrPort{}, 0, err
	}
	mapped, granted, err := m.mapTCP(ctx, port, port, lifetime)
	if err != nil {
		return netip.AddrPort{}, 0, err
	}
	return netip.AddrPortFrom(external, mapped), granted, nil
}

func (m natpmp) remove(ctx context.Context, port uint16) error {
	// RFC 6886 section 3.4: a zero lifetime and zero suggested external port
	// delete the mapping for the internal port.
	_, _, err := m.mapTCP(ctx, port, 0, 0)
	return err
}

func (m natpmp) externalAddress(ctx context.Context) (netip.Addr, error) {
	response, err := exchange(ctx, m.gateway, []byte{0, 0}, 12)
	if err != nil {
		return netip.Addr{}, err
	}
	if err := natpmpResult(response, 128); err != nil {
		return netip.Addr{}, err
	}
	return netip.AddrFrom4([4]byte(response[8:12])), nil
}

func (m natpmp) mapTCP(ctx context.Context, internal, external uint16, lifetime time.Duration) (uint16, time.Duration, error) {
	request := make([]byte, 12)
	request[1] = 2 // map TCP
	binary.BigEndian.PutUint16(request[4:], internal)
	binary.BigEndian.PutUint16(request[6:], external)
	binary.BigEndian.PutUint32(request[8:], uint32(lifetime/time.Second))
	response, err := exchange(ctx, m.gateway, request, 16)
	if err != nil {
		return 0, 0, err
	}
	if err := natpmpResult(response, 130); err != nil {
		return 0, 0, err
	}
	if got := binary.BigEndian.Uint16(response[8:]); got != internal {
		return 0, 0, fmt.Errorf("nat-pmp: response for internal port %d, want %d", got, internal)
	}
	mapped := binary.BigEndian.Uint16(response[10:])
	granted := time.Duration(binary.BigEndian.Uint32(response[12:])) * time.Second
	return mapped, granted, nil
}

func natpmpResult(response []byte, opcode byte) error {
	if response[0] != 0 || response[1] != opcode {
		return fmt.Errorf("nat-pmp: unexpected response version %d opcode %d", response[0], response[1])
	}
	if code := binary.BigEndian.Uint16(response[2:]); code != 0 {
		return fmt.Errorf("nat-pmp: gateway returned result code %d", code)
	}
	return nil
}

// exchange sends request to the gateway and waits for a response of at least
// minSize bytes, retrying with the RFC 6886 initial 250 ms interval doubling
// until ctx ends.
func exchange(ctx context.Context, gateway netip.AddrPort, request []byte, minSize int) ([]byte, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "udp", gateway.String())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.SetDeadline(time.Now()) })
	defer stop()
	buf := make([]byte, 1100)
	for wait := 250 * time.Millisecond; ; wait *= 2 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, err := conn.Write(request); err != nil {
			return nil, err
		}
		if err := conn.SetReadDeadline(time.Now().Add(wait)); err != nil {
			return nil, err
		}
		for {
			n, err := conn.Read(buf)
			if err != nil {
				var timeout net.Error
				if errors.As(err, &timeout) && timeout.Timeout() && ctx.Err() == nil {
					break
				}
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, err
			}
			if n >= minSize {
				return append([]byte(nil), buf[:n]...), nil
			}
		}
	}
}

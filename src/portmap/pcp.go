package portmap

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net/netip"
	"sync"
	"time"
)

const (
	pcpVersion     = 2
	pcpOpcodeMap   = 1
	pcpResponseBit = 0x80
	pcpMapSize     = 60
	pcpProtocolTCP = 6
)

// pcp maps TCP ports with the PCP MAP opcode (RFC 6887). Each internal port
// keeps one nonce so renewals and deletion refer to the same mapping.
type pcp struct {
	gateway netip.AddrPort
	client  netip.Addr

	mu     sync.Mutex
	nonces map[uint16][12]byte
}

func newPCP(gateway netip.AddrPort, client netip.Addr) *pcp {
	return &pcp{gateway: gateway, client: client, nonces: make(map[uint16][12]byte)}
}

func (*pcp) name() string { return "pcp" }

func (m *pcp) add(ctx context.Context, port uint16, lifetime time.Duration) (netip.AddrPort, time.Duration, error) {
	return m.mapTCP(ctx, port, port, lifetime)
}

func (m *pcp) remove(ctx context.Context, port uint16) error {
	_, _, err := m.mapTCP(ctx, port, 0, 0)
	return err
}

func (m *pcp) nonce(port uint16) ([12]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if nonce, ok := m.nonces[port]; ok {
		return nonce, nil
	}
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nonce, err
	}
	m.nonces[port] = nonce
	return nonce, nil
}

func (m *pcp) mapTCP(ctx context.Context, internal, external uint16, lifetime time.Duration) (netip.AddrPort, time.Duration, error) {
	nonce, err := m.nonce(internal)
	if err != nil {
		return netip.AddrPort{}, 0, err
	}
	request := make([]byte, pcpMapSize)
	request[0] = pcpVersion
	request[1] = pcpOpcodeMap
	binary.BigEndian.PutUint32(request[4:], uint32(lifetime/time.Second))
	client := m.client.As16()
	copy(request[8:24], client[:])
	copy(request[24:36], nonce[:])
	request[36] = pcpProtocolTCP
	binary.BigEndian.PutUint16(request[40:], internal)
	binary.BigEndian.PutUint16(request[42:], external)
	// Suggested external address: the all-zero IPv4-mapped address lets the
	// gateway choose.
	request[54], request[55] = 0xff, 0xff

	response, err := exchange(ctx, m.gateway, request, pcpMapSize)
	if err != nil {
		return netip.AddrPort{}, 0, err
	}
	if response[0] != pcpVersion || response[1] != pcpOpcodeMap|pcpResponseBit {
		return netip.AddrPort{}, 0, fmt.Errorf("pcp: unexpected response version %d opcode %d", response[0], response[1])
	}
	if code := response[3]; code != 0 {
		return netip.AddrPort{}, 0, fmt.Errorf("pcp: gateway returned result code %d", code)
	}
	if !bytes.Equal(response[24:36], nonce[:]) || response[36] != pcpProtocolTCP || binary.BigEndian.Uint16(response[40:]) != internal {
		return netip.AddrPort{}, 0, fmt.Errorf("pcp: response does not match the request for port %d", internal)
	}
	granted := time.Duration(binary.BigEndian.Uint32(response[4:])) * time.Second
	addr := netip.AddrFrom16([16]byte(response[44:60])).Unmap()
	return netip.AddrPortFrom(addr, binary.BigEndian.Uint16(response[42:])), granted, nil
}

package common

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"os"
	"time"

	"github.com/katzenpost/katzenpost/core/crypto/rand"
	"github.com/katzenpost/katzenpost/core/worker"
	"github.com/katzenpost/katzenpost/http/common"
	quic "github.com/quic-go/quic-go"
)

var errHalted = errors.New("Halted")

type Transport interface {
	Accept(context.Context) net.Conn
	Dial(context.Context, net.Addr) net.Conn
}

// this type implements net.PacketConn and sends and receives QUIC protocol messages.
// Method ProxyTo(conn) returns a net.Conn wrapping a QUIC Stream.
// Method ProxyFrom(conn) returns a net.Conn wrapping a QUIC Stream from the listener
// uses sends and receives QUIC messages exposes methods WriteMessage and ReadMessage that an application
type QUICProxyConn struct {
	worker.Worker

	qcfg          *quic.Config
	tlsConf       *tls.Config
	localAddr     net.Addr
	remoteAddr    net.Addr
	readDeadline  time.Time
	writeDeadline time.Time

	// channels for payloads
	incoming chan *pkt
	outgoing chan *pkt
}

type pkt struct {
	payload []byte
	src     net.Addr
	dst     net.Addr
}

func UniqAddr(entropy []byte) net.Addr {
	return &uniqAddr{r: base64.StdEncoding.EncodeToString(entropy)}
}

// uniqAddr is a non-routable unique identifier to associate this connection with
type uniqAddr struct {
	r string
}

// Network() implements net.Addr
func (w *uniqAddr) Network() string {
	return "katzenpost"
}

// String() implements net.Addr
func (w *uniqAddr) String() string {
	if w.r == "" {
		ip := make([]byte, 20)
		io.ReadFull(rand.Reader, ip)
		w.r = base64.StdEncoding.EncodeToString(ip)
	}
	return w.r
}

// NewQUICProxyConn returns a
func NewQUICProxyConn() *QUICProxyConn {
	addr := &uniqAddr{}

	return &QUICProxyConn{localAddr: addr, incoming: make(chan *pkt), outgoing: make(chan *pkt),
		tlsConf: common.GenerateTLSConfig()}
}

func (k *QUICProxyConn) Config() *quic.Config {
	return k.qcfg
}

func (k *QUICProxyConn) TLSConfig() *tls.Config {
	if k.tlsConf == nil {
		k.tlsConf = common.GenerateTLSConfig()
	}
	return k.tlsConf
}

func (k *QUICProxyConn) SetReadBuffer(bytes int) error {
	return nil
}

func (k *QUICProxyConn) SetWriteBuffer(bytes int) error {
	return nil
}

// WritePacket into QUICProxyConn
func (k *QUICProxyConn) WritePacket(p []byte, addr net.Addr) (int, error) {
	select {
	case k.incoming <- &pkt{payload: p, src: addr}:
	case <-k.HaltCh():
		return 0, errHalted
	}
	return len(p), nil
}

// ReadPacket from QUICProxyConn
func (k *QUICProxyConn) ReadPacket(p []byte) (int, net.Addr, error) {
	select {
	case pkt := <-k.outgoing:
		return copy(p, pkt.payload), pkt.dst, nil
	case <-k.HaltCh():
		return 0, nil, errHalted
	}
}

// ReadFrom implements net.PacketConn
func (k *QUICProxyConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	if k.readDeadline.Unix() == (&time.Time{}).Unix() {
		select {
		case pkt := <-k.incoming:
			return copy(p, pkt.payload), pkt.src, nil
		case <-k.HaltCh():
			return 0, nil, errHalted
		}
	} else {
		select {
		case pkt := <-k.incoming:
			return copy(p, pkt.payload), pkt.src, nil
		case <-k.HaltCh():
			return 0, nil, errHalted
		case <-time.After(k.readDeadline.Sub(time.Now())):
			return 0, nil, os.ErrDeadlineExceeded
		}
	}
}

// WriteTo implements net.PacketConn
func (k *QUICProxyConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	p2 := &pkt{payload: make([]byte, len(p)), dst: addr}
	copy(p2.payload, p)

	if k.writeDeadline.Unix() == (&time.Time{}).Unix() {
		select {
		case k.outgoing <- p2:
			return len(p2.payload), nil
		case <-k.HaltCh():
			return 0, errHalted
		}
	} else {
		select {
		case k.outgoing <- p2:
			return len(p2.payload), nil
		case <-time.After(k.writeDeadline.Sub(time.Now())):
			return 0, os.ErrDeadlineExceeded
		case <-k.HaltCh():
			return 0, errHalted
		}
	}
}

// Close implements net.PacketConn
func (k *QUICProxyConn) Close() error {
	return nil
}

// LocalAddr implements net.PacketConn
func (k *QUICProxyConn) LocalAddr() net.Addr {
	return k.localAddr
}

// SetDeadline implements net.PacketConn
func (k *QUICProxyConn) SetDeadline(t time.Time) error {
	k.readDeadline = t
	k.writeDeadline = t
	return nil
}

// SetReadDeadline implements net.PacketConn
func (k *QUICProxyConn) SetReadDeadline(t time.Time) error {
	k.readDeadline = t
	return nil
}

// SetWriteDeadline implements net.PacketConn
func (k *QUICProxyConn) SetWriteDeadline(t time.Time) error {
	k.writeDeadline = t
	return nil
}

// Accept is for the Receiver side of Transport and returns a net.Conn after handshaking
func (k *QUICProxyConn) Accept(ctx context.Context) (net.Conn, error) {
	// start quic Listener
	l, err := quic.Listen(k, k.tlsConf, nil)
	if err != nil {
		return nil, err
	}

	for {
		select {
		case <-k.HaltCh():
			return nil, errHalted
		case <-ctx.Done():
			return nil, os.ErrDeadlineExceeded
		default:
		}
		c, err := l.Accept(ctx)
		if e, ok := err.(net.Error); ok && e.Timeout() {
			continue
		}
		if err != nil {
			return nil, err
		}
		k.remoteAddr = c.RemoteAddr()

		// accept stream
		s, err := c.AcceptStream(ctx)
		if e, ok := err.(net.Error); ok && e.Timeout() {
			continue
		}
		if err != nil {
			return nil, err
		}

		qc := &common.QuicConn{Stream: s, Conn: c}
		return qc, nil
	}
}

// Dial is for the Client side of Transport and returns a net.Conn after handshaking
func (k *QUICProxyConn) Dial(ctx context.Context, addr net.Addr) (net.Conn, error) {
	if addr == nil {
		return nil, errors.New("Dial() called with nil net.Addr")
	}
	k.remoteAddr = addr
	for {
		select {
		case <-k.HaltCh():
			return nil, errHalted
		case <-ctx.Done():
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, errors.New("Cancelled")
		default:
		}

		c, err := quic.Dial(ctx, k, addr, k.tlsConf, k.Config())
		if e, ok := err.(net.Error); ok && e.Timeout() {
			continue
		}
		if err != nil {
			return nil, err
		}

		s, err := c.OpenStream()
		if e, ok := err.(net.Error); ok && e.Timeout() {
			continue
		}
		if err != nil {
			return nil, err
		}
		return &common.QuicConn{Stream: s, Conn: c}, nil
	}
}
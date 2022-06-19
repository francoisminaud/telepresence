package forwarder

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/ipproto"
	"github.com/telepresenceio/telepresence/v2/pkg/iputil"
	"github.com/telepresenceio/telepresence/v2/pkg/tunnel"
)

type udp struct {
	base
	targets *tunnel.Pool
}

func newUDP(listen *net.UDPAddr, targetHost string, targetPort uint16) Interceptor {
	return &udp{
		base: base{
			listenAddr: listen,
			targetHost: targetHost,
			targetPort: targetPort,
		},
		targets: tunnel.NewPool(),
	}
}

func (f *udp) Serve(ctx context.Context) error {
	// Set up listener lifetime (same as the overall forwarder lifetime)
	f.mu.Lock()
	la := f.listenAddr
	ctx, f.lCancel = context.WithCancel(ctx)
	ctx = dlog.WithField(ctx, "lis", la.String())
	f.ctx = ctx

	// Set up target lifetime
	f.tCtx, f.tCancel = context.WithCancel(ctx)
	f.mu.Unlock()

	defer func() {
		f.lCancel()
		f.targets.CloseAll(ctx)
		dlog.Infof(ctx, "Done forwarding udp from %s", la)
	}()

	dlog.Infof(ctx, "Forwarding udp from %s", la)
	for {
		f.mu.Lock()
		ctx = f.tCtx
		intercept := f.intercept
		f.mu.Unlock()
		if ctx.Err() != nil {
			return nil
		}
		if err := f.forward(ctx, intercept); err != nil {
			return err
		}
	}
}

func (f *udp) forward(ctx context.Context, intercept *manager.InterceptInfo) error {
	la := f.listenAddr
	lc := net.ListenConfig{}
	ag, err := lc.ListenPacket(ctx, la.Network(), la.String())
	if err != nil {
		return err
	}
	agentConn := ag.(*net.UDPConn)
	defer agentConn.Close()
	if intercept != nil {
		return f.interceptConn(ctx, agentConn, intercept)
	}

	targetAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", f.targetHost, f.targetPort))
	if err != nil {
		return fmt.Errorf("error on resolve(%s:%d): %w", f.targetHost, f.targetPort, err)
	}

	dlog.Infof(ctx, "Forwarding udp from %s to %s", la, targetAddr)
	defer func() {
		f.targets.CloseAll(ctx)
		dlog.Infof(ctx, "Done forwarding udp from %s to %s", la, targetAddr)
	}()

	ch := make(chan tunnel.UdpReadResult)
	go tunnel.UdpReader(ctx, agentConn, ch)
	for {
		select {
		case <-ctx.Done():
			return nil
		case rr, ok := <-ch:
			if !ok {
				return nil
			}
			if err := f.sendPacket(ctx, agentConn, rr.Addr, targetAddr, rr.Payload); err != nil {
				return err
			}
		}
	}
}

func (f *udp) sendPacket(ctx context.Context, agentConn *net.UDPConn, src, dest *net.UDPAddr, payload []byte) error {
	id := tunnel.NewConnID(ipproto.UDP, src.IP, dest.IP, uint16(src.Port), uint16(dest.Port))
	dlog.Tracef(ctx, "<- SRC udp %s, len %d", id, len(payload))
	target, _, err := f.targets.GetOrCreate(ctx, id, func(ctx context.Context, release func()) (tunnel.Handler, error) {
		tc, err := net.DialUDP("udp", nil, id.DestinationAddr().(*net.UDPAddr))
		if err != nil {
			return nil, err
		}
		return &udpHandler{
			UDPConn:   tc,
			id:        id,
			replyWith: agentConn,
			release:   release,
		}, nil
	})
	if err != nil {
		return err
	}
	n, err := target.(*udpHandler).Write(payload)
	if err != nil {
		dlog.Errorf(ctx, "!! TRG udp %s write: %v", id, err)
		return err
	}
	dlog.Tracef(ctx, "-> TRG udp %s, len %d", id, n)
	return nil
}

type udpHandler struct {
	*net.UDPConn
	id        tunnel.ConnID
	replyWith net.PacketConn
	release   func()
}

func (u *udpHandler) Close() error {
	u.release()
	return u.UDPConn.Close()
}

func (u *udpHandler) Stop(_ context.Context) {
	_ = u.Close()
}

func (u *udpHandler) Start(ctx context.Context) {
	ch := make(chan tunnel.UdpReadResult)
	go tunnel.UdpReader(ctx, u, ch)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case rr, ok := <-ch:
				if !ok {
					return
				}
				dlog.Tracef(ctx, "<- TRG udp %s, len %d", u.id, len(rr.Payload))
				n, werr := u.replyWith.WriteTo(rr.Payload, u.id.SourceAddr())
				if werr != nil {
					dlog.Errorf(ctx, "!! SRC udp %s write: %v", u.id, werr)
					return
				}
				dlog.Tracef(ctx, "-> SRC udp %s, len %d", u.id, n)
			}
		}
	}()
}

func (f *udp) interceptConn(ctx context.Context, conn *net.UDPConn, iCept *manager.InterceptInfo) error {
	spec := iCept.Spec
	destIP := iputil.Parse(spec.TargetHost)
	destPort := uint16(spec.TargetPort)

	dlog.Infof(ctx, "Forwarding udp from %s to %s %s:%d", conn.LocalAddr(), spec.Client, destIP, destPort)
	defer dlog.Infof(ctx, "Done forwarding udp from %s to %s %s:%d", conn.LocalAddr(), spec.Client, destIP, destPort)
	d := tunnel.NewUDPListener(conn, destIP, destPort, func(ctx context.Context, id tunnel.ConnID) (tunnel.Stream, error) {
		ms, err := f.manager.Tunnel(ctx)
		if err != nil {
			return nil, fmt.Errorf("call to manager.Tunnel() failed. Id %s: %v", id, err)
		}
		s, err := tunnel.NewClientStream(ctx, ms, id, f.sessionInfo.SessionId, time.Duration(spec.RoundtripLatency), time.Duration(spec.DialTimeout))
		if err != nil {
			return nil, err
		}
		if err = s.Send(ctx, tunnel.SessionMessage(iCept.ClientSession.SessionId)); err != nil {
			return nil, fmt.Errorf("unable to send client session id. Id %s: %v", id, err)
		}
		return s, nil
	})
	d.Start(ctx)
	<-d.Done()
	return nil
}

package forwarder

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/blang/semver"

	"github.com/datawire/dlib/dlog"
	"github.com/telepresenceio/telepresence/rpc/v2/manager"
	"github.com/telepresenceio/telepresence/v2/pkg/restapi"
)

type Interceptor interface {
	io.Closer
	SetManager(sessionInfo *manager.SessionInfo, manager manager.ManagerClient, version semver.Version)
	InterceptId() (id string)
	InterceptInfo() *restapi.InterceptInfo
	Serve(ctx context.Context) error
	SetIntercepting(intercept *manager.InterceptInfo)
	Target() (string, uint16)
}

type base struct {
	mu sync.Mutex

	ctx        context.Context
	lCancel    context.CancelFunc
	listenAddr net.Addr
	tCtx       context.Context
	tCancel    context.CancelFunc

	targetHost string
	targetPort uint16

	manager     manager.ManagerClient
	sessionInfo *manager.SessionInfo

	intercept  *manager.InterceptInfo
	mgrVersion semver.Version
}

func NewInterceptor(addr net.Addr, targetHost string, targetPort uint16) Interceptor {
	switch addr := addr.(type) {
	case *net.TCPAddr:
		return newTCP(addr, targetHost, targetPort)
	case *net.UDPAddr:
		return newUDP(addr, targetHost, targetPort)
	default:
		panic(fmt.Errorf("unsupported net.Addr type %T", addr))
	}
}

func (f *base) SetManager(sessionInfo *manager.SessionInfo, manager manager.ManagerClient, version semver.Version) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sessionInfo = sessionInfo
	f.manager = manager
	f.mgrVersion = version
}

func (f *base) Close() error {
	f.lCancel()
	return nil
}

func (f *base) Target() (string, uint16) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.targetHost, f.targetPort
}

func (f *base) InterceptInfo() *restapi.InterceptInfo {
	ii := &restapi.InterceptInfo{}
	f.mu.Lock()
	if f.intercept != nil {
		ii.Intercepted = true
		ii.Metadata = f.intercept.Metadata
	}
	f.mu.Unlock()
	return ii
}

func (f *base) InterceptId() (id string) {
	f.mu.Lock()
	if f.intercept != nil {
		id = f.intercept.Id
	}
	f.mu.Unlock()
	return id
}

func (f *base) SetIntercepting(intercept *manager.InterceptInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()

	iceptInfo := func(ii *manager.InterceptInfo) string {
		is := ii.Spec
		return fmt.Sprintf("'%s' (%s:%d)", is.Name, is.Client, is.TargetPort)
	}
	if intercept == nil {
		if f.intercept == nil {
			return
		}
		dlog.Debugf(f.ctx, "Forward target changed from intercept %s to %s:%d", iceptInfo(f.intercept), f.targetHost, f.targetPort)
	} else {
		if f.intercept == nil {
			dlog.Debugf(f.ctx, "Forward target changed from %s:%d to intercept %s", f.targetHost, f.targetPort, iceptInfo(intercept))
		} else {
			if f.intercept.Id == intercept.Id {
				return
			}
			dlog.Debugf(f.ctx, "Forward target changed from intercept %s to intercept %q", iceptInfo(f.intercept), iceptInfo(intercept))
		}
	}

	// Drop existing connections
	f.tCancel()

	// Set up new target and lifetime
	f.tCtx, f.tCancel = context.WithCancel(f.ctx)
	f.intercept = intercept
}

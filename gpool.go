package pool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	// ErrClosed is error which pool has been closed but still been used
	ErrClosed = errors.New("pool has been closed")
	// ErrNil is error which pool is nil but has been used
	ErrNil = errors.New("pool is nil")
)

// Config used for config the connection pool
type Config struct {
	Network string
	Address string
	// InitCap of the connection pool
	InitCap int
	// Maxcap is max connection number of the pool
	MaxCap int
	// WaitTimeout is the timeout for waiting to borrow a connection
	WaitTimeout time.Duration
	// IdleTimeout is the timeout for a connection to be alive
	IdleTimeout time.Duration
}

//Pool store connections and pool info
type Pool struct {
	conns     chan net.Conn
	factory   Factory
	mu        sync.RWMutex
	config    *Config
	idleConns int
	createNum int
	//will be used for blocking calls
	remainingSpace chan bool
}

// Factory generate a new connection
type Factory func(network, address string) (net.Conn, error)

func (p *Pool) addRemainingSpace() {
	p.remainingSpace <- true
}

func (p *Pool) removeRemainingSpace() {
	<-p.remainingSpace
}

// New create a connection pool
func New(pc *Config) (*Pool, error) {
	// test initCap and maxCap
	if pc.InitCap < 0 || pc.MaxCap < 0 || pc.InitCap > pc.MaxCap {
		return nil, errors.New("invalid capacity setting")
	}
	p := &Pool{
		conns:          make(chan net.Conn, pc.MaxCap),
		config:         pc,
		idleConns:      pc.InitCap,
		remainingSpace: make(chan bool, pc.MaxCap),
	}

	p.factory = func(network, address string) (net.Conn, error) {
		return net.Dial(network, address)
	}

	//fill the remainingSpace channel so we can use it for blocking calls
	for i := 0; i < pc.MaxCap; i++ {
		p.addRemainingSpace()
	}

	// create initial connection, if wrong just close it
	for i := 0; i < pc.InitCap; i++ {
		log.WithFields(log.Fields{
			"Network": pc.Network,
			"Address": pc.Address,
		}).Info("Creating connection")
		conn, err := p.factory(pc.Network, pc.Address)
		p.removeRemainingSpace()
		if err != nil {
			p.Close()
			p.addRemainingSpace()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		p.createNum = pc.InitCap
		p.conns <- conn
	}
	return p, nil
}

// wrapConn wraps a standard net.Conn to a poolConn net.Conn.
func (p *Pool) wrapConn(conn net.Conn) *GConn {
	log.WithFields(log.Fields{
		"Connection Id": conn,
		"Address":       conn.RemoteAddr(),
	}).Info("Wrapping connection")
	gconn := &GConn{p: p}
	gconn.Conn = conn
	return gconn
}

// getConnsAndFactory get conn channel and factory by once
func (p *Pool) getConnsAndFactory() (chan net.Conn, Factory) {
	p.mu.RLock()
	conns := p.conns
	factory := p.factory
	p.mu.RUnlock()
	return conns, factory
}

// Return return the connection back to the pool. If the pool is full or closed,
// conn is simply closed. A nil conn will be rejected.
func (p *Pool) Return(conn net.Conn) error {
	if conn == nil {
		log.WithFields(log.Fields{
			"Connection Id": conn,
			"Address":       conn.RemoteAddr(),
			"Error":         "connection is nil. rejecting",
		}).Info("Returning connection to pool")
		return errors.New("connection is nil. rejecting")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conns == nil {
		log.WithFields(log.Fields{
			"Connection Id": conn,
			"Address":       conn.RemoteAddr(),
			"Error":         "pool is closed, close passed connection",
		}).Info("Pool closed")
		// pool is closed, close passed connection
		return conn.Close()
	}

	// put the resource back into the pool. If the pool is full, this will
	// block and the default case will be executed.
	select {
	case p.conns <- conn:
		p.idleConns++
		return nil
	default:
		// pool is full, close passed connection
		log.WithFields(log.Fields{
			"Connection Id": conn,
			"Address":       conn.RemoteAddr(),
			"Error":         "pool is full, close passed connection",
		}).Info("Pool Full")
		return conn.Close()
	}
}

// Get implement Pool get interface
// if don't have any connection available, it will try to new one
func (p *Pool) Get() (*GConn, error) {
	conns, factory := p.getConnsAndFactory()
	if conns == nil {
		return nil, ErrNil
	}
	// wrap our connections with out custom net.Conn implementation (wrapConn
	// method) that puts the connection back to the pool if it's closed.
	select {
	case conn := <-conns:
		if conn == nil {
			return nil, ErrClosed
		}
		log.WithFields(log.Fields{
			"Connection Id": conn,
			"Address":       conn.RemoteAddr(),
			"Status":        "found existing connection",
		}).Info("Get Connection")
		p.mu.Lock()
		p.idleConns--
		p.mu.Unlock()
		return p.wrapConn(conn), nil
	default:
		p.mu.Lock()
		defer p.mu.Unlock()
		p.createNum++
		if p.createNum > p.config.MaxCap {
			return nil, errors.New("More than MaxCap")
		}
		conn, err := factory(p.config.Network, p.config.Address)
		log.WithFields(log.Fields{
			"Connection Id": conn,
			"Address":       conn.RemoteAddr(),
			"Status":        "no connection found, creating new one",
		}).Info("Get Connection")
		p.removeRemainingSpace()
		if err != nil {
			p.addRemainingSpace()
			return nil, err
		}

		return p.wrapConn(conn), nil
	}
}

// BlockingGet will block until it gets an idle connection from pool. Context timeout can be passed with context
// to wait for specific amount of time. If nil is passed, this will wait indefinitely until a connection is
// available.
func (p *Pool) BlockingGet(ctx context.Context) (*GConn, error) {
	conns, factory := p.getConnsAndFactory()
	if conns == nil {
		return nil, ErrNil
	}
	//if context is nil it means we have no timeout, we can wait indefinitely
	if ctx == nil {
		ctx = context.Background()
	}

	// wrap our connections with out custom net.Conn implementation (wrapConn
	// method) that puts the connection back to the pool if it's closed.
	select {
	case conn := <-conns:
		if conn == nil {
			return nil, ErrClosed
		}
		log.WithFields(log.Fields{
			"Connection Id": conn,
			"Address":       conn.RemoteAddr(),
			"Status":        "found existing connection",
		}).Info("BlockingGet Connection")
		p.mu.Lock()
		p.idleConns--
		p.mu.Unlock()
		return p.wrapConn(conn), nil
	case _ = <-p.remainingSpace:
		p.mu.Lock()
		defer p.mu.Unlock()
		p.createNum++
		conn, err := factory(p.config.Network, p.config.Address)
		if err != nil {
			p.addRemainingSpace()
			return nil, err
		}
		log.WithFields(log.Fields{
			"Connection Id": conn,
			"Address":       conn.RemoteAddr(),
			"Status":        "no connection found, creating new one",
		}).Info("BlockingGet Connection")
		return p.wrapConn(conn), nil
	//if context deadline is reached, return timeout error
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close implement Pool close interface
// it will close all the connection in the pool
func (p *Pool) Close() {
	p.mu.Lock()
	conns := p.conns
	p.conns = nil
	p.factory = nil
	p.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for conn := range conns {
		conn.Close()
		p.addRemainingSpace()
	}
}

// Len implement Pool Len interface
// it will return current length of the pool
func (p *Pool) Len() int {
	conns, _ := p.getConnsAndFactory()
	return len(conns)
}

// Idle implement Pool Idle interface
// it will return current idle length of the pool
func (p *Pool) Idle() int {
	p.mu.Lock()
	idle := p.idleConns
	p.mu.Unlock()
	return int(idle)
}

package main

import (
	"log"
	"net"
	"sync"
)

type ConnPool struct {
	maxConns      int
	conns         map[*ConnHandler]struct{}
	newConn       chan struct{}
	connsToHandle chan *ConnHandler
	errs          chan error
	rw            sync.RWMutex
	listener      net.Listener
	shutdown      bool
}

func NewConnPoll(maxConns int, ln net.Listener) *ConnPool {
	conMap := make(map[*ConnHandler]struct{}, maxConns)
	return &ConnPool{
		maxConns:      maxConns,
		conns:         conMap,
		newConn:       make(chan struct{}, 1),
		connsToHandle: make(chan *ConnHandler),
		errs:          make(chan error),
		rw:            sync.RWMutex{},
		listener:      ln,
	}
}

func (c *ConnPool) Accept() {
	if c.MoreConnsAllowed() {
		log.Printf("Listening for connections...")
		conn, err := c.listener.Accept()
		// don't handle connection errors, just bail
		if err != nil {
			c.errs <- err
			c.RequestNewConnection()
			return
		}
		nc := &ConnHandler{
			conn: conn,
		}
		c.connsToHandle <- nc
	}
	// queue up another listener
	c.RequestNewConnection()
}

func (c *ConnPool) RequestNewConnection() {
	// Don't request again if there's already
	// a pending request
	if len(c.newConn) > 0 {
		return
	}
	// Don't request if we are shutting down
	if c.shutdown {
		return
	}
	if c.MoreConnsAllowed() {
		log.Printf("Queued up request for connection")
		c.newConn <- struct{}{}
	}
}

func (c *ConnPool) Register(ch *ConnHandler) bool {
	c.rw.Lock()
	defer c.rw.Unlock()
	if len(c.conns) <= 3 {
		c.conns[ch] = struct{}{}
		return true
	}
	return false
}

func (c *ConnPool) Shutdown() {
	c.rw.Lock()
	defer c.rw.Unlock()
	c.shutdown = true
	for c := range c.conns {
		log.Printf("Pool: shutting down connection %v", c.conn.RemoteAddr())
		c.shutdown <- struct{}{}
	}
}

func (c *ConnPool) Deregister(ch *ConnHandler) {
	c.rw.Lock()
	defer c.rw.Unlock()
	delete(c.conns, ch)
}

func (c *ConnPool) MoreConnsAllowed() bool {
	c.rw.RLock()
	defer c.rw.RUnlock()
	if len(c.conns) < c.maxConns {
		return true
	}
	return false
}

func (c *ConnPool) ActiveConnections() int {
	c.rw.RLock()
	defer c.rw.RUnlock()
	return len(c.conns)
}

func (c *ConnPool) NewConnection() (ConnHandler, bool) {
	var ch ConnHandler
	if !c.MoreConnsAllowed() {
		return ch, false
	}
	return ch, true
}

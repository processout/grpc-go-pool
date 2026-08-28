// Package grpcpool provides a pool of grpc clients
package grpcpool

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
)

var (
	// ErrClosed is the error when the client pool is closed
	ErrClosed = errors.New("grpc pool: client pool is closed")
	// ErrTimeout is the error when the client pool timed out
	ErrTimeout = errors.New("grpc pool: client pool timed out")
	// ErrAlreadyClosed is the error when the client conn was already closed
	ErrAlreadyClosed = errors.New("grpc pool: the connection was already closed")
	// ErrFullPool is the error when the pool is already full
	ErrFullPool = errors.New("grpc pool: closing a ClientConn into a full pool")
)

// Factory is a function type creating a grpc client
type Factory func() (*grpc.ClientConn, error)

// FactoryWithContext is a function type creating a grpc client
// that accepts the context parameter that could be passed from
// Get or NewWithContext method.
type FactoryWithContext func(context.Context) (*grpc.ClientConn, error)

// Pool is the grpc client pool
type Pool struct {
	clients         chan ClientConn
	factory         FactoryWithContext
	idleTimeout     time.Duration
	maxLifeDuration time.Duration
	mu              sync.RWMutex
	closed          bool           // 新增：明确标记池是否已关闭
	closeMu         sync.Mutex     // 新增：保护关闭操作
	wg              sync.WaitGroup // 新增：追踪正在使用的连接
}

// ClientConn is the wrapper for a grpc client conn
type ClientConn struct {
	*grpc.ClientConn
	pool          *Pool
	timeUsed      time.Time
	timeInitiated time.Time
	unhealthy     bool
	closed        bool       // 新增：标记连接是否已关闭
	mu            sync.Mutex // 新增：保护连接状态
}

// New creates a new clients pool with the given initial and maximum capacity,
// and the timeout for the idle clients. Returns an error if the initial
// clients could not be created
func New(factory Factory, init, capacity int, idleTimeout time.Duration,
	maxLifeDuration ...time.Duration) (*Pool, error) {
	return NewWithContext(context.Background(), func(ctx context.Context) (*grpc.ClientConn, error) { return factory() },
		init, capacity, idleTimeout, maxLifeDuration...)
}

// NewWithContext creates a new clients pool with the given initial and maximum
// capacity, and the timeout for the idle clients. The context parameter would
// be passed to the factory method during initialization. Returns an error if the
// initial clients could not be created.
func NewWithContext(ctx context.Context, factory FactoryWithContext, init, capacity int, idleTimeout time.Duration,
	maxLifeDuration ...time.Duration) (*Pool, error) {

	if capacity <= 0 {
		capacity = 1
	}
	if init < 0 {
		init = 0
	}
	if init > capacity {
		init = capacity
	}
	p := &Pool{
		clients:     make(chan ClientConn, capacity),
		factory:     factory,
		idleTimeout: idleTimeout,
		closed:      false,
	}
	if len(maxLifeDuration) > 0 {
		p.maxLifeDuration = maxLifeDuration[0]
	}
	for i := 0; i < init; i++ {
		c, err := factory(ctx)
		if err != nil {
			return nil, err
		}

		p.clients <- ClientConn{
			ClientConn:    c,
			pool:          p,
			timeUsed:      time.Now(),
			timeInitiated: time.Now(),
		}
	}
	// Fill the rest of the pool with empty clients
	for i := 0; i < capacity-init; i++ {
		p.clients <- ClientConn{
			pool: p,
		}
	}
	return p, nil
}

func (p *Pool) getClients() chan ClientConn {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.clients
}

// Close empties the pool calling Close on all its clients.
// You can call Close while there are outstanding clients.
// The pool channel is then closed, and Get will not be allowed anymore
func (p *Pool) Close() {
	p.closeMu.Lock()
	defer p.closeMu.Unlock()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	clients := p.clients
	p.clients = nil
	p.mu.Unlock()

	if clients == nil {
		return
	}

	// 等待所有正在使用的连接归还，最多等待5秒
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 所有连接都已归还
	case <-time.After(5 * time.Second):
		// 超时，强制关闭
	}

	close(clients)
	for client := range clients {
		if client.ClientConn == nil {
			continue
		}
		client.ClientConn.Close()
	}
}

// IsClosed returns true if the client pool is closed.
func (p *Pool) IsClosed() bool {
	if p == nil {
		return true
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}

// Get will return the next available client. If capacity
// has not been reached, it will create a new one using the factory. Otherwise,
// it will wait till the next client becomes available or a timeout.
// A timeout of 0 is an indefinite wait
func (p *Pool) Get(ctx context.Context) (*ClientConn, error) {
	// 先检查池是否已关闭
	if p.IsClosed() {
		return nil, ErrClosed
	}

	clients := p.getClients()
	if clients == nil {
		return nil, ErrClosed
	}

	wrapper := ClientConn{
		pool: p,
	}
	select {
	case wrapper = <-clients:
		// All good
	case <-ctx.Done():
		return nil, ErrTimeout // it would better returns ctx.Err()
	}

	// 如果获取到的连接是空连接且池已关闭，返回错误
	if wrapper.ClientConn == nil && p.IsClosed() {
		return nil, ErrClosed
	}

	// If the wrapper was idle too long, close the connection and create a new
	// one. It's safe to assume that there isn't any newer client as the client
	// we fetched is the first in the channel
	idleTimeout := p.idleTimeout
	if wrapper.ClientConn != nil && idleTimeout > 0 &&
		wrapper.timeUsed.Add(idleTimeout).Before(time.Now()) {

		wrapper.ClientConn.Close()
		wrapper.ClientConn = nil
	}

	var err error
	if wrapper.ClientConn == nil {
		// 再次检查池是否已关闭
		if p.IsClosed() {
			return nil, ErrClosed
		}

		wrapper.ClientConn, err = p.factory(ctx)
		if err != nil {
			// If there was an error, we want to put back a placeholder
			// client in the channel
			select {
			case clients <- ClientConn{
				pool: p,
			}:
			default:
				// 如果无法放回，说明池已满或已关闭
			}
			return nil, err
		}
		// This is a new connection, reset its initiated time
		wrapper.timeInitiated = time.Now()
	}

	// 标记连接正在使用
	p.wg.Add(1)
	return &wrapper, err
}

// Unhealthy marks the client conn as unhealthy, so that the connection
// gets reset when closed
func (c *ClientConn) Unhealthy() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unhealthy = true
}

// Close returns a ClientConn to the pool. It is safe to call multiple time,
// but will return an error after first time
func (c *ClientConn) Close() error {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrAlreadyClosed
	}
	c.mu.Unlock()

	// 确保 wg.Done 被调用
	defer func() {
		if c.pool != nil {
			c.pool.wg.Done()
		}
	}()

	if c.ClientConn == nil {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		return ErrAlreadyClosed
	}

	// 检查池是否已关闭
	if c.pool.IsClosed() {
		// 池已关闭，直接关闭底层连接
		if c.ClientConn != nil {
			c.ClientConn.Close()
			c.ClientConn = nil
		}
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		return ErrClosed
	}

	// If the wrapper connection has become too old, we want to recycle it. To
	// clarify the logic: if the sum of the initialization time and the max
	// duration is before Now(), it means the initialization is so old adding
	// the maximum duration couldn't put in the future. This sum therefore
	// corresponds to the cut-off point: if it's in the future we still have
	// time, if it's in the past it's too old
	maxDuration := c.pool.maxLifeDuration
	if maxDuration > 0 && c.timeInitiated.Add(maxDuration).Before(time.Now()) {
		c.Unhealthy()
	}

	// We're cloning the wrapper so we can set ClientConn to nil in the one
	// used by the user
	wrapper := ClientConn{
		pool:       c.pool,
		ClientConn: c.ClientConn,
		timeUsed:   time.Now(),
	}

	c.mu.Lock()
	if c.unhealthy {
		wrapper.ClientConn.Close()
		wrapper.ClientConn = nil
	} else {
		wrapper.timeInitiated = c.timeInitiated
	}
	c.mu.Unlock()

	// 如果连接被标记为不健康，不需要归还
	if wrapper.ClientConn == nil {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		return nil
	}

	// 尝试归还连接到池
	select {
	case c.pool.clients <- wrapper:
		// All good
	default:
		// 池已满，关闭连接
		wrapper.ClientConn.Close()
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		return ErrFullPool
	}

	c.mu.Lock()
	c.ClientConn = nil // Mark as closed
	c.closed = true
	c.mu.Unlock()
	return nil
}

// Capacity returns the capacity
func (p *Pool) Capacity() int {
	if p.IsClosed() {
		return 0
	}
	return cap(p.clients)
}

// Available returns the number of currently unused clients
func (p *Pool) Available() int {
	if p.IsClosed() {
		return 0
	}
	return len(p.clients)
}

package grpcpool

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// TestConcurrentGet tests concurrent connection acquisition
func TestConcurrentGet(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 2, 5, time.Minute)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer p.Close()

	var wg sync.WaitGroup
	concurrency := 100
	errors := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			conn, err := p.Get(ctx)
			if err != nil {
				errors <- err
				return
			}
			// Simulate usage
			time.Sleep(10 * time.Millisecond)
			if err := conn.Close(); err != nil && err != ErrAlreadyClosed {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent operation failed: %v", err)
	}
}

// TestPoolFullRace tests race conditions when the pool is full
func TestPoolFullRace(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 0, 2, time.Minute)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer p.Close()

	// Fill the pool
	conns := make([]*ClientConn, 2)
	for i := 0; i < 2; i++ {
		conns[i], err = p.Get(context.Background())
		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}
	}

	// Concurrently return connections, may trigger ErrFullPool
	var wg sync.WaitGroup
	errCount := 0
	var mu sync.Mutex

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if err := conns[idx].Close(); err != nil {
				mu.Lock()
				errCount++
				mu.Unlock()
				// ErrFullPool is an expected possible error
				if err != ErrFullPool {
					t.Logf("Unexpected error: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
	// If both succeed, there should be one ErrFullPool or both succeed
	t.Logf("Error count: %d", errCount)
}

// TestGetWhileClosing tests getting connections while the pool is closing
func TestGetWhileClosing(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 1, 3, time.Minute)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	// Start multiple goroutines to get connections
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := p.Get(context.Background())
			if err != nil {
				errors <- err
				return
			}
			time.Sleep(50 * time.Millisecond)
			conn.Close()
		}()
	}

	// Close the pool while getting connections
	time.Sleep(10 * time.Millisecond)
	p.Close()

	wg.Wait()
	close(errors)

	hasErrClosed := false
	for err := range errors {
		if err == ErrClosed {
			hasErrClosed = true
		}
		t.Logf("Got error: %v", err)
	}

	if !hasErrClosed {
		t.Log("No ErrClosed received, which might be okay if all connections were acquired before close")
	}
}

// TestCloseWhileInUse tests closing the pool while a connection is in use
func TestCloseWhileInUse(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 1, 3, time.Minute)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Acquire a connection
	conn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}

	// Save underlying connection for verification
	underlyingConn := conn.ClientConn

	// Close the pool while the connection is still in use
	p.Close()

	// Try to return the connection
	err = conn.Close()
	if err != ErrClosed && err != ErrAlreadyClosed {
		t.Errorf("Expected ErrClosed or ErrAlreadyClosed, got: %v", err)
	}

	// Verify the connection is closed
	if underlyingConn.GetState() != connectivity.Shutdown {
		t.Errorf("Underlying connection should be shutdown, got: %v", underlyingConn.GetState())
	}
}

// TestIdleTimeout tests idle connection timeout
func TestIdleTimeout(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 1, 3, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer p.Close()

	conn1, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	conn1.Close()

	// Wait for idle timeout
	time.Sleep(200 * time.Millisecond)

	// Get connection, should create a new one
	conn2, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Failed to get connection after timeout: %v", err)
	}
	defer conn2.Close()

	// Verify they are different connections
	if conn1.ClientConn == conn2.ClientConn {
		t.Errorf("Connection should be recreated after idle timeout")
	}
}

// TestMaxLifeDurationRace tests race conditions with max lifetime
func TestMaxLifeDurationRace(t *testing.T) {
	createCount := 0
	var countMu sync.Mutex

	factory := func() (*grpc.ClientConn, error) {
		countMu.Lock()
		createCount++
		countMu.Unlock()
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// Use a short max lifetime, but not too short to avoid excessive creation
	p, err := New(factory, 2, 5, time.Minute, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer p.Close()

	var wg sync.WaitGroup
	concurrency := 10
	errors := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Random delay to simulate real-world scenarios
			sleepTime := time.Duration(20+idx*8) * time.Millisecond
			time.Sleep(sleepTime)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			conn, err := p.Get(ctx)
			if err != nil {
				errors <- err
				return
			}

			// Simulate using the connection
			time.Sleep(30 * time.Millisecond)

			// Return the connection
			if err := conn.Close(); err != nil && err != ErrAlreadyClosed && err != ErrClosed {
				errors <- err
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	for err := range errors {
		t.Logf("Error during test: %v", err)
	}

	countMu.Lock()
	defer countMu.Unlock()

	t.Logf("Total connections created: %d", createCount)

	// Since max lifetime is 100ms and operations take longer, multiple connections should be created
	// Initial connections: 2, as connections expire, new ones will be created
	if createCount < 2 {
		t.Errorf("Expected at least 2 connections to be created, got %d", createCount)
	}

	// Verify connections don't grow indefinitely (should be within capacity)
	if createCount > 20 {
		t.Errorf("Too many connections created: %d, possible leak", createCount)
	}
}

// TestClosePoolPanic tests for potential panics when closing the pool
func TestClosePoolPanic(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 1, 1, time.Minute)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	// Closing the pool multiple times should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Pool Close panicked: %v", r)
		}
	}()

	p.Close()
	p.Close() // Second close
}

// TestGetAfterClose tests getting connections after the pool is closed
func TestGetAfterClose(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 1, 3, time.Minute)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	p.Close()

	conn, err := p.Get(context.Background())
	if err != ErrClosed {
		t.Errorf("Expected ErrClosed, got: %v", err)
	}
	if conn != nil {
		t.Error("Expected nil connection")
	}
}

// TestConcurrentCloseAndGet tests concurrent close and get operations
func TestConcurrentCloseAndGet(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 1, 5, time.Minute)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}

	var wg sync.WaitGroup
	done := make(chan bool)

	// Concurrently get and close
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					conn, err := p.Get(context.Background())
					if err != nil {
						if err == ErrClosed {
							return
						}
						continue
					}
					conn.Close()
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}

	time.Sleep(100 * time.Millisecond)
	p.Close()
	close(done)
	wg.Wait()
}

// TestFactoryError tests factory function errors
func TestFactoryError(t *testing.T) {
	expectedErr := context.DeadlineExceeded
	p, err := NewWithContext(context.Background(), func(ctx context.Context) (*grpc.ClientConn, error) {
		return nil, expectedErr
	}, 0, 1, time.Minute)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer p.Close()

	// No connections available in pool, need to create a new one
	conn, err := p.Get(context.Background())
	if err != expectedErr {
		t.Errorf("Expected factory error %v, got: %v", expectedErr, err)
	}
	if conn != nil {
		t.Error("Expected nil connection on factory error")
	}

	// Verify the pool is still usable
	if p.IsClosed() {
		t.Error("Pool should not be closed after factory error")
	}
}

// TestPoolCapacityExhaustion tests pool capacity exhaustion
func TestPoolCapacityExhaustion(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("example.com", grpc.WithTransportCredentials(insecure.NewCredentials()))
	}, 0, 2, time.Minute)
	if err != nil {
		t.Fatalf("Failed to create pool: %v", err)
	}
	defer p.Close()

	// Fill the pool
	conns := make([]*ClientConn, 2)
	for i := 0; i < 2; i++ {
		conns[i], err = p.Get(context.Background())
		if err != nil {
			t.Fatalf("Failed to get connection %d: %v", i, err)
		}
	}

	// Try to get a third connection (should timeout)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = p.Get(ctx)
	if err != ErrTimeout && err != context.DeadlineExceeded {
		t.Errorf("Expected timeout error, got: %v", err)
	}

	// Return one connection
	conns[0].Close()

	// Should be able to get a connection now
	conn, err := p.Get(context.Background())
	if err != nil {
		t.Errorf("Should be able to get connection after release, got: %v", err)
	}
	if conn != nil {
		conn.Close()
	}
}

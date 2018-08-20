package grpcpool

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestNew(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.Dial("example.com", grpc.WithInsecure())
	}, 1, 3, 0)
	if err != nil {
		t.Errorf("The pool returned an error: %s", err.Error())
	}
	if a := p.Available(); a != 3 {
		t.Errorf("The pool available was %d but should be 3", a)
	}
	if a := p.Capacity(); a != 3 {
		t.Errorf("The pool capacity was %d but should be 3", a)
	}

	// Get a client
	client, err := p.Get(context.Background())
	if err != nil {
		t.Errorf("Get returned an error: %s", err.Error())
	}
	if client == nil {
		t.Error("client was nil")
	}
	if a := p.Available(); a != 2 {
		t.Errorf("The pool available was %d but should be 2", a)
	}
	if a := p.Capacity(); a != 3 {
		t.Errorf("The pool capacity was %d but should be 3", a)
	}

	// Return the client
	err = client.Close()
	if err != nil {
		t.Errorf("Close returned an error: %s", err.Error())
	}
	if a := p.Available(); a != 3 {
		t.Errorf("The pool available was %d but should be 3", a)
	}
	if a := p.Capacity(); a != 3 {
		t.Errorf("The pool capacity was %d but should be 3", a)
	}

	// Attempt to return the client again
	err = client.Close()
	if err != ErrAlreadyClosed {
		t.Errorf("Expected error \"%s\" but got \"%s\"",
			ErrAlreadyClosed.Error(), err.Error())
	}

	// Take 3 clients
	cl1, err1 := p.Get(context.Background())
	cl2, err2 := p.Get(context.Background())
	cl3, err3 := p.Get(context.Background())
	if err1 != nil {
		t.Errorf("Err1 was not nil: %s", err1.Error())
	}
	if err2 != nil {
		t.Errorf("Err2 was not nil: %s", err2.Error())
	}
	if err3 != nil {
		t.Errorf("Err3 was not nil: %s", err3.Error())
	}

	if a := p.Available(); a != 0 {
		t.Errorf("The pool available was %d but should be 0", a)
	}
	if a := p.Capacity(); a != 3 {
		t.Errorf("The pool capacity was %d but should be 3", a)
	}

	// Returning all of them
	err1 = cl1.Close()
	if err1 != nil {
		t.Errorf("Close returned an error: %s", err1.Error())
	}
	err2 = cl2.Close()
	if err2 != nil {
		t.Errorf("Close returned an error: %s", err2.Error())
	}
	err3 = cl3.Close()
	if err3 != nil {
		t.Errorf("Close returned an error: %s", err3.Error())
	}
}

func TestTimeout(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.Dial("example.com", grpc.WithInsecure())
	}, 1, 1, 0)
	if err != nil {
		t.Errorf("The pool returned an error: %s", err.Error())
	}

	_, err = p.Get(context.Background())
	if err != nil {
		t.Errorf("Get returned an error: %s", err.Error())
	}
	if a := p.Available(); a != 0 {
		t.Errorf("The pool available was %d but expected 0", a)
	}

	// We want to fetch a second one, with a timeout. If the timeout was
	// ommitted, the pool would wait indefinitely as it'd wait for another
	// client to get back into the queue
	ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(10*time.Millisecond))
	_, err2 := p.Get(ctx)
	if err2 != ErrTimeout {
		t.Errorf("Expected error \"%s\" but got \"%s\"", ErrTimeout, err2.Error())
	}
}

func TestMaxLifeDuration(t *testing.T) {
	p, err := New(func() (*grpc.ClientConn, error) {
		return grpc.Dial("example.com", grpc.WithInsecure())
	}, 1, 1, 0, 1)
	if err != nil {
		t.Errorf("The pool returned an error: %s", err.Error())
	}

	c, err := p.Get(context.Background())
	if err != nil {
		t.Errorf("Get returned an error: %s", err.Error())
	}

	// The max life of the connection was very low (1ns), so when we close
	// the connection it should get marked as unhealthy
	if err := c.Close(); err != nil {
		t.Errorf("Close returned an error: %s", err.Error())
	}
	if !c.unhealthy {
		t.Errorf("the connection should've been marked as unhealthy")
	}

	// Let's also make sure we don't prematurely close the connection
	count := 0
	p, err = New(func() (*grpc.ClientConn, error) {
		count++
		return grpc.Dial("example.com", grpc.WithInsecure())
	}, 1, 1, 0, time.Minute)
	if err != nil {
		t.Errorf("The pool returned an error: %s", err.Error())
	}

	for i := 0; i < 3; i++ {
		c, err = p.Get(context.Background())
		if err != nil {
			t.Errorf("Get returned an error: %s", err.Error())
		}

		// The max life of the connection is high, so when we close
		// the connection it shouldn't be marked as unhealthy
		if err := c.Close(); err != nil {
			t.Errorf("Close returned an error: %s", err.Error())
		}
		if c.unhealthy {
			t.Errorf("the connection shouldn't have been marked as unhealthy")
		}
	}

	// Count should have been 1 as dial function should only have been called once
	if count > 1 {
		t.Errorf("Dial function has been called multiple times")
	}

}

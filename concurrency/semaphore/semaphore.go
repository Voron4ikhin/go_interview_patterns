package semaphore

import (
	"context"
	"errors"
)

type Semaphore chan struct{}

func NewSemaphore(n int) Semaphore {
	return make(Semaphore, n)
}

func (s Semaphore) Acquire(ctx context.Context) error {
	select {
	case s <- struct{}{}:
		return nil
	case <-ctx.Done():
		return errors.New("semaphore: acquire canceled")
	}
}

func (s Semaphore) Release() error {
	select {
	case <-s:
		return nil
	default:
		return errors.New("semaphore: release without acquire")
	}
}

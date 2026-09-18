package pipeline

import (
	"context"
	"math/rand"
	"time"
)

func Generate(ctx context.Context, n int) <-chan int {
	out := make(chan int)

	go func() {
		defer close(out)
		for i := 0; i < n; i++ {
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}

			select {
			case out <- rand.Intn(100) + 1:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

func Multiply(ctx context.Context, in <-chan int, x int) <-chan int {
	out := make(chan int)

	go func() {
		defer close(out)
		for {
			select {
			case v, ok := <-in:
				if !ok {
					return
				}
				select {
				case out <- x * v:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

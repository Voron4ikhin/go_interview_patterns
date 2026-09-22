package fanin_fanout

import (
	"context"
	"sync"
)

type FaninFanout[T any] struct {
}

func NewFaninFanout[T any]() *FaninFanout[T] {
	return &FaninFanout[T]{}
}

func (in *FaninFanout[T]) Merge(ctx context.Context, cs ...<-chan T) <-chan T {
	result := make(chan T)
	var wg sync.WaitGroup
	wg.Add(len(cs))

	for _, c := range cs {
		go func(c <-chan T) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case value, ok := <-c:
					if !ok {
						return
					}
					select {
					case <-ctx.Done():
						return
					case result <- value:
					}
				}
			}
		}(c)
	}

	go func() {
		wg.Wait()
		defer close(result)
	}()

	return result
}

func (in *FaninFanout[T]) Split(ctx context.Context, input <-chan T, n int) []<-chan T {
	result := make([]chan T, 0, n)
	for i := 0; i < n; i++ {
		result = append(result, make(chan T))
	}

	go func() {
		defer func() {
			for _, c := range result {
				close(c)
			}
		}()
		cid := 0
		for {
			select {
			case <-ctx.Done():
				return
			case value, ok := <-input:
				if !ok {
					return
				}
				for {
					if trySend(ctx, result[cid], value) {
						break
					}
					cid = (cid + 1) % len(result)
				}
			}
		}
	}()

	return toReadOnly(result)
}

func trySend[T any](ctx context.Context, c chan<- T, v T) bool {
	select {
	case <-ctx.Done():
		return true
	case c <- v:
		return true
	default:
		return false
	}
}

func toReadOnly[T any](input []chan T) []<-chan T {
	result := make([]<-chan T, 0, len(input))
	for _, c := range input {
		result = append(result, c)
	}
	return result
}

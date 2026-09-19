package workerpool

import (
	"context"
	"sync"
)

type Pool[T, R any] struct {
	workers int
	fn      func(ctx context.Context, n T) R
}

func NewPool[T, R any](workers int, fn func(ctx context.Context, n T) R) *Pool[T, R] {
	return &Pool[T, R]{
		workers: workers,
		fn:      fn,
	}
}

func (p *Pool[T, R]) Run(ctx context.Context, jobs []T) <-chan R {
	resultsCh := make(chan R)

	if p.workers <= 0 {
		close(resultsCh)
		return resultsCh
	}

	jobsCh := make(chan T)
	go func() {
		defer close(jobsCh)
		for _, job := range jobs {
			select {
			case <-ctx.Done():
				return
			case jobsCh <- job:
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(p.workers)
	for i := 0; i < p.workers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobsCh:
					if !ok {
						return
					}
					select {
					case <-ctx.Done():
						return
					case resultsCh <- p.fn(ctx, job):
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	return resultsCh
}

package workerpool

import (
	"context"
	"sync"
)

type Pool struct {
	workers int
	fn      func(ctx context.Context, n int) (int, error)
}

type Result struct {
	Job   int
	Value int
	Err   error
}

func NewPool(workers int, fn func(ctx context.Context, n int) (int, error)) *Pool {
	return &Pool{
		workers: workers,
		fn:      fn,
	}
}

func (p *Pool) Run(ctx context.Context, jobs []int) <-chan Result {
	resultsCh := make(chan Result)

	if p.workers <= 0 {
		close(resultsCh)
		return resultsCh
	}

	jobsCh := make(chan int)
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
			for job := range jobsCh {
				val, err := p.fn(ctx, job)
				select {
				case resultsCh <- Result{Job: job, Value: val, Err: err}:
				case <-ctx.Done():
					return
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

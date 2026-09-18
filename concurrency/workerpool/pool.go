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
	producer := make(chan int)
	consumer := make(chan Result)

	go func() {
		defer close(producer)
		for i := 0; i < len(jobs); i++ {
			select {
			case <-ctx.Done():
				return
			case producer <- jobs[i]:
			}
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(p.workers)
	for i := 0; i < p.workers; i++ {
		go func() {
			defer wg.Done()
			for job := range producer {
				val, err := p.fn(ctx, job)
				select {
				case consumer <- Result{Job: job, Value: val, Err: err}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		defer close(consumer)
	}()

	return consumer
}

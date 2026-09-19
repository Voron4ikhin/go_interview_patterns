package workerpool

import (
	"context"
	"fmt"
	"testing"
)

func makeJobs(n int) []int {
	jobs := make([]int, n)
	for i := range jobs {
		jobs[i] = i
	}
	return jobs
}

func drain(ch <-chan Result) {
	for range ch {
	}
}

func BenchmarkRun_Workers(b *testing.B) {
	jobs := makeJobs(1000)
	fn := func(_ context.Context, n int) Result {
		return Result{
			Value: n * n,
			Job:   n,
			Err:   nil,
		}
	}

	for _, workers := range []int{1, 2, 4, 8, 16, 32} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			p := NewPool[int, Result](workers, fn)
			ctx := context.Background()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				drain(p.Run(ctx, jobs))
			}
		})
	}
}

func BenchmarkRun_JobSize(b *testing.B) {
	fn := func(_ context.Context, n int) Result {
		return Result{
			Value: n * n,
			Job:   n,
			Err:   nil,
		}
	}
	p := NewPool[int, Result](8, fn)
	ctx := context.Background()

	for _, n := range []int{10, 100, 1000, 10000} {
		jobs := makeJobs(n)
		b.Run(fmt.Sprintf("jobs=%d", n), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				drain(p.Run(ctx, jobs))
			}
		})
	}
}

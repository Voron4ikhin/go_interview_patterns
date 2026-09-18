package workerpool

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// 1. Happy path: все задачи обработаны
func TestRun_ProcessesAllJobs(t *testing.T) {
	jobs := []int{1, 2, 3, 4, 5}
	got := make([]int, 0, len(jobs))
	workers := 3
	workingPool := NewPool(workers, func(ctx context.Context, n int) (int, error) {
		return n * n, nil
	})

	for r := range workingPool.Run(context.Background(), jobs) {
		if r.Err != nil {
			t.Fatalf("unexpected error: %v", r.Err)
		}
		got = append(got, r.Value)
	}

	if len(got) != len(jobs) {
		t.Fatalf("want %d results, got %d", len(jobs), len(got))
	}

	want := map[int]bool{1: true, 4: true, 9: true, 16: true, 25: true}
	for _, v := range got {
		if !want[v] {
			t.Fatalf("unexpected value: %d", v)
		}
		delete(want, v)
	}
	if len(want) != 0 {
		t.Fatalf("missing values: %v", want)
	}
}

// 2. Пустой список задач: канал результатов закрывается сразу
func TestRun_EmptyJobs(t *testing.T) {
	workers := 3
	workingPool := NewPool(workers, func(ctx context.Context, n int) (int, error) {
		return n * n, nil
	})
	ch := workingPool.Run(context.Background(), nil)

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected closed channel")
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed for empty jobs")
	}
}

// 3. workers=0: не должно быть паники и зависания
func TestRun_ZeroWorkers(t *testing.T) {
	workers := 0
	workingPool := NewPool(workers, func(ctx context.Context, n int) (int, error) {
		return n * n, nil
	})
	ch := workingPool.Run(context.Background(), []int{1, 2, 3})

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected no results")
		}
	case <-time.After(time.Second):
		t.Fatal("hung with zero workers")
	}
}

// 4. Ограничение конкурентности соблюдается
func TestRun_LimitsConcurrency(t *testing.T) {
	const (
		workers = 4
		total   = 100
	)

	jobs := make([]int, total)
	for i := range jobs {
		jobs[i] = i
	}

	var inFlight atomic.Int32
	var maxSeen atomic.Int32

	workingPool := NewPool(workers, func(ctx context.Context, n int) (int, error) {
		cur := inFlight.Add(1)
		for {
			prev := maxSeen.Load()
			if cur <= prev || maxSeen.CompareAndSwap(prev, cur) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		inFlight.Add(-1)
		return n, nil
	})
	for range workingPool.Run(context.Background(), jobs) {
	}

	if got := maxSeen.Load(); got > workers {
		t.Fatalf("concurrency exceeded: want <= %d, got %d", workers, got)
	}
}

// 5. Отмена контекста: воркеры завершаются
func TestRun_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	jobs := make([]int, 1000)
	for i := range jobs {
		jobs[i] = i
	}

	done := make(chan struct{})
	workers := 4
	workingPool := NewPool(workers, func(ctx context.Context, n int) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(10 * time.Millisecond):
			return n, nil
		}
	})
	go func() {
		for range workingPool.Run(ctx, jobs) {
		}
		close(done)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workers did not stop after cancel")
	}
}

// 6. Ошибки из fn прокидываются в Result
func TestRun_PropagatesErrors(t *testing.T) {
	boom := errors.New("boom")
	jobs := []int{1, 2, 3}

	var errs int
	workers := 2
	workingPool := NewPool(workers, func(_ context.Context, n int) (int, error) {
		if n == 2 {
			return 0, boom
		}
		return n, nil
	})
	for r := range workingPool.Run(context.Background(), jobs) {
		if r.Err != nil {
			errs++
			if !errors.Is(r.Err, boom) {
				t.Fatalf("want boom, got %v", r.Err)
			}
		}
	}
	if errs != 1 {
		t.Fatalf("want 1 error, got %d", errs)
	}
}

// 7. Нет утечки горутин
func TestRun_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	jobs := make([]int, 50)
	for i := range jobs {
		jobs[i] = i
	}
	workers := 8
	workingPool := NewPool(workers, func(_ context.Context, n int) (int, error) {
		return n, nil
	})

	for range workingPool.Run(context.Background(), jobs) {
	}

	time.Sleep(50 * time.Millisecond)

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Fatalf("goroutine leak: before=%d after=%d", before, after)
	}
}

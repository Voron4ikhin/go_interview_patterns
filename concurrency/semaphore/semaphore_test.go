package semaphore

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 1. Acquire успевает n раз подряд без блокировки, n+1-й блокируется
func TestSemaphore_AcquireUpToCapacity(t *testing.T) {
	const n = 3
	s := NewSemaphore(n)

	for i := 0; i < n; i++ {
		if err := s.Acquire(context.Background()); err != nil {
			t.Fatalf("acquire %d: unexpected error: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.Acquire(ctx); err == nil {
		t.Fatal("expected acquire to block once semaphore is full")
	}
}

// 2. Release освобождает слот, следующий Acquire проходит
func TestSemaphore_ReleaseFreesSlot(t *testing.T) {
	s := NewSemaphore(1)

	if err := s.Acquire(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.Release(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.Acquire(ctx); err != nil {
		t.Fatalf("expected acquire to succeed after release, got: %v", err)
	}
}

// 3. Release без предварительного Acquire возвращает ошибку
func TestSemaphore_ReleaseWithoutAcquire(t *testing.T) {
	s := NewSemaphore(1)
	if err := s.Release(); err == nil {
		t.Fatal("expected error releasing an unacquired semaphore")
	}
}

// 4. Отмена контекста прерывает блокирующий Acquire и не занимает слот
func TestSemaphore_AcquireCanceled(t *testing.T) {
	s := NewSemaphore(1)
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.Acquire(ctx)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected canceled acquire to return an error")
		}
	case <-time.After(time.Second):
		t.Fatal("acquire did not return after context cancel")
	}

	if err := s.Release(); err != nil {
		t.Fatalf("unexpected error releasing the original holder: %v", err)
	}
	if err := s.Release(); err == nil {
		t.Fatal("expected second release to fail: canceled acquire must not have taken a slot")
	}
}

// 5. Конкурентный доступ: одновременно держат слот не больше n горутин
func TestSemaphore_LimitsConcurrency(t *testing.T) {
	const (
		n       = 4
		workers = 50
	)
	s := NewSemaphore(n)

	var inFlight, maxSeen atomic.Int32
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			if err := s.Acquire(context.Background()); err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			defer s.Release()

			cur := inFlight.Add(1)
			for {
				prev := maxSeen.Load()
				if cur <= prev || maxSeen.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
		}()
	}
	wg.Wait()

	if got := maxSeen.Load(); got > n {
		t.Fatalf("concurrency exceeded: want <= %d, got %d", n, got)
	}
}

// 6. Отменённый Acquire не оставляет висящих горутин
func TestSemaphore_NoGoroutineLeakOnCancel(t *testing.T) {
	s := NewSemaphore(1)
	if err := s.Acquire(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	before := runtime.NumGoroutine()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_ = s.Acquire(ctx)

	time.Sleep(50 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutine leak: before=%d after=%d", before, after)
	}
}

// 7. NewSemaphore паникует на n <= 0
func TestNewSemaphore_InvalidCapacity(t *testing.T) {
	for _, n := range []int{0, -1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic for n=%d", n)
				}
			}()
			_ = NewSemaphore(n)
		}()
	}
}

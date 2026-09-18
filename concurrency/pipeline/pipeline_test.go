package pipeline

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// 1. Happy path: через конвейер проходит ровно n значений, каждое умножено на x
func TestPipeline_ProcessesAllValues(t *testing.T) {
	const (
		n = 5
		x = 2
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := 0
	for v := range Multiply(ctx, Generate(ctx, n), x) {
		got++
		if v < x || v > x*100 {
			t.Fatalf("value %d out of expected range [%d, %d]", v, x, x*100)
		}
		if v%x != 0 {
			t.Fatalf("value %d is not a multiple of x=%d", v, x)
		}
	}

	if got != n {
		t.Fatalf("want %d values, got %d", n, got)
	}
}

// 2. n=0: канал результата закрывается сразу, без значений
func TestPipeline_ZeroValues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	select {
	case _, ok := <-Multiply(ctx, Generate(ctx, 0), 3):
		if ok {
			t.Fatal("expected closed channel with no values")
		}
	case <-time.After(time.Second):
		t.Fatal("pipeline did not close for n=0")
	}
}

// 3. Ранний выход потребителя + отмена ctx не оставляют висящих горутин
func TestPipeline_NoGoroutineLeakOnCancel(t *testing.T) {
	before := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	out := Multiply(ctx, Generate(ctx, 50), 2)

	<-out
	<-out
	cancel()

	time.Sleep(200 * time.Millisecond)

	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutine leak after early consumer exit + cancel: before=%d after=%d", before, after)
	}
}

// 4. Отмена ctx до получения значений быстро закрывает канал результата
func TestPipeline_CancelBeforeConsuming(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := Multiply(ctx, Generate(ctx, 1000), 2)
	cancel()

	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("did not expect a value after immediate cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("pipeline did not stop promptly after cancel")
	}
}

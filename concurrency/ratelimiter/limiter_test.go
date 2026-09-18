package ratelimiter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 1. Базовый: за один период доступно не больше limit токенов
func TestTokenBucket_LimitWithinPeriod(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const limit = 5
	l := NewTokenBucketLimiter(ctx, limit, time.Second)
	allowed := 0
	for i := 0; i < limit*3; i++ {
		if l.Allow() {
			allowed++
		}
	}

	if allowed != limit {
		t.Fatalf("want %d allowed, got %d", limit, allowed)
	}
}

// 2. После периода токены восстанавливаются
func TestTokenBucket_Replenishes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const limit = 3
	period := 300 * time.Millisecond
	l := NewTokenBucketLimiter(ctx, limit, period)

	for i := 0; i < limit; i++ {
		if !l.Allow() {
			t.Fatalf("expected token %d to be available", i)
		}
	}
	if l.Allow() {
		t.Fatal("expected bucket to be empty")
	}

	time.Sleep(period/time.Duration(limit) + 50*time.Millisecond)

	if !l.Allow() {
		t.Fatal("expected token to be replenished")
	}
}

// 3. Полное восстановление за период
func TestTokenBucket_FullReplenishment(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const limit = 4
	period := 400 * time.Millisecond
	l := NewTokenBucketLimiter(ctx, limit, period)

	for i := 0; i < limit; i++ {
		l.Allow()
	}

	time.Sleep(period + 100*time.Millisecond)

	allowed := 0
	for i := 0; i < limit; i++ {
		if l.Allow() {
			allowed++
		}
	}
	if allowed != limit {
		t.Fatalf("want %d tokens after full period, got %d", limit, allowed)
	}
}

// 4. Overflow: пополнение не превышает limit
func TestTokenBucket_NoOverflow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const limit = 2
	period := 100 * time.Millisecond
	l := NewTokenBucketLimiter(ctx, limit, period)

	time.Sleep(period * 5)

	allowed := 0
	for i := 0; i < limit*10; i++ {
		if l.Allow() {
			allowed++
		}
	}
	if allowed != limit {
		t.Fatalf("bucket overflowed: want %d, got %d", limit, allowed)
	}
}

// 5. Конкурентный доступ: суммарно не больше limit в окне
func TestTokenBucket_Concurrent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		limit   = 100
		workers = 200
	)
	l := NewTokenBucketLimiter(ctx, limit, time.Hour)

	var allowed int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			if l.Allow() {
				atomic.AddInt64(&allowed, 1)
			}
		}()
	}
	wg.Wait()

	if allowed != limit {
		t.Fatalf("want exactly %d allowed, got %d", limit, allowed)
	}
}

// 6. Отмена контекста останавливает пополнение
func TestTokenBucket_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	const limit = 1
	period := 50 * time.Millisecond
	l := NewTokenBucketLimiter(ctx, limit, period)

	if !l.Allow() {
		t.Fatal("expected initial token")
	}
	cancel()

	// после отмены токены не должны появляться
	time.Sleep(period * 5)
	if l.Allow() {
		t.Fatal("expected no replenishment after context cancel")
	}
}

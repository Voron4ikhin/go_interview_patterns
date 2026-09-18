package ratelimiter

import (
	"context"
	"fmt"
	"time"
)

type TokenBucketLimiter struct {
	tokenBucketCh chan struct{}
}

func NewTokenBucketLimiter(ctx context.Context, limit int, period time.Duration) *TokenBucketLimiter {
	if limit <= 0 {
		panic(fmt.Sprintf("ratelimiter: limit must be positive, got %d", limit))
	}
	if period <= 0 {
		panic(fmt.Sprintf("ratelimiter: period must be positive, got %s", period))
	}

	limiter := &TokenBucketLimiter{
		tokenBucketCh: make(chan struct{}, limit),
	}

	for i := 0; i < limit; i++ {
		limiter.tokenBucketCh <- struct{}{}
	}

	replenishmentInterval := period / time.Duration(limit)
	go limiter.startPeriodicReplenishment(ctx, replenishmentInterval)
	return limiter
}

func (l *TokenBucketLimiter) startPeriodicReplenishment(ctx context.Context, interval time.Duration) {
	timer := time.NewTicker(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			select {
			case l.tokenBucketCh <- struct{}{}:
			default:
			}
		}
	}
}

func (l *TokenBucketLimiter) Allow() bool {
	select {
	case <-l.tokenBucketCh:
		return true
	default:
		return false
	}
}

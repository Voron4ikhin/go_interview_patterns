package ratelimiter

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func BenchmarkAllow_Contended(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	l := NewTokenBucketLimiter(ctx, 1000, time.Millisecond)

	for _, goroutines := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("goroutines=%d", goroutines), func(b *testing.B) {
			b.SetParallelism(goroutines)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					l.Allow()
				}
			})
		})
	}
}

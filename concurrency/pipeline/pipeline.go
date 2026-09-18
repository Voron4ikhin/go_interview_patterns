package pipeline

import (
	"math/rand"
	"time"
)

func simpleGenerator(n int) <-chan int {
	out := make(chan int)

	go func() {
		defer close(out)
		for i := 0; i < n; i++ {
			out <- rand.Intn(100) + 1
			time.Sleep(time.Millisecond * 100)
		}
	}()

	return out
}

func multiplier(val <-chan int, x int) <-chan int {
	out := make(chan int)

	go func() {
		defer close(out)
		for v := range val {
			out <- x * v
		}
	}()

	return out
}

// Запуск multiplier(simpleGenerator(4), 2)

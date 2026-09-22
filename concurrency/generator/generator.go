package generator

import (
	"context"
	"fmt"
)

type Generator struct {
}

func NewGenerator() *Generator {
	return &Generator{}
}

func (g *Generator) Producer(ctx context.Context, jobs []any) <-chan any {
	resultCh := make(chan any)
	defer close(resultCh)

	for _, v := range jobs {
		select {
		case <-ctx.Done():
			return resultCh
		case resultCh <- v:
		}
	}

	return resultCh
}

func (g *Generator) Consumer(ctx context.Context, jobCh <-chan any) {
	for {
		select {
		case <-ctx.Done():
			return
		case result := <-jobCh:
			fmt.Println(result)
		}
	}
}

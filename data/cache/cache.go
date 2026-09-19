package cache

import "errors"

var ErrNotFound = errors.New("key not found")

type Cache interface {
	Get(key int) (int, error)
	Put(key, value int)
	Peek(key int) (int, error)
	Contains(key int) bool
	Delete(key int) error
	Len() int
	Keys() []int
	Clear()
}

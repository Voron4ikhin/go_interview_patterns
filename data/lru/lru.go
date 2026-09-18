package lru

import (
	"errors"
	"sync"
)

var (
	ErrorItemNotFound = errors.New("item not found")
)

type LRUCache interface {
	Get(key int) (int, error)
	Delete(key int) error
	Insert(key, value int) error
	Len() int
}

type Node struct {
	prev, next *Node
	key, value int
}

type Cache struct {
	start, end *Node
	storage    map[int]*Node
	mu         sync.RWMutex
	cap        int
}

func NewLRUCache(capacity int) *Cache {
	return &Cache{
		storage: make(map[int]*Node, capacity),
		cap:     capacity,
	}
}

func (c *Cache) Get(key int) (int, error) {
	c.mu.RLock()
	ptr, ok := c.storage[key]
	c.mu.RUnlock()
	if !ok {
		return 0, ErrorItemNotFound
	}
	if err := c.Delete(key); err != nil {
		return 0, err
	}
	if err := c.Insert(key, ptr.value); err != nil {
		return 0, err
	}

	return ptr.value, nil
}

func (c *Cache) Insert(key, value int) error {
	newNode := &Node{
		key:   key,
		value: value,
	}
	c.mu.Lock()
	if c.start == nil {
		c.start = newNode
	} else {
		newNode.next = c.start
		c.start.prev = newNode
		c.start = newNode
	}
	if c.end == nil {
		c.end = newNode
	} else {
		if c.checkToDelete() {
			if err := c.Delete(c.end.key); err != nil {
				return err
			}
		}
	}
	c.storage[key] = newNode
	c.mu.Unlock()
	return nil
}

func (c *Cache) checkToDelete() bool {
	if c.Len() != c.cap {
		return false
	}
	return true
}

func (c *Cache) Delete(key int) error {
	c.mu.RLock()
	ptr, ok := c.storage[key]
	c.mu.RUnlock()
	if !ok {
		return ErrorItemNotFound
	}
	afterPtr := ptr.next
	prePtr := ptr.prev
	if afterPtr == nil && prePtr == nil {
		c.start = ptr
		c.end = ptr
	} else if afterPtr == nil {
		prePtr.next = nil
		c.end = prePtr
	} else if prePtr == nil {
		afterPtr.prev = nil
		c.start = afterPtr
	} else {
		afterPtr.next = prePtr
		prePtr.prev = afterPtr
	}
	delete(c.storage, key)

	return nil

}

func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.storage)
}

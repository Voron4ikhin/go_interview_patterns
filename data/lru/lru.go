package lru

import (
	"fmt"
	"sync"

	"github.com/Voron4ikhin/go_interview_patterns/data/cache"
)

type node struct {
	key, value int
	prev, next *node
}

type LRUCache struct {
	mu         sync.Mutex
	cap        int
	data       map[int]*node
	head, tail *node
}

var _ cache.Cache = (*LRUCache)(nil)

func NewLRUCache(capacity int) *LRUCache {
	if capacity <= 0 {
		panic(fmt.Sprintf("lru: capacity must be positive, got %d", capacity))
	}

	head := &node{}
	tail := &node{}
	head.next = tail
	tail.prev = head

	return &LRUCache{
		cap:  capacity,
		data: make(map[int]*node, capacity),
		head: head,
		tail: tail,
	}
}

func (c *LRUCache) Get(key int) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	n, ok := c.data[key]
	if !ok {
		return 0, cache.ErrNotFound
	}
	c.moveToFront(n)
	return n.value, nil
}

func (c *LRUCache) Peek(key int) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	n, ok := c.data[key]
	if !ok {
		return 0, cache.ErrNotFound
	}
	return n.value, nil
}

func (c *LRUCache) Contains(key int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.data[key]
	return ok
}

func (c *LRUCache) Put(key, value int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if n, ok := c.data[key]; ok {
		n.value = value
		c.moveToFront(n)
		return
	}

	n := &node{key: key, value: value}
	c.data[key] = n
	c.pushFront(n)

	if len(c.data) > c.cap {
		lru := c.tail.prev
		c.unlink(lru)
		delete(c.data, lru.key)
	}
}

func (c *LRUCache) Delete(key int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	n, ok := c.data[key]
	if !ok {
		return cache.ErrNotFound
	}
	c.unlink(n)
	delete(c.data, key)
	return nil
}

func (c *LRUCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.data)
}

func (c *LRUCache) Cap() int {
	return c.cap
}

func (c *LRUCache) Keys() []int {
	c.mu.Lock()
	defer c.mu.Unlock()

	keys := make([]int, 0, len(c.data))
	for n := c.head.next; n != c.tail; n = n.next {
		keys = append(keys, n.key)
	}
	return keys
}

func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data = make(map[int]*node, c.cap)
	c.head.next = c.tail
	c.tail.prev = c.head
}

func (c *LRUCache) pushFront(n *node) {
	n.prev = c.head
	n.next = c.head.next
	c.head.next.prev = n
	c.head.next = n
}

func (c *LRUCache) unlink(n *node) {
	n.prev.next = n.next
	n.next.prev = n.prev
	n.prev = nil
	n.next = nil
}

func (c *LRUCache) moveToFront(n *node) {
	c.unlink(n)
	c.pushFront(n)
}

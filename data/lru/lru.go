package lru

import (
	"errors"
	"fmt"
	"sync"
)

var ErrNotFound = errors.New("lru: key not found")

type LRUCache interface {
	Get(key int) (int, error)
	Put(key, value int)
	Peek(key int) (int, error)
	Contains(key int) bool
	Delete(key int) error
	Len() int
	Cap() int
	Keys() []int
	Clear()
}

type node struct {
	key, value int
	prev, next *node
}

type Cache struct {
	mu         sync.Mutex
	cap        int
	data       map[int]*node
	head, tail *node
}

var _ LRUCache = (*Cache)(nil)

func NewLRUCache(capacity int) *Cache {
	if capacity <= 0 {
		panic(fmt.Sprintf("lru: capacity must be positive, got %d", capacity))
	}

	head := &node{}
	tail := &node{}
	head.next = tail
	tail.prev = head

	return &Cache{
		cap:  capacity,
		data: make(map[int]*node, capacity),
		head: head,
		tail: tail,
	}
}

func (c *Cache) Get(key int) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	n, ok := c.data[key]
	if !ok {
		return 0, ErrNotFound
	}
	c.moveToFront(n)
	return n.value, nil
}

func (c *Cache) Peek(key int) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	n, ok := c.data[key]
	if !ok {
		return 0, ErrNotFound
	}
	return n.value, nil
}

func (c *Cache) Contains(key int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.data[key]
	return ok
}

func (c *Cache) Put(key, value int) {
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

func (c *Cache) Delete(key int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	n, ok := c.data[key]
	if !ok {
		return ErrNotFound
	}
	c.unlink(n)
	delete(c.data, key)
	return nil
}

func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.data)
}

func (c *Cache) Cap() int {
	return c.cap
}

func (c *Cache) Keys() []int {
	c.mu.Lock()
	defer c.mu.Unlock()

	keys := make([]int, 0, len(c.data))
	for n := c.head.next; n != c.tail; n = n.next {
		keys = append(keys, n.key)
	}
	return keys
}

func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data = make(map[int]*node, c.cap)
	c.head.next = c.tail
	c.tail.prev = c.head
}

func (c *Cache) pushFront(n *node) {
	n.prev = c.head
	n.next = c.head.next
	c.head.next.prev = n
	c.head.next = n
}

func (c *Cache) unlink(n *node) {
	n.prev.next = n.next
	n.next.prev = n.prev
	n.prev = nil
	n.next = nil
}

func (c *Cache) moveToFront(n *node) {
	c.unlink(n)
	c.pushFront(n)
}

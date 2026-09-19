package ttl_cache

import (
	"sync"
	"time"

	"github.com/Voron4ikhin/go_interview_patterns/data/cache"
)

type Item struct {
	Value      int
	Expiration time.Time
}

type TTLCache struct {
	mu     sync.RWMutex
	data   map[int]Item
	ttl    time.Duration
	stopCh chan struct{}
}

var _ cache.Cache = (*TTLCache)(nil)

func NewTTLCache(cleanupInterval time.Duration) *TTLCache {
	ttlCache := &TTLCache{
		ttl:    cleanupInterval,
		data:   make(map[int]Item),
		stopCh: make(chan struct{}),
	}

	go ttlCache.startCleanup()

	return ttlCache
}

func (c *TTLCache) startCleanup() {
	ticker := time.NewTicker(c.ttl)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now()
			for k, v := range c.data {
				if now.After(v.Expiration) {
					delete(c.data, k)
				}
			}
			c.mu.Unlock()
		case <-c.stopCh:
			return
		}
	}
}

func (c *TTLCache) Close() {
	close(c.stopCh)
}

func (c *TTLCache) Get(key int) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	val, ok := c.data[key]
	if !ok || time.Now().After(val.Expiration) {
		return 0, cache.ErrNotFound
	}

	val.Expiration = time.Now().Add(c.ttl)
	c.data[key] = val

	return val.Value, nil
}

func (c *TTLCache) Put(key, value int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data[key] = Item{
		Value:      value,
		Expiration: time.Now().Add(c.ttl),
	}
}

func (c *TTLCache) Peek(key int) (int, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	val, ok := c.data[key]
	if !ok || time.Now().After(val.Expiration) {
		return 0, cache.ErrNotFound
	}

	return val.Value, nil

}
func (c *TTLCache) Contains(key int) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	val, ok := c.data[key]
	if !ok || time.Now().After(val.Expiration) {
		return false
	}

	return true
}

func (c *TTLCache) Delete(key int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.data[key]; !ok {
		return cache.ErrNotFound
	}

	delete(c.data, key)
	return nil
}

func (c *TTLCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.data)
}

func (c *TTLCache) Keys() []int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	length := len(c.data)
	keys := make([]int, 0, length)

	for k, _ := range c.data {
		keys = append(keys, k)
	}

	return keys
}

func (c *TTLCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = make(map[int]Item)
}

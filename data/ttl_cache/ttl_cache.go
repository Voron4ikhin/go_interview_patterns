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

func (it Item) expired(now time.Time) bool {
	return now.After(it.Expiration)
}

type TTLCache struct {
	mu       sync.RWMutex
	data     map[int]Item
	ttl      time.Duration
	stopCh   chan struct{}
	stopOnce sync.Once
}

var _ cache.Cache = (*TTLCache)(nil)

func NewTTLCache(ttl, cleanupInterval time.Duration) *TTLCache {
	ttlCache := &TTLCache{
		ttl:    ttl,
		data:   make(map[int]Item),
		stopCh: make(chan struct{}),
	}

	go ttlCache.startCleanup(cleanupInterval)

	return ttlCache
}

func (c *TTLCache) startCleanup(cleanupInterval time.Duration) {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now()
			for k, v := range c.data {
				if v.expired(now) {
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
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
}

func (c *TTLCache) Get(key int) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	val, ok := c.data[key]
	if !ok || val.expired(time.Now()) {
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
	if !ok || val.expired(time.Now()) {
		return 0, cache.ErrNotFound
	}

	return val.Value, nil
}

func (c *TTLCache) Contains(key int) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	val, ok := c.data[key]
	if !ok || val.expired(time.Now()) {
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

	now := time.Now()
	count := 0
	for _, v := range c.data {
		if !v.expired(now) {
			count++
		}
	}
	return count
}

func (c *TTLCache) Keys() []int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	now := time.Now()
	keys := make([]int, 0, len(c.data))
	for k, v := range c.data {
		if !v.expired(now) {
			keys = append(keys, k)
		}
	}

	return keys
}

func (c *TTLCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = make(map[int]Item)
}

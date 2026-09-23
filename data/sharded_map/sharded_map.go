package sharded_map

import (
	"fmt"
	"hash/fnv"
	"sync"
)

type Cache[K comparable, V any] interface {
	Get(key K) (V, bool)
	Set(key K, value V)
	Delete(key K) bool
	Contains(key K) bool
	Len() int
	Keys() []K
	Clear()
	Range(fn func(key K, value V) bool)
}

type HashFunc[K any] func(key K) uint32

type shard[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]V
}

type ShardedMap[K comparable, V any] struct {
	shards []*shard[K, V]
	hash   HashFunc[K]
}

var _ Cache[string, int] = (*ShardedMap[string, int])(nil)

func NewShardedMap[K comparable, V any](shardCount int, hash HashFunc[K]) *ShardedMap[K, V] {
	if shardCount <= 0 {
		panic(fmt.Sprintf("sharded_map: shardCount must be positive, got %d", shardCount))
	}
	if hash == nil {
		panic("sharded_map: hash must not be nil")
	}

	shards := make([]*shard[K, V], shardCount)
	for i := range shards {
		shards[i] = &shard[K, V]{data: make(map[K]V)}
	}

	return &ShardedMap[K, V]{shards: shards, hash: hash}
}

func NewStringShardedMap[V any](shardCount int) *ShardedMap[string, V] {
	return NewShardedMap[string, V](shardCount, FNV32String)
}

func FNV32String(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

func (m *ShardedMap[K, V]) shardFor(key K) *shard[K, V] {
	idx := m.hash(key) % uint32(len(m.shards))
	return m.shards[idx]
}

func (m *ShardedMap[K, V]) Get(key K) (V, bool) {
	s := m.shardFor(key)
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	return value, ok
}

func (m *ShardedMap[K, V]) Set(key K, value V) {
	s := m.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
}

func (m *ShardedMap[K, V]) Delete(key K) bool {
	s := m.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; !ok {
		return false
	}
	delete(s.data, key)
	return true
}

func (m *ShardedMap[K, V]) Contains(key K) bool {
	s := m.shardFor(key)
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.data[key]
	return ok
}

func (m *ShardedMap[K, V]) Len() int {
	total := 0
	for _, s := range m.shards {
		s.mu.RLock()
		total += len(s.data)
		s.mu.RUnlock()
	}
	return total
}

func (m *ShardedMap[K, V]) Keys() []K {
	keys := make([]K, 0, m.Len())
	for _, s := range m.shards {
		s.mu.RLock()
		for k := range s.data {
			keys = append(keys, k)
		}
		s.mu.RUnlock()
	}
	return keys
}

func (m *ShardedMap[K, V]) Clear() {
	for _, s := range m.shards {
		s.mu.Lock()
		s.data = make(map[K]V)
		s.mu.Unlock()
	}
}

func (m *ShardedMap[K, V]) Range(fn func(key K, value V) bool) {
	for _, s := range m.shards {
		if !s.rangeShard(fn) {
			return
		}
	}
}

func (s *shard[K, V]) rangeShard(fn func(key K, value V) bool) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for k, v := range s.data {
		if !fn(k, v) {
			return false
		}
	}
	return true
}

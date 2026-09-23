package sharded_map

import (
	"fmt"
	"sort"
	"sync"
	"testing"
)

// 1. Set сохраняет значение, Get его возвращает
func TestShardedMap_SetAndGet(t *testing.T) {
	m := NewStringShardedMap[int](4)
	m.Set("a", 1)

	got, ok := m.Get("a")
	if !ok {
		t.Fatal("expected key to be found")
	}
	if got != 1 {
		t.Fatalf("want 1, got %d", got)
	}
}

// 2. Get отсутствующего ключа возвращает ok=false
func TestShardedMap_GetMissing(t *testing.T) {
	m := NewStringShardedMap[int](4)
	if _, ok := m.Get("missing"); ok {
		t.Fatal("expected key not to be found")
	}
}

// 3. Set существующего ключа обновляет значение
func TestShardedMap_SetOverwrites(t *testing.T) {
	m := NewStringShardedMap[int](4)
	m.Set("a", 1)
	m.Set("a", 2)

	got, ok := m.Get("a")
	if !ok || got != 2 {
		t.Fatalf("want 2, got %d ok=%v", got, ok)
	}
}

// 4. Delete удаляет ключ и сообщает, существовал ли он
func TestShardedMap_Delete(t *testing.T) {
	m := NewStringShardedMap[int](4)
	m.Set("a", 1)

	if !m.Delete("a") {
		t.Fatal("expected Delete to report the key existed")
	}
	if _, ok := m.Get("a"); ok {
		t.Fatal("key should be gone after Delete")
	}
	if m.Delete("a") {
		t.Fatal("deleting a missing key should report false")
	}
}

// 5. Contains сообщает о наличии ключа
func TestShardedMap_Contains(t *testing.T) {
	m := NewStringShardedMap[int](4)
	if m.Contains("a") {
		t.Fatal("map should not contain key yet")
	}
	m.Set("a", 1)
	if !m.Contains("a") {
		t.Fatal("map should contain key")
	}
}

// 6. Len отражает количество элементов по мере вставок и удалений
func TestShardedMap_Len(t *testing.T) {
	m := NewStringShardedMap[int](4)
	if m.Len() != 0 {
		t.Fatalf("want len 0, got %d", m.Len())
	}
	m.Set("a", 1)
	m.Set("b", 2)
	if m.Len() != 2 {
		t.Fatalf("want len 2, got %d", m.Len())
	}
	m.Set("a", 100)
	if m.Len() != 2 {
		t.Fatalf("want len 2 after overwrite, got %d", m.Len())
	}
	m.Delete("a")
	if m.Len() != 1 {
		t.Fatalf("want len 1 after delete, got %d", m.Len())
	}
}

// 7. Keys возвращает все ключи независимо от того, в каком шарде они лежат
func TestShardedMap_Keys(t *testing.T) {
	m := NewStringShardedMap[int](4)
	want := []string{"a", "b", "c", "d", "e"}
	for i, k := range want {
		m.Set(k, i)
	}

	got := m.Keys()
	sort.Strings(got)
	sort.Strings(want)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

// 8. Range проходит по всем элементам и может остановиться досрочно
func TestShardedMap_Range(t *testing.T) {
	m := NewStringShardedMap[int](4)
	for i, k := range []string{"a", "b", "c", "d"} {
		m.Set(k, i)
	}

	visited := make(map[string]int)
	m.Range(func(k string, v int) bool {
		visited[k] = v
		return true
	})
	if len(visited) != 4 {
		t.Fatalf("want 4 visited entries, got %d", len(visited))
	}

	count := 0
	m.Range(func(k string, v int) bool {
		count++
		return false
	})
	if count != 1 {
		t.Fatalf("want Range to stop after first entry, visited %d", count)
	}
}

// 9. Clear опустошает все шарды
func TestShardedMap_Clear(t *testing.T) {
	m := NewStringShardedMap[int](4)
	m.Set("a", 1)
	m.Set("b", 2)

	m.Clear()

	if m.Len() != 0 {
		t.Fatalf("want len 0 after Clear, got %d", m.Len())
	}
	if _, ok := m.Get("a"); ok {
		t.Fatal("want key gone after Clear")
	}

	m.Set("c", 3)
	got, ok := m.Get("c")
	if !ok || got != 3 {
		t.Fatalf("map should work after Clear, got v=%d ok=%v", got, ok)
	}
}

// 10. Один и тот же ключ всегда попадает в один и тот же шард
func TestShardedMap_KeyStableAcrossShard(t *testing.T) {
	m := NewStringShardedMap[int](8)
	first := m.shardFor("stable-key")
	for i := 0; i < 100; i++ {
		if m.shardFor("stable-key") != first {
			t.Fatal("same key resolved to different shards")
		}
	}
}

// 11. NewShardedMap паникует на некорректных аргументах
func TestNewShardedMap_InvalidArgs(t *testing.T) {
	mustPanic := func(name string, fn func()) {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			fn()
		})
	}

	mustPanic("zero shards", func() { NewShardedMap[string, int](0, FNV32String) })
	mustPanic("negative shards", func() { NewShardedMap[string, int](-1, FNV32String) })
	mustPanic("nil hash", func() { NewShardedMap[string, int](4, nil) })
}

// 12. Работает с произвольными типами ключей и значений, не только string/int
func TestShardedMap_GenericTypes(t *testing.T) {
	type point struct{ x, y int }

	hash := func(k int) uint32 { return uint32(k) }
	m := NewShardedMap[int, point](4, hash)

	m.Set(1, point{1, 1})
	got, ok := m.Get(1)
	if !ok || got != (point{1, 1}) {
		t.Fatalf("want {1 1}, got %v ok=%v", got, ok)
	}
}

// 13. Конкурентный доступ не гонится по данным (запускать с -race)
func TestShardedMap_ConcurrentAccess(t *testing.T) {
	const (
		shardCount = 16
		workers    = 32
		opsEach    = 200
	)
	m := NewStringShardedMap[int](shardCount)

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < opsEach; i++ {
				key := fmt.Sprintf("key-%d", (w*opsEach+i)%(shardCount*4))
				m.Set(key, i)
				m.Get(key)
				m.Contains(key)
				m.Len()
				if i%7 == 0 {
					m.Delete(key)
				}
			}
		}(w)
	}
	wg.Wait()
}

// 14. Хеш-функция распределяет ключи по всем шардам, а не в один
func TestFNV32String_Distribution(t *testing.T) {
	const shardCount = 8
	buckets := make(map[uint32]int)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("user-%d", i)
		buckets[FNV32String(key)%shardCount] = buckets[FNV32String(key)%shardCount] + 1
	}
	if len(buckets) < shardCount {
		t.Fatalf("expected keys to spread across all %d shards, only hit %d", shardCount, len(buckets))
	}
}

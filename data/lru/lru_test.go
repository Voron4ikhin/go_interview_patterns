package lru

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

// 1. Put сохраняет значение, Get его возвращает
func TestCache_PutAndGet(t *testing.T) {
	c := NewLRUCache(2)
	c.Put(1, 100)

	got, err := c.Get(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 100 {
		t.Fatalf("want 100, got %d", got)
	}
}

// 2. Get отсутствующего ключа возвращает ErrNotFound
func TestCache_GetMissing(t *testing.T) {
	c := NewLRUCache(2)
	if _, err := c.Get(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// 3. При превышении ёмкости вытесняется наименее недавно использованный элемент
func TestCache_EvictsLeastRecentlyUsed(t *testing.T) {
	c := NewLRUCache(2)
	c.Put(1, 10)
	c.Put(2, 20)
	c.Put(3, 30)

	if _, err := c.Get(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("key 1 should have been evicted, got err=%v", err)
	}
	if v, err := c.Get(2); err != nil || v != 20 {
		t.Fatalf("key 2 should survive, got v=%d err=%v", v, err)
	}
	if v, err := c.Get(3); err != nil || v != 30 {
		t.Fatalf("key 3 should survive, got v=%d err=%v", v, err)
	}
}

// 4. Get обновляет порядок использования, спасая ключ от вытеснения
func TestCache_GetRefreshesRecency(t *testing.T) {
	c := NewLRUCache(2)
	c.Put(1, 10)
	c.Put(2, 20)
	c.Get(1)
	c.Put(3, 30)

	if _, err := c.Get(2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("key 2 should have been evicted, got err=%v", err)
	}
	if v, err := c.Get(1); err != nil || v != 10 {
		t.Fatalf("key 1 should survive, got v=%d err=%v", v, err)
	}
}

// 5. Put существующего ключа обновляет значение и порядок использования, без вытеснения
func TestCache_PutExistingKeyUpdatesValueAndRecency(t *testing.T) {
	c := NewLRUCache(2)
	c.Put(1, 10)
	c.Put(2, 20)
	c.Put(1, 999)
	c.Put(3, 30)

	if c.Len() != 2 {
		t.Fatalf("want len 2, got %d", c.Len())
	}
	if v, err := c.Get(1); err != nil || v != 999 {
		t.Fatalf("key 1 should be updated, got v=%d err=%v", v, err)
	}
	if _, err := c.Get(2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("key 2 should have been evicted, got err=%v", err)
	}
}

// 6. Delete удаляет ключ; повторное удаление или удаление отсутствующего ключа возвращает ошибку
func TestCache_Delete(t *testing.T) {
	c := NewLRUCache(2)
	c.Put(1, 10)

	if err := c.Delete(1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := c.Get(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("key 1 should be gone, got err=%v", err)
	}
	if err := c.Delete(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleting missing key should return ErrNotFound, got %v", err)
	}
}

// 7. Peek возвращает значение, не меняя порядок использования
func TestCache_Peek(t *testing.T) {
	c := NewLRUCache(2)
	c.Put(1, 10)
	c.Put(2, 20)

	if v, err := c.Peek(1); err != nil || v != 10 {
		t.Fatalf("want v=10, got v=%d err=%v", v, err)
	}

	c.Put(3, 30)

	if _, err := c.Get(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("key 1 should have been evicted despite Peek, got err=%v", err)
	}
}

// 8. Contains сообщает о наличии ключа, не меняя порядок использования
func TestCache_Contains(t *testing.T) {
	c := NewLRUCache(1)
	if c.Contains(1) {
		t.Fatal("cache should not contain key 1 yet")
	}
	c.Put(1, 10)
	if !c.Contains(1) {
		t.Fatal("cache should contain key 1")
	}
}

// 9. Len отражает количество элементов по мере вставок, обновлений, удалений и вытеснений
func TestCache_Len(t *testing.T) {
	c := NewLRUCache(2)
	if c.Len() != 0 {
		t.Fatalf("want len 0, got %d", c.Len())
	}
	c.Put(1, 10)
	c.Put(2, 20)
	if c.Len() != 2 {
		t.Fatalf("want len 2, got %d", c.Len())
	}
	c.Put(2, 200)
	if c.Len() != 2 {
		t.Fatalf("want len 2 after update, got %d", c.Len())
	}
	c.Put(3, 30)
	if c.Len() != 2 {
		t.Fatalf("want len 2 after eviction, got %d", c.Len())
	}
	c.Delete(3)
	if c.Len() != 1 {
		t.Fatalf("want len 1 after delete, got %d", c.Len())
	}
}

// 10. Keys возвращает ключи от самого недавно использованного к самому старому
func TestCache_Keys(t *testing.T) {
	c := NewLRUCache(3)
	c.Put(1, 10)
	c.Put(2, 20)
	c.Put(3, 30)
	c.Get(1)

	want := []int{1, 3, 2}
	if got := c.Keys(); !reflect.DeepEqual(got, want) {
		t.Fatalf("want %v, got %v", want, got)
	}
}

// 11. Clear опустошает кэш
func TestCache_Clear(t *testing.T) {
	c := NewLRUCache(2)
	c.Put(1, 10)
	c.Put(2, 20)

	c.Clear()

	if c.Len() != 0 {
		t.Fatalf("want len 0 after Clear, got %d", c.Len())
	}
	if got := c.Keys(); len(got) != 0 {
		t.Fatalf("want no keys after Clear, got %v", got)
	}
	if _, err := c.Get(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound after Clear, got %v", err)
	}

	c.Put(3, 30)
	if v, err := c.Get(3); err != nil || v != 30 {
		t.Fatalf("cache should work after Clear, got v=%d err=%v", v, err)
	}
}

// 12. Ёмкость 1: каждый новый Put вытесняет единственный существующий элемент
func TestCache_CapacityOne(t *testing.T) {
	c := NewLRUCache(1)
	c.Put(1, 10)
	c.Put(2, 20)

	if _, err := c.Get(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("key 1 should have been evicted, got err=%v", err)
	}
	if v, err := c.Get(2); err != nil || v != 20 {
		t.Fatalf("key 2 should survive, got v=%d err=%v", v, err)
	}
	if c.Len() != 1 {
		t.Fatalf("want len 1, got %d", c.Len())
	}
}

// 13. NewLRUCache паникует на неположительной ёмкости
func TestNewLRUCache_InvalidCapacity(t *testing.T) {
	for _, capacity := range []int{0, -1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic for capacity=%d", capacity)
				}
			}()
			_ = NewLRUCache(capacity)
		}()
	}
}

// 14. Конкурентный доступ не гонится по данным и не превышает ёмкость (запускать с -race)
func TestCache_ConcurrentAccess(t *testing.T) {
	const (
		capacity = 16
		workers  = 32
		opsEach  = 200
	)
	c := NewLRUCache(capacity)

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < opsEach; i++ {
				key := (w*opsEach + i) % (capacity * 2)
				c.Put(key, key*10)
				c.Get(key)
				c.Peek(key)
				c.Contains(key)
				if key%7 == 0 {
					c.Delete(key)
				}
			}
		}(w)
	}
	wg.Wait()

	if c.Len() > capacity {
		t.Fatalf("cache exceeded capacity: len=%d cap=%d", c.Len(), capacity)
	}
}

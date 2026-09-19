package ttl_cache

import (
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Voron4ikhin/go_interview_patterns/data/cache"
)

const (
	testTTL     = 40 * time.Millisecond
	testCleanup = 20 * time.Millisecond
	testMargin  = 30 * time.Millisecond
)

// 1. Put сохраняет значение, Get его возвращает
func TestCache_PutAndGet(t *testing.T) {
	c := NewTTLCache(testTTL, testCleanup)
	defer c.Close()

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
	c := NewTTLCache(testTTL, testCleanup)
	defer c.Close()

	if _, err := c.Get(1); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// 3. По истечении TTL Get, Peek и Contains считают ключ отсутствующим
func TestCache_Expiration(t *testing.T) {
	c := NewTTLCache(testTTL, testCleanup)
	defer c.Close()

	c.Put(1, 10)
	time.Sleep(testTTL + testMargin)

	if _, err := c.Get(1); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("key should have expired, got err=%v", err)
	}
	if _, err := c.Peek(1); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("Peek should not see expired key, got err=%v", err)
	}
	if c.Contains(1) {
		t.Fatal("Contains should report false for expired key")
	}
}

// 4. Get продлевает TTL (скользящее окно), спасая ключ от истечения
func TestCache_GetRefreshesExpiration(t *testing.T) {
	c := NewTTLCache(testTTL, testCleanup)
	defer c.Close()

	c.Put(1, 10)

	deadline := time.Now().Add(testTTL + testMargin)
	for time.Now().Before(deadline) {
		if _, err := c.Get(1); err != nil {
			t.Fatalf("key should still be alive due to refresh, got err=%v", err)
		}
		time.Sleep(testTTL / 2)
	}

	if v, err := c.Get(1); err != nil || v != 10 {
		t.Fatalf("key kept alive by Get should survive, got v=%d err=%v", v, err)
	}
}

// 5. Peek не продлевает TTL: ключ истекает по расписанию, несмотря на повторные Peek
func TestCache_PeekDoesNotRefreshExpiration(t *testing.T) {
	c := NewTTLCache(testTTL, testCleanup)
	defer c.Close()

	c.Put(1, 10)

	deadline := time.Now().Add(testTTL - 5*time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := c.Peek(1); err != nil {
			t.Fatalf("unexpected error before expiration: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}

	time.Sleep(testTTL)

	if _, err := c.Peek(1); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("key should have expired despite repeated Peek, got err=%v", err)
	}
}

// 6. Delete удаляет ключ; повторное удаление или удаление отсутствующего ключа возвращает ошибку
func TestCache_Delete(t *testing.T) {
	c := NewTTLCache(testTTL, testCleanup)
	defer c.Close()

	c.Put(1, 10)

	if err := c.Delete(1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := c.Get(1); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("key 1 should be gone, got err=%v", err)
	}
	if err := c.Delete(1); !errors.Is(err, cache.ErrNotFound) {
		t.Fatalf("deleting missing key should return ErrNotFound, got %v", err)
	}
}

// 7. Len учитывает только неистёкшие элементы
func TestCache_LenExcludesExpired(t *testing.T) {
	c := NewTTLCache(testTTL, time.Hour) // фоновую уборку отключаем большим интервалом
	defer c.Close()

	c.Put(1, 10)
	c.Put(2, 20)
	if c.Len() != 2 {
		t.Fatalf("want len 2, got %d", c.Len())
	}

	time.Sleep(testTTL + testMargin)

	if c.Len() != 0 {
		t.Fatalf("want len 0 after expiration (before background sweep), got %d", c.Len())
	}
}

// 8. Keys возвращает только неистёкшие ключи
func TestCache_KeysExcludesExpired(t *testing.T) {
	c := NewTTLCache(testTTL, time.Hour)
	defer c.Close()

	c.Put(1, 10)
	c.Put(2, 20)
	time.Sleep(testTTL / 2)
	c.Put(3, 30) // добавлен позже, ещё не истечёт вместе с 1 и 2

	// ждём, пока 1 и 2 (expire в testTTL) истекут, но 3 (expire в 1.5*testTTL) ещё жив
	time.Sleep(testTTL/2 + 10*time.Millisecond)

	got := c.Keys()
	sort.Ints(got)
	want := []int{3}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("want %v, got %v", want, got)
	}
}

// 9. Clear опустошает кэш
func TestCache_Clear(t *testing.T) {
	c := NewTTLCache(testTTL, testCleanup)
	defer c.Close()

	c.Put(1, 10)
	c.Put(2, 20)

	c.Clear()

	if c.Len() != 0 {
		t.Fatalf("want len 0 after Clear, got %d", c.Len())
	}
	if got := c.Keys(); len(got) != 0 {
		t.Fatalf("want no keys after Clear, got %v", got)
	}
}

// 10. Фоновая уборка реально удаляет истёкшие записи из внутренней карты
func TestCache_BackgroundCleanupRemovesExpiredEntries(t *testing.T) {
	c := NewTTLCache(testTTL, testCleanup)
	defer c.Close()

	c.Put(1, 10)
	time.Sleep(testTTL + testCleanup + testMargin)

	c.mu.RLock()
	n := len(c.data)
	c.mu.RUnlock()

	if n != 0 {
		t.Fatalf("want internal map empty after background cleanup, got %d entries", n)
	}
}

// 11. Close можно вызывать более одного раза без паники
func TestCache_CloseIsIdempotent(t *testing.T) {
	c := NewTTLCache(testTTL, testCleanup)

	c.Close()
	c.Close() // не должно паниковать
}

// 12. Конкурентный доступ не гонится по данным (запускать с -race)
func TestCache_ConcurrentAccess(t *testing.T) {
	const (
		workers = 32
		opsEach = 200
	)
	c := NewTTLCache(time.Hour, time.Hour)
	defer c.Close()

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < opsEach; i++ {
				key := (w*opsEach + i) % 64
				c.Put(key, key*10)
				c.Get(key)
				c.Peek(key)
				c.Contains(key)
				c.Len()
				c.Keys()
				if key%7 == 0 {
					c.Delete(key)
				}
			}
		}(w)
	}
	wg.Wait()
}

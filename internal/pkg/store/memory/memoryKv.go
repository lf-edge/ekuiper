package memory

import (
	"fmt"
	"strings"

	"github.com/lf-edge/ekuiper/v2/pkg/kv"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
)

type memoryKvStore struct {
	data map[string]string
	mu   syncx.RWMutex
}

func NewMemoryKV() kv.KeyValue {
	return &memoryKvStore{
		data: make(map[string]string),
	}
}

func (m *memoryKvStore) Open() error {
	return nil
}

func (m *memoryKvStore) Close() error {
	return nil
}

func (m *memoryKvStore) Set(key string, value interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := value.(string); ok {
		m.data[key] = v
		return nil
	}
	return fmt.Errorf("value must be string")
}

func (m *memoryKvStore) Setnx(key string, value interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[key]; ok {
		return fmt.Errorf("key %s already exists", key)
	}
	if v, ok := value.(string); ok {
		m.data[key] = v
		return nil
	}
	return fmt.Errorf("value must be string")
}

func (m *memoryKvStore) Get(key string, value interface{}) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if v, ok := m.data[key]; ok {
		if ptr, ok := value.(*string); ok && ptr != nil {
			*ptr = v
		}
		return true, nil
	}
	return false, nil
}

func (m *memoryKvStore) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *memoryKvStore) Keys() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys, nil
}

func (m *memoryKvStore) All() (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Return a copy to avoid race conditions if the caller modifies the map (though signature returns map[string]string, usually it's better to copy)
	// But matching the interface, let's just return a copy.
	result := make(map[string]string, len(m.data))
	for k, v := range m.data {
		result[k] = v
	}
	return result, nil
}

func (m *memoryKvStore) Clean() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]string)
	return nil
}

func (m *memoryKvStore) Drop() error {
	return m.Clean()
}

func (m *memoryKvStore) SetKeyedState(key string, value interface{}) error {
	return m.Set(key, value)
}

func (m *memoryKvStore) GetKeyedState(key string) (interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if v, ok := m.data[key]; ok {
		return v, nil
	}
	return nil, nil
}

func (m *memoryKvStore) GetByPrefix(prefix string) (map[string][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string][]byte)
	for k, v := range m.data {
		if strings.HasPrefix(k, prefix) {
			result[k] = []byte(v)
		}
	}
	return result, nil
}

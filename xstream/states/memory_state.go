package states

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"sync"
)

type MemoryState struct {
	storage sync.Map
}

func newMemoryState() *MemoryState {
	return &MemoryState{
		storage: sync.Map{},
	}
}

func (s *MemoryState) IncrCounter(key string, amount int) error {
	if v, ok := s.storage.Load(key); ok {
		if vi, err := common.ToInt(v); err != nil {
			return fmt.Errorf("state[%s] must be an int", key)
		} else {
			s.storage.Store(key, vi+amount)
		}
	} else {
		s.storage.Store(key, amount)
	}
	return nil
}

func (s *MemoryState) GetCounter(key string) (int, error) {
	if v, ok := s.storage.Load(key); ok {
		if vi, err := common.ToInt(v); err != nil {
			return 0, fmt.Errorf("state[%s] is not a number, but %v", key, v)
		} else {
			return vi, nil
		}
	} else {
		s.storage.Store(key, 0)
		return 0, nil
	}
}

func (s *MemoryState) PutState(key string, value interface{}) error {
	s.storage.Store(key, value)
	return nil
}

func (s *MemoryState) GetState(key string) (interface{}, error) {
	if v, ok := s.storage.Load(key); ok {
		return v, nil
	} else {
		return nil, nil
	}
}

func (s *MemoryState) DeleteState(key string) error {
	s.storage.Delete(key)
	return nil
}

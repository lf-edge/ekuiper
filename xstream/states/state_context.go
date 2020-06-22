package states

import "github.com/emqx/kuiper/xstream/api"

type StateType int

const (
	MEMORY StateType = iota
)

type StateContext interface {
	IncrCounter(key string, amount int) error
	GetCounter(key string) (int, error)
	PutState(key string, value interface{}) error
	GetState(key string) (interface{}, error)
	DeleteState(key string) error
}

// If StateType is invalid, return a
func NewStateContext(st StateType, logger api.Logger) StateContext {
	switch st {
	case MEMORY:
		return &MemoryState{
			storage: make(map[string]interface{}),
		}
	default:
		logger.Warnf("request for invalid state type %d, return MemoryState instead", st)
		return &MemoryState{
			storage: make(map[string]interface{}),
		}
	}
	return nil
}

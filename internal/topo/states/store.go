package states

import (
	"github.com/emqx/kuiper/pkg/api"
)

const CheckpointListKey = "checkpoints"

func CreateStore(ruleId string, qos api.Qos) (api.Store, error) {
	if qos >= api.AtLeastOnce {
		return getKVStore(ruleId)
	} else {
		return newMemoryStore(), nil
	}
}

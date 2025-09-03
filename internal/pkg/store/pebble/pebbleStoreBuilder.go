package pebble

import "github.com/lf-edge/ekuiper/v2/pkg/kv"

type StoreBuilder struct {
	database KVDatabase
}

func NewStoreBuilder(d KVDatabase) *StoreBuilder {
	return &StoreBuilder{
		database: d,
	}
}

func (b StoreBuilder) CreateStore(name string) (kv.KeyValue, error) {
	return createPebbleKvStore(b.database, name)
}

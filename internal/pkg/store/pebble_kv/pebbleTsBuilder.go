package pebble_kv

import "github.com/lf-edge/ekuiper/v2/pkg/kv"

type TsBuilder struct {
	database KVDatabase
}

func NewTsBuilder(d KVDatabase) TsBuilder {
	return TsBuilder{
		database: d,
	}
}

func (b TsBuilder) CreateTs(name string) (kv.Tskv, error) {
	return createPebbleTs(b.database, name)
}

package pebble

import (
	"fmt"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/definition"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/pebble/pebble_kv"
)

func BuildStores(c definition.Config, name string) (definition.StoreBuilder, definition.TsBuilder, error) {
	db, err := pebble.NewPebbleDatabase(c, name)
	if err != nil {
		return nil, nil, err
	}

	if err = db.Connect(); err != nil {
		return nil, nil, err
	}

	d, ok := db.(KVDatabase)
	if !ok {
		return nil, nil, fmt.Errorf("unrecognized database type")
	}

	kvBuilder := NewStoreBuilder(d)
	tsBuilder := NewTsBuilder(d)

	return kvBuilder, tsBuilder, nil
}

func BuildPebbleStore(c definition.Config, name string) (KVDatabase, error) {
	db, err := pebble.NewPebbleDatabase(c, name)
	if err != nil {
		return nil, err
	}

	if err = db.Connect(); err != nil {
		return nil, err
	}

	d, ok := db.(KVDatabase)
	if !ok {
		return nil, fmt.Errorf("unrecognized database type")
	}

	return d, nil
}

package pebble_kv

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"

	kvEncoding "github.com/lf-edge/ekuiper/v2/internal/pkg/store/encoding"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"

	"github.com/cockroachdb/pebble"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

type pebbleKvStore struct {
	database KVDatabase
	table    string
}

func createPebbleKvStore(database KVDatabase, table string) (kv.KeyValue, error) {
	return &pebbleKvStore{
		database: database,
		table:    table,
	}, nil
}

func (p *pebbleKvStore) key(k string) []byte {
	return []byte(fmt.Sprintf("%s:%s", p.table, k))
}

func (p *pebbleKvStore) Setnx(key string, value interface{}) error {
	return p.database.Apply(func(db *pebble.DB) error {
		k := p.key(key)
		_, closer, err := db.Get(k)
		if err == nil {
			closer.Close()
			return fmt.Errorf("item %s already exists", key)
		} else if !errors.Is(err, pebble.ErrNotFound) {
			return err
		}

		b, err := kvEncoding.Encode(value)
		if err != nil {
			return err
		}

		return db.Set(k, b, pebble.Sync)
	})
}

func (p *pebbleKvStore) Set(key string, value interface{}) error {
	return p.database.Apply(func(db *pebble.DB) error {
		b, err := kvEncoding.Encode(value)
		if err != nil {
			return err
		}

		return db.Set(p.key(key), b, pebble.Sync)
	})
}

func (p *pebbleKvStore) Get(key string, value interface{}) (bool, error) {
	var found bool
	err := p.database.Apply(func(db *pebble.DB) error {
		k := p.key(key)
		data, closer, err := db.Get(k)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				found = false
				return nil
			}

			return err
		}
		defer closer.Close()

		dec := gob.NewDecoder(bytes.NewReader(data))
		if err = dec.Decode(value); err != nil {
			return err
		}

		found = true
		return nil
	})

	return found, err
}

func (p *pebbleKvStore) GetKeyedState(key string) (interface{}, error) {
	var val string
	found, err := p.Get(key, &val)
	if err != nil {
		return nil, err
	}

	if !found {
		return nil, fmt.Errorf("key not found")
	}

	return val, nil
}

func (p *pebbleKvStore) SetKeyedState(key string, value interface{}) error {
	return p.Set(key, value)
}

func (p *pebbleKvStore) Delete(key string) error {
	return p.database.Apply(func(db *pebble.DB) error {
		k := p.key(key)
		_, closer, err := db.Get(k)
		if err != nil {
			return errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("%s is not found", key))
		}
		closer.Close()
		return db.Delete(k, pebble.Sync)
	})
}

func (p *pebbleKvStore) Keys() ([]string, error) {
	var keys []string
	err := p.database.Apply(func(db *pebble.DB) error {
		iter, err := db.NewIter(nil)
		if err != nil {
			return err
		}
		defer iter.Close()

		prefix := []byte(p.table + ":")
		for iter.First(); iter.Valid(); iter.Next() {
			k := iter.Key()
			if !bytes.HasPrefix(k, prefix) {
				continue
			}

			keys = append(keys, string(k[len(prefix):]))
		}

		return nil
	})

	return keys, err
}

func (p *pebbleKvStore) All() (map[string]string, error) {
	all := make(map[string]string)
	err := p.database.Apply(func(db *pebble.DB) error {
		iter, err := db.NewIter(nil)
		if err != nil {
			return err
		}
		defer iter.Close()

		prefix := []byte(p.table + ":")
		for iter.First(); iter.Valid(); iter.Next() {
			k := iter.Key()
			v := iter.Value()
			if !bytes.HasPrefix(k, prefix) {
				continue
			}

			var val string
			if err = gob.NewDecoder(bytes.NewReader(v)).Decode(&val); err != nil {
				return err
			}

			all[string(k[len(prefix):])] = val
		}

		return nil
	})

	return all, err
}

func (p *pebbleKvStore) Clean() error {
	return p.Drop()
}

func (p *pebbleKvStore) Drop() error {
	return p.database.Apply(func(db *pebble.DB) error {
		iter, err := db.NewIter(nil)
		if err != nil {
			return err
		}
		defer iter.Close()

		batch := db.NewBatch()
		prefix := []byte(p.table + ":")
		for iter.First(); iter.Valid(); iter.Next() {
			k := iter.Key()
			if bytes.HasPrefix(k, prefix) {
				batch.Delete(k, pebble.Sync)
			}
		}

		return db.Apply(batch, pebble.Sync)
	})
}

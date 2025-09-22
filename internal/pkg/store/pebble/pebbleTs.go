// Copyright 2025 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pebble

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"

	kvEncoding "github.com/lf-edge/ekuiper/v2/internal/pkg/store/encoding"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

type pebbleTsStore struct {
	database KVDatabase
	table    string
	last     int64
}

func createPebbleTs(database KVDatabase, table string) (kv.Tskv, error) {
	last := getLastTs(database, table)
	return &pebbleTsStore{
		database: database,
		table:    table,
		last:     last,
	}, nil
}

func (t *pebbleTsStore) key(k int64) []byte {
	buf := new(bytes.Buffer)
	buf.WriteString(fmt.Sprintf("%s", t.table))
	_ = binary.Write(buf, binary.BigEndian, k)
	return buf.Bytes()
}

func (t *pebbleTsStore) Set(key int64, value interface{}) (bool, error) {
	if key <= t.last {
		return false, nil
	}

	err := t.database.Apply(func(db *pebble.DB) error {
		b, err := kvEncoding.Encode(value)
		if err != nil {
			return err
		}

		return db.Set(t.key(key), b, pebble.Sync)
	})
	if err != nil {
		return false, err
	}

	t.last = key
	return true, nil
}

func (t *pebbleTsStore) Get(key int64, value interface{}) (bool, error) {
	var found bool
	err := t.database.Apply(func(db *pebble.DB) error {
		data, closer, err := db.Get(t.key(key))
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

func (t *pebbleTsStore) Last(value interface{}) (int64, error) {
	_, err := t.Get(t.last, value)
	if err != nil {
		return 0, err
	}

	return t.last, nil
}

func (t *pebbleTsStore) Delete(key int64) error {
	return t.database.Apply(func(db *pebble.DB) error {
		return db.Delete(t.key(key), pebble.Sync)
	})
}

func (t *pebbleTsStore) DeleteBefore(key int64) error {
	return t.database.Apply(func(db *pebble.DB) error {
		iter, err := db.NewIter(nil)
		if err != nil {
			return err
		}
		defer iter.Close()

		batch := db.NewBatch()
		prefix := []byte(fmt.Sprintf("%s", t.table))

		for iter.First(); iter.Valid(); iter.Next() {
			k := iter.Key()
			if !bytes.HasPrefix(k, prefix) {
				continue
			}

			suffix := k[len(prefix):]
			var storedTs int64
			_ = binary.Read(bytes.NewReader(suffix), binary.BigEndian, &storedTs)

			if storedTs < key {
				_ = batch.Delete(k, pebble.Sync)
			}
		}

		return db.Apply(batch, pebble.Sync)
	})
}

func (t *pebbleTsStore) Close() error {
	return nil
}

func (t *pebbleTsStore) Drop() error {
	return t.DeleteBefore(^int64(0))
}

func getLastTs(d KVDatabase, table string) int64 {
	var last int64 = 0
	_ = d.Apply(func(db *pebble.DB) error {
		iter, err := db.NewIter(nil)
		if err != nil {
			return err
		}
		defer iter.Close()

		prefix := []byte(table)
		for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
			k := iter.Key()
			if !bytes.HasPrefix(k, prefix) {
				break
			}

			suffix := k[len(prefix):]
			if len(suffix) < 8 {
				continue
			}
			var ts int64
			_ = binary.Read(bytes.NewReader(suffix[:8]), binary.BigEndian, &ts)
			if ts > last {
				last = ts
			}
		}

		return nil
	})

	return last
}

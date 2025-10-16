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
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/cockroachdb/pebble"

	"github.com/lf-edge/ekuiper/v2/internal/conf/logger"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/definition"
)

type KVDatabase struct {
	db   *pebble.DB
	Path string
	mu   sync.Mutex
}

func NewPebbleDatabase(c definition.Config, name string) (definition.Database, error) {
	logger.Log.Infof("use pebble kv as store %v", name)
	pebbleConf := c.Pebble
	dir := pebbleConf.Path
	if dir == "" {
		return nil, fmt.Errorf("pebble directory path is empty in config")
	}
	if pebbleConf.Name != "" {
		name = pebbleConf.Name
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, fmt.Errorf("failed to create pebble dir: %w", err)
		}
	}
	dbPath := path.Join(dir, name)
	return &KVDatabase{
		db:   nil,
		Path: dbPath,
		mu:   sync.Mutex{},
	}, nil
}

func (d *KVDatabase) Connect() error {
	db, err := pebble.Open(d.Path, &pebble.Options{})
	if err != nil {
		return err
	}

	d.db = db
	return nil
}

func (d *KVDatabase) Disconnect() error {
	if d.db == nil {
		return nil
	}
	return d.db.Close()
}

func (d *KVDatabase) Apply(f func(db *pebble.DB) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return f(d.db)
}

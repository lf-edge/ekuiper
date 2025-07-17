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

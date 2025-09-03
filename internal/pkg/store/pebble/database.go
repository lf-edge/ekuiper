package pebble

import "github.com/cockroachdb/pebble"

type KVDatabase interface {
	Apply(f func(db *pebble.DB) error) error
}

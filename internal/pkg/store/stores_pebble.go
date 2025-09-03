//go:build pebble || !core
// +build pebble !core

package store

import (
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/pebble"
)

func init() {
	// Register pebble for extStateType only when the pebble tag is enabled
	storeBuilders["pebble"] = pebble.BuildStores
}

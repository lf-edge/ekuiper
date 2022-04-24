//go:build (all || most || trino) && !no_trino

package driver

import (
	_ "github.com/trinodb/trino-go-client/trino" // Trino driver
)

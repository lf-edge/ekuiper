//go:build (all || most || ignite) && !no_ignite

package driver

import (
	_ "github.com/amsokol/ignite-go-client/sql" // Apache Ignite driver
)

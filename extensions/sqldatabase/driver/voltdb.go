//go:build (all || most || voltdb) && !no_voltdb

package driver

import (
	_ "github.com/VoltDB/voltdb-client-go/voltdbclient" // VoltDB driver
)

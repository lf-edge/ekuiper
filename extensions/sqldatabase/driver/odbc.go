//go:build (all || odbc) && !no_odbc

package driver

import (
	_ "github.com/alexbrainman/odbc" // ODBC driver
)

//go:build (!no_base || sqlite3) && !no_sqlite3

package driver

import (
	_ "github.com/mattn/go-sqlite3" // SQLite3 driver
)

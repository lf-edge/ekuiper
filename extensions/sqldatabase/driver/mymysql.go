//go:build (all || most || mymysql) && !no_mymysql

package driver

import (
	_ "github.com/ziutek/mymysql/godrv" // MySQL MyMySQL driver
)

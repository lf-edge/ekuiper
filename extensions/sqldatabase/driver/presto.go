//go:build (all || most || presto) && !no_presto

package driver

import (
	_ "github.com/prestodb/presto-go-client/presto" // Presto driver
)

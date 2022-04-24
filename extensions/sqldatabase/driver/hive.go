//go:build (all || hive) && !no_hive

package driver

import (
	_ "sqlflow.org/gohive" // Apache Hive driver
)

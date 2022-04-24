//go:build (all || impala) && !no_impala

package driver

import (
	_ "github.com/bippio/go-impala" // Apache Impala driver
)

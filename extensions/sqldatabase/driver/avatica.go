//go:build (all || most || avatica) && !no_avatica

package driver

import (
	_ "github.com/apache/calcite-avatica-go/v5" // Apache Avatica driver
)

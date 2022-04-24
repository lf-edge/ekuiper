//go:build (all || most || vertica) && !no_vertica

package driver

import (
	_ "github.com/vertica/vertica-sql-go" // Vertica driver
)

//go:build (all || most || spanner) && !no_spanner

package driver

import (
	_ "github.com/googleapis/go-sql-spanner" // Google Spanner driver
)

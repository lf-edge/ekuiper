//go:build (!no_base || postgres) && !no_postgres

package driver

import (
	_ "github.com/lib/pq" // PostgreSQL driver
)

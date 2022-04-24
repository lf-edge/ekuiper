//go:build (all || most || pgx) && !no_pgx

package driver

import (
	_ "github.com/jackc/pgx/v4/stdlib" // PostgreSQL PGX driver
)

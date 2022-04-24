//go:build (all || most || clickhouse) && !no_clickhouse

package driver

import (
	_ "github.com/ClickHouse/clickhouse-go" // ClickHouse driver
)

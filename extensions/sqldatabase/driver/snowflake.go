//go:build (all || snowflake) && !no_snowflake

package driver

import (
	_ "github.com/snowflakedb/gosnowflake" // Snowflake driver
)

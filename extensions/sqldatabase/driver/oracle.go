//go:build (!no_base || oracle) && !no_oracle

package driver

import (
	_ "github.com/sijms/go-ora/v2" // Oracle Database driver
)

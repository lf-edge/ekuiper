//go:build (all || most || firebird) && !no_firebird

package driver

import (
	_ "github.com/nakagami/firebirdsql" // Firebird driver
)

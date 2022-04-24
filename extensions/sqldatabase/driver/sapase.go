//go:build (all || most || sapase) && !no_sapase

package driver

import (
	_ "github.com/thda/tds" // SAP ASE driver
)

//go:build (all || most || netezza) && !no_netezza

package driver

import (
	_ "github.com/IBM/nzgo" // Netezza driver
)

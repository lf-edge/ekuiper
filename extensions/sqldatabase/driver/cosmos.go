//go:build (all || cosmos) && !no_cosmos

package driver

import (
	_ "github.com/btnguyen2k/gocosmos" // Azure CosmosDB driver
)

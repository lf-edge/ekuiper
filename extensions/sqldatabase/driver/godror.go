//go:build (all || godror) && !no_godror

package driver

import (
	_ "github.com/godror/godror" // GO DRiver for ORacle driver
)

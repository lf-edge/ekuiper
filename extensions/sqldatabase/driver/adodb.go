//go:build (all || most || adodb) && !no_adodb

package driver

import (
	_ "github.com/mattn/go-adodb" // Microsoft ADODB driver
)

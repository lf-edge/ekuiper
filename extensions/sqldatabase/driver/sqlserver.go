//go:build (!no_base || sqlserver) && !no_sqlserver
// +build !no_base sqlserver
// +build !no_sqlserver

package driver

import (
	_ "github.com/denisenkom/go-mssqldb" // Microsoft SQL Server sqlgen
)

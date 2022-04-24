//go:build (!no_base || mysql) && !no_mysql
// +build !no_base mysql
// +build !no_mysql

package driver

import (
	_ "github.com/go-sql-driver/mysql" // Microsoft SQL Server sqlgen
)

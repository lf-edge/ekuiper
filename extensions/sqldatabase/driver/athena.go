//go:build (all || most || athena) && !no_athena

package driver

import (
	_ "github.com/uber/athenadriver/go" // AWS Athena driver
)

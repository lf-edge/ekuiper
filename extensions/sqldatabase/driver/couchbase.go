//go:build (all || most || couchbase) && !no_couchbase

package driver

import (
	_ "github.com/couchbase/go_n1ql" // Couchbase driver
)

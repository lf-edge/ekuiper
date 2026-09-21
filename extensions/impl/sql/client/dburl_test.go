// Copyright 2026 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParseDriver verifies static driver validation: malformed URLs and
// unknown schemes fail, and so do schemes that dburl recognizes but whose
// database/sql driver is not compiled into this binary (e.g. duckdb has no
// driver file at all, so it stays unregistered under every build tag).
// Supported drivers still resolve to their dialect without touching the
// network.
func TestParseDriver(t *testing.T) {
	_, err := ParseDriver("123")
	require.Error(t, err)

	_, err = ParseDriver("unknown-driver://database")
	require.Error(t, err)

	_, err = ParseDriver("duckdb:///tmp/ekuiper_parse_driver_test.db")
	require.Error(t, err)

	driver, err := ParseDriver("mysql://root:@127.0.0.1:3306/test")
	require.NoError(t, err)
	require.Equal(t, "mysql", driver)

	_, err = ParseDriver("sqlite:///tmp/ekuiper_parse_driver_test.db")
	require.NoError(t, err)
}

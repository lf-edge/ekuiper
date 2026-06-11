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

// These tests run in the default build (no duckdb build tag, no CGO) because
// dburl resolves the scheme from its built-in scheme table, independent of
// whether the Go driver is compiled in.
func TestParseDuckDBUrl(t *testing.T) {
	driver, dsn, err := ParseDBUrl("duckdb:///tmp/ekuiper_duckdb_test.db")
	require.NoError(t, err)
	require.Equal(t, "duckdb", driver)
	require.NotEmpty(t, dsn)
}

func TestParseDuckDBDriver(t *testing.T) {
	driver, err := ParseDriver("duckdb:///tmp/ekuiper_duckdb_test.db")
	require.NoError(t, err)
	require.Equal(t, "duckdb", driver)
}

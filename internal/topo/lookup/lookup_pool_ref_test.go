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

package lookup

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	// Import the SQL connector for its lookup source and drivers. There
	// is no import cycle: extensions/impl/sql never imports this package.
	// Registration is explicit because binder/io only registers the SQL
	// lookup source under the `full` build tag.
	sqlconnector "github.com/lf-edge/ekuiper/v2/extensions/impl/sql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

// TestTwoLookupTablesSharePoolConnection drives the full framework path
// for two lookup resources on the same database:
//
//	lookup.CreateInstance
//	  → framework injects "lookup:<name>"
//	  → SqlLookupSource canonicalizes the shared key and attaches
//	  → Pool holds two refs on one Meta
//
// then drops one table at a time and verifies the Pool ref lifecycle.
// Lookup Connect is attach-only, so no database needs to be reachable.
func TestTwoLookupTablesSharePoolConnection(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	modules.RegisterLookupSource("sql", sqlconnector.GetLookupSource)
	dburl := fmt.Sprintf("sqlite://%s", filepath.Join(t.TempDir(), "shared.db"))
	newOpts := func() *ast.Options {
		return &ast.Options{
			DATASOURCE: "t",
			EXTRA:      fmt.Sprintf(`{"dburl":%q}`, dburl),
		}
	}

	// Failure isolation only, registered BEFORE creating anything: if
	// the second CreateInstance fails, require.NoError exits the test
	// immediately and a later registration would never run, leaking the
	// first instance into the package-global registry. DropInstance on a
	// nonexistent name is nil, so registering early is safe. The success
	// path still drops explicitly to verify ref counts.
	t.Cleanup(func() {
		_ = DropInstance("poolTableA")
		_ = DropInstance("poolTableB")
	})
	require.NoError(t, CreateInstance("poolTableA", "sql", newOpts()))
	require.NoError(t, CreateInstance("poolTableB", "sql", newOpts()))

	meta, err := connection.GetConnectionDetail(nil, dburl)
	require.NoError(t, err)
	require.Equal(t, 2, meta.GetRefCount())
	require.ElementsMatch(t,
		[]string{"lookup:poolTableA", "lookup:poolTableB"},
		meta.GetRefNames(),
	)

	require.NoError(t, DropInstance("poolTableA"))
	meta, err = connection.GetConnectionDetail(nil, dburl)
	require.NoError(t, err)
	require.Equal(t, 1, meta.GetRefCount())
	require.Equal(t, []string{"lookup:poolTableB"}, meta.GetRefNames())

	require.NoError(t, DropInstance("poolTableB"))
	_, err = connection.GetConnectionDetail(nil, dburl)
	require.Error(t, err, "anonymous zero-ref Meta must be removed")
}

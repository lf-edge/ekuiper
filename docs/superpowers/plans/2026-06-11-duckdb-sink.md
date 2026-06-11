# DuckDB Sink Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add DuckDB write support to eKuiper's existing SQL sink so that rules can stream data into a DuckDB database file.

**Architecture:** eKuiper's SQL sink (`extensions/impl/sql/`) is fully driver-agnostic — connection management, INSERT/UPDATE/DELETE SQL construction, and the connection pool all flow through Go's `database/sql`. Adding a new database only requires registering its Go driver so `sql.Open("<driver>", dsn)` works. DuckDB plugs in by adding one driver-registration file that blank-imports `github.com/marcboeker/go-duckdb/v2`, which registers itself under the name `"duckdb"`. The `xo/dburl` library already maps the `duckdb://` URL scheme to driver name `"duckdb"`, so no URL-fixup code is needed.

**Tech Stack:** Go 1.25, `database/sql`, `github.com/marcboeker/go-duckdb/v2` (CGO — binds DuckDB C++), `github.com/xo/dburl` (URL→driver parsing), `github.com/stretchr/testify`. Tests gated behind a `duckdb` build tag.

**Spec:** `docs/superpowers/specs/2026-06-11-duckdb-sink-design.md`

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `extensions/impl/sql/sqldatabase/driver/duckdb.go` | Create | Blank-import go-duckdb so the `"duckdb"` driver registers with `database/sql`. Gated by `(all \|\| most \|\| duckdb) && !no_duckdb`. |
| `extensions/impl/sql/client/dburl_test.go` | Create | Non-gated unit test: `ParseDBUrl`/`ParseDriver` recognize the `duckdb` scheme. Runs in the default build (no CGO). |
| `extensions/impl/sql/client/duckdb_driver_test.go` | Create | `duckdb`-tagged test: `sql.Open("duckdb", "")` succeeds — proves the driver is registered. |
| `extensions/impl/sql/duckdb_test.go` | Create | `duckdb`-tagged end-to-end test: sink writes a row to a DuckDB file and it round-trips. |
| `go.mod` / `go.sum` | Modify | Add `github.com/marcboeker/go-duckdb/v2 v2.4.3`. |
| `docs/en_US/guide/sinks/plugin/sql.md` | Modify | Add DuckDB to supported list + build command + URL sample. |
| `docs/zh_CN/guide/sinks/plugin/sql.md` | Modify | Chinese equivalent. |

Files explicitly **not** changed (verified): `client/dburl.go` (driver name matches on both sides), `sqldatabase/sqlgen/sqlgen.go` (DuckDB uses the common-SQL default dialect), `impl/sql/sink.go` (driver-agnostic), `extensions/sinks/sql/sql.go` (plugin entry), `Makefile`, `build-plugins.sh`.

---

## Task 1: URL parsing unit test (default build)

This test runs in the default build (no CGO, no `duckdb` tag) because `xo/dburl` parses the scheme from its built-in table without needing the Go driver compiled in. It verifies the prerequisite that dburl already understands DuckDB.

**Files:**
- Create: `extensions/impl/sql/client/dburl_test.go`

- [ ] **Step 1: Write the test**

Create `extensions/impl/sql/client/dburl_test.go`:

```go
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
```

- [ ] **Step 2: Run the test (no tag, no CGO) — expect PASS**

Run:
```bash
CGO_ENABLED=0 go test -run 'TestParseDuckDB' ./extensions/impl/sql/client/
```
Expected: `PASS`. Both tests pass because dburl v0.23.2 ships with a `duckdb` scheme entry.

If this FAILS with a parse error, dburl does not recognize `duckdb://` — stop and re-check the dburl version/scheme before proceeding (the whole approach depends on this).

- [ ] **Step 3: Commit**

```bash
git add extensions/impl/sql/client/dburl_test.go
git commit -m "test(sql): add duckdb url parsing test"
```

---

## Task 2: Register the DuckDB driver (TDD red → green)

- [ ] **Step 1: Write the failing test**

Create `extensions/impl/sql/client/duckdb_driver_test.go`:

```go
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

//go:build duckdb

package client

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDuckDBDriverRegistered(t *testing.T) {
	db, err := sql.Open("duckdb", "") // "" = in-memory DuckDB
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.Ping())
}
```

- [ ] **Step 2: Run the test — expect FAIL (driver not yet registered)**

Run:
```bash
CGO_ENABLED=1 go test -tags duckdb -run TestDuckDBDriverRegistered ./extensions/impl/sql/client/
```
Expected: FAIL with `sql: unknown driver "duckdb" (forgotten import?)`. This confirms the test fails for the right reason before any implementation.

- [ ] **Step 3: Add the driver dependency**

Run:
```bash
go get github.com/marcboeker/go-duckdb/v2@v2.4.3
```
This updates `go.mod` and `go.sum`. Requires network access. (go-duckdb is a v2 module — the `/v2` suffix in the import path is mandatory.)

- [ ] **Step 4: Create the driver registration file**

Create `extensions/impl/sql/sqldatabase/driver/duckdb.go`:

```go
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

//go:build (all || most || duckdb) && !no_duckdb

package driver

import (
	_ "github.com/marcboeker/go-duckdb/v2" // DuckDB driver (registers as "duckdb")
)
```

- [ ] **Step 5: Run the test — expect PASS**

Run:
```bash
CGO_ENABLED=1 go test -tags duckdb -run TestDuckDBDriverRegistered ./extensions/impl/sql/client/
```
Expected: `PASS`. The first build will compile the bundled DuckDB C++ — it is slow (minutes) and requires a C/C++ compiler (clang via Xcode Command Line Tools on macOS; gcc on Linux).

- [ ] **Step 6: Verify the default build is unaffected**

Run:
```bash
CGO_ENABLED=0 go build ./extensions/impl/sql/...
```
Expected: succeeds. The `duckdb.go` file is excluded when its build tag is not satisfied, so the no-CGO build does not pull in go-duckdb.

- [ ] **Step 7: Commit**

```bash
git add extensions/impl/sql/sqldatabase/driver/duckdb.go go.mod go.sum extensions/impl/sql/client/duckdb_driver_test.go
git commit -m "feat(sql): add duckdb driver support"
```

---

## Task 3: Sink end-to-end test (write + round-trip)

This test proves the sink actually writes to DuckDB through the full sink pipeline (`Provision` → `Connect` → `collect` → `db.Exec`). It also empirically resolves the open question (spec Risk #1) of which URL form dburl translates into a usable DuckDB file DSN.

**Files:**
- Create: `extensions/impl/sql/duckdb_test.go`

- [ ] **Step 1: Write the test**

Create `extensions/impl/sql/duckdb_test.go`:

```go
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

//go:build duckdb

package sql

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/client"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestDuckDBSinkCollect(t *testing.T) {
	connection.InitConnectionManager4Test()
	ctx := mockContext.NewMockContext("1", "2")

	// File-mode URL. Parse it through the SAME path the sink uses so the
	// setup connection and the sink open the identical database file.
	dbPath := filepath.Join(t.TempDir(), "test.db")
	dburl := "duckdb://" + dbPath
	driver, dsn, err := client.ParseDBUrl(dburl)
	require.NoError(t, err)
	require.Equal(t, "duckdb", driver)

	// The sink only issues INSERT; create the target table up front.
	setup, err := sql.Open("duckdb", dsn)
	require.NoError(t, err)
	_, err = setup.Exec("CREATE TABLE t (a INTEGER, b INTEGER)")
	require.NoError(t, err)
	require.NoError(t, setup.Close())

	// Drive the sink with a single insert.
	sink := &SQLSinkConnector{}
	require.NoError(t, sink.Provision(ctx, map[string]any{
		"dburl":  dburl,
		"table":  "t",
		"fields": []string{"a", "b"},
	}))
	sink.Consume(map[string]any{})
	require.NoError(t, sink.Connect(ctx, func(string, string) {}))
	require.NoError(t, sink.collect(ctx, map[string]any{"a": 1, "b": 2}))

	// Reopen the file and confirm the row landed.
	verify, err := sql.Open("duckdb", dsn)
	require.NoError(t, err)
	rows, err := verify.Query("SELECT a, b FROM t WHERE a = 1 AND b = 2")
	require.NoError(t, err)
	count := 0
	for rows.Next() {
		var a, b int
		require.NoError(t, rows.Scan(&a, &b))
		require.Equal(t, 1, a)
		require.Equal(t, 2, b)
		count++
	}
	require.NoError(t, rows.Close())
	require.NoError(t, verify.Close())
	require.NoError(t, sink.Close(ctx))
	require.Equal(t, 1, count)
}
```

- [ ] **Step 2: Run the test — expect PASS**

Run:
```bash
CGO_ENABLED=1 go test -tags duckdb -run TestDuckDBSinkCollect ./extensions/impl/sql/
```
Expected: `PASS`.

If the test FAILS at `sql.Open("duckdb", dsn)` in setup, dburl did not produce a DSN go-duckdb accepts for a file path. Try the alternative URL forms below, one at a time, re-running the test each time, and keep the first that passes:
1. `dburl := "duckdb://" + dbPath` (current; matches the `sqlite://test.db` convention in the docs)
2. `dburl := "duckdb://" + filepath.Base(dbPath)` with the working dir set to the temp dir
3. Direct DSN bypass: if none of the URL forms work through dburl, the sink's URL flow cannot reach DuckDB and the approach needs revisiting — raise it rather than forcing a workaround.

Record the working URL form; it is what the docs (Task 4) must document.

- [ ] **Step 3: Commit**

```bash
git add extensions/impl/sql/duckdb_test.go
git commit -m "test(sql): add duckdb sink end-to-end test"
```

---

## Task 4: Documentation

**Files:**
- Modify: `docs/en_US/guide/sinks/plugin/sql.md`
- Modify: `docs/zh_CN/guide/sinks/plugin/sql.md`

- [ ] **Step 1: Add DuckDB to the supported-drivers line (English)**

In `docs/en_US/guide/sinks/plugin/sql.md`, the line currently reads:

```
This plugin supports `sqlserver\postgres\mysql\sqlite3\oracle` drivers by default. User can compile plugin that only support one driver by himself,
```

Change the supported-drivers sentence to mention that DuckDB is available via a build tag (it is NOT in the default build because it requires CGO):

```
This plugin supports `sqlserver\postgres\mysql\sqlite3\oracle` drivers by default. DuckDB is also supported but requires CGO, so it is not included in the default build — build with the `duckdb` tag (see below).
```

- [ ] **Step 2: Add a DuckDB build command section (English)**

After the `### MySql build command` section (which ends with the `cp plugins/sinks/Sql.so ...` line), insert:

```markdown
### DuckDB build command

DuckDB requires CGO and a C/C++ compiler.

	# cd $eKuiper_src
	# CGO_ENABLED=1 go build -trimpath --buildmode=plugin -tags duckdb -o plugins/sinks/Sql.so extensions/sinks/sql/sql.go
	# cp plugins/sinks/Sql.so $eKuiper_install/plugins/sinks
```

- [ ] **Step 3: Add a DuckDB URL sample (English)**

In the `### Update Sample` area near the existing `sqlite://test.db` sample, add a DuckDB example block alongside it:

```json
        "url": "duckdb://test.db",
```

with a note: `DuckDB is single-writer; for file mode set eKuiper's SQL maxConnections to 1 to avoid lock contention. Use duckdb:///absolute/path/to.db for absolute paths.`

Use the URL form confirmed in Task 3 Step 2. If Task 3 confirmed a different form, use that here instead.

- [ ] **Step 4: Mirror the three changes in the Chinese doc**

Apply the equivalent edits to `docs/zh_CN/guide/sinks/plugin/sql.md`:
- Update the supported-drivers sentence to note DuckDB needs CGO + the `duckdb` build tag.
- Add a `### DuckDB 构建命令` section with the same build command.
- Add the `duckdb://test.db` URL sample with the single-writer note in Chinese.

- [ ] **Step 5: Commit**

```bash
git add docs/en_US/guide/sinks/plugin/sql.md docs/zh_CN/guide/sinks/plugin/sql.md
git commit -m "docs(sql): document duckdb sink support"
```

---

## Definition of Done

- [ ] `CGO_ENABLED=0 go test ./extensions/impl/sql/client/` passes (Task 1, runs in default CI).
- [ ] `CGO_ENABLED=1 go test -tags duckdb ./extensions/impl/sql/...` passes (Tasks 2 & 3).
- [ ] `CGO_ENABLED=0 go build ./extensions/impl/sql/...` still succeeds (default build unaffected).
- [ ] `go.mod` contains `github.com/marcboeker/go-duckdb/v2 v2.4.3`.
- [ ] Both English and Chinese SQL sink docs mention DuckDB, its build command, and a URL sample.
- [ ] A custom plugin can be built and used: `CGO_ENABLED=1 go build -trimpath --buildmode=plugin -tags duckdb -o plugins/sinks/Sql.so extensions/sinks/sql/sql.go`.

---

## Notes for the implementer

- **Why `/v2`:** `github.com/marcboeker/go-duckdb` is a v2 Go module. The import path must include `/v2` or it will not resolve. Verified against the package source.
- **Driver name:** go-duckdb calls `sql.Register("duckdb", ...)`; `xo/dburl` maps `duckdb://` → driver `"duckdb"`. They match, which is why `client/dburl.go` needs no special-case (unlike sqlite3, which dburl names `"sqlite3"` but the modernc driver registers as `"sqlite"`).
- **CGO is mandatory** for go-duckdb (it embeds the DuckDB C++ engine). The default `CGO_ENABLED=0` eKuiper build cannot use it; that is why the driver is gated behind the `duckdb` build tag rather than placed in the `base` set.
- **First compile is slow** (compiles bundled DuckDB C++). Subsequent builds are cached.
- **CI gap:** eKuiper's CI test matrix uses `-tags "full deadlock"`, which does not include `duckdb`. The Task 2/3 tests therefore do not run in default CI. Running them is a local/manual step unless a dedicated CI job is added (out of scope for this plan).
- **DuckDB single-writer:** DuckDB serializes writes to a file. If a deployment opens many connections, set eKuiper's SQL `maxConnections` to 1 (see `internal/conf` `SQLConf.MaxConnections`, consumed in `client/dburl.go:openDB`).

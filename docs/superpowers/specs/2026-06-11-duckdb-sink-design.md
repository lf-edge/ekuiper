# DuckDB Sink Support for eKuiper

**Date:** 2026-06-11
**Scope:** SQL sink only (write path)
**Approach:** Register `go-duckdb` driver into existing SQL sink infrastructure

## Background

eKuiper's SQL sink (`extensions/impl/sql/`) is a driver-agnostic module that supports 40+ databases via the Go `database/sql` interface. Each database is integrated by adding a driver registration file under `extensions/impl/sql/sqldatabase/driver/`. DuckDB is an embedded analytical database compatible with PostgreSQL dialect, making it a natural fit for this architecture.

## Architecture

DuckDB reuses the entire existing SQL sink stack — connection management, SQL construction, INSERT/UPDATE/DELETE logic. The only addition is registering `go-duckdb` as a `database/sql` driver so that `sql.Open("duckdb", dsn)` works.

### Data Flow

```
User config: url = "duckdb:///path/to/file.db"
  -> dburl.Parse         -> driver="duckdb", dsn="/path/to/file.db"
  -> sql.Open("duckdb", dsn) -> go-duckdb handles connection
  -> sink.Collect        -> buildInsertSQL -> db.Exec -> write to DuckDB
```

### Driver Name Resolution

- `xo/dburl` v0.23.2 natively supports the `duckdb://` scheme (alias `dk`), parsing it to driver name `"duckdb"`.
- `duckdb-go` (`github.com/duckdb/duckdb-go/v2`) registers itself under the name `"duckdb"` via `sql.Register("duckdb", ...)`.
- Names match — no fixup needed in `dburl.go` (unlike sqlite3 which requires a rename from "sqlite3" to "sqlite").

### SQL Dialect

DuckDB uses a PostgreSQL-compatible SQL dialect. The `GetQueryGenerator` function in `sqlgen.go` routes unknown drivers to `NewCommonSqlQuery`, which produces standard SQL compatible with DuckDB. No dialect-specific changes needed.

## File Changes

### 1. New: `extensions/impl/sql/sqldatabase/driver/duckdb.go`

A blank-import driver file with build tag, matching the pattern of `clickhouse.go`, `trino.go`, etc.

```go
// Copyright 2026 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...

//go:build (all || most || duckdb) && !no_duckdb

package driver

import (
    _ "github.com/duckdb/duckdb-go/v2"
)
```

Build tag `(all || most || duckdb) && !no_duckdb` — consistent with all other non-base SQL drivers. DuckDB is compiled in only when building with `-tags duckdb`, `-tags most`, or `-tags all`.

### 2. Modified: `go.mod` / `go.sum`

Add the official `github.com/duckdb/duckdb-go/v2` dependency (the successor to the deprecated `github.com/marcboeker/go-duckdb`; it is a v2 module, so the `/v2` suffix is required in the import path). Pinned version is `v2.10503.1`. It ships pre-built per-platform bindings, so no DuckDB C++ is compiled from source, but CGO is still required to link them.

```bash
go get github.com/duckdb/duckdb-go/v2@v2.10503.1
```

### 3. No changes needed

| File | Reason |
|---|---|
| `extensions/impl/sql/client/dburl.go` | Driver name "duckdb" matches on both sides |
| `extensions/impl/sql/sqldatabase/sqlgen/sqlgen.go` | DuckDB uses common SQL dialect |
| `extensions/impl/sql/sink.go` | INSERT/UPDATE/DELETE logic is driver-agnostic |
| `extensions/sinks/sql/sql.go` | Plugin entry point unchanged |
| `Makefile` | No changes needed |
| `build-plugins.sh` | No changes (see build/distribution below) |

## Build & Distribution

### How SQL plugins are built

The SQL sink is an **external plugin**, not compiled into the main eKuiper binary. CI runs `make sinks/sql` → `build-plugins.sh sinks sql` → `go build --buildmode=plugin` with no extra tags. Under default tags, only `base` drivers (mysql, postgres, oracle, sqlserver, sqlite3) are compiled in. Non-base drivers (clickhouse, trino, duckdb, etc.) require explicit build tags.

### Custom build command for DuckDB

```bash
CGO_ENABLED=1 go build -trimpath --buildmode=plugin -tags duckdb \
  -o plugins/sinks/sql.so \
  extensions/sinks/sql/sql.go
```

CGO is required because `go-duckdb` embeds the DuckDB C++ library.

### Build-plugins.sh: intentionally unchanged

Adding a `sql)` case to `build-plugins.sh` that passes `-tags duckdb` would make DuckDB available in the standard plugin build, but would introduce CGO + C++ compiler requirements into the CI matrix (debian + alpine, amd64 + arm64). This is deferred — users who need DuckDB can custom-build the plugin, matching how clickhouse and other non-base drivers work today.

## Runtime Considerations

### Single-writer constraint

DuckDB allows only one concurrent writer to a file. eKuiper's SQL connection pool defaults to `MaxOpenConns` from `SQLConf`. When using DuckDB file mode, set `maxConnections` to 1, or use in-memory mode (`duckdb:` or `duckdb::memory:`) to avoid file lock contention.

### Supported URL formats

| Mode | URL | Notes |
|---|---|---|
| File | `duckdb:///path/to/file.db` | Persistent storage, single writer |
| In-memory | (needs verification, see Risks) | No persistence, no file lock issues |
| Alias | `dk:///path/to/file.db` | Short form via dburl alias |

The go-duckdb driver accepts DSN `""` or `":memory:"` for in-memory databases. The exact dburl URL that maps to these DSNs must be verified at runtime during implementation (see Risks).

## Testing

Reference: `extensions/impl/sql/sink_test.go`.

- DuckDB test code must be gated behind the `duckdb` build tag, because CI's default test invocation uses `-tags "full deadlock"`, which does not satisfy the driver's build constraint. Without the gate, tests either fail to compile or fail at runtime (driver not registered).
- Use DuckDB in-memory mode for CI-friendly tests (no filesystem, no cleanup).
- Test cases:
  1. URL parsing: `dburl.Parse(...)` returns driver="duckdb" for the chosen URL.
  2. Connection open: `sql.Open("duckdb", dsn)` succeeds for in-memory mode.
  3. INSERT flow: configure sink with a file-mode DuckDB URL (`duckdb://<tempfile>`), `table: "t"`, verify the row round-trips.
- Tests run with: `CGO_ENABLED=1 go test -tags duckdb ./extensions/impl/sql/`

## Risks / To Verify During Implementation

1. **URL → DSN mapping (file mode confirmed).** File mode `duckdb://<abspath>` is verified working: `xo/dburl` translates it to a DSN that `duckdb-go` accepts as a file path, and the sink round-trips data (see `TestDuckDBSinkCollect`). In-memory mode (`""` / `:memory:`) reached through a dburl URL remains unverified; users should prefer file mode.
2. **Test isolation.** DuckDB tests require CGO + the `duckdb` build tag; they will not run in CI's default test matrix. Decide whether to add a dedicated CI job or keep tests manual.
3. **Version pinning (resolved).** Using the official `github.com/duckdb/duckdb-go/v2` upstream directly (the deprecated `marcboeker/go-duckdb` was avoided per user decision).

## Documentation

Update `docs/en_US/sqls/sinks/sql.md` and `docs/zh_CN/sqls/sinks/sql.md`:
- Add DuckDB to supported databases list.
- Add file-mode URL example (`duckdb:///path/to/file.db`); add in-memory example only after the URL form is verified (see Risks).
- Note CGO requirement and custom build instructions.

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

//go:build mysql_integration_test

package sql

import (
	"database/sql"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/testx"
)

const benchAddr = "localhost"
const benchPort = 33071

func openBenchDB(b *testing.B, dsn string) *sql.DB {
	b.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(b, err)
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS bench (a BIGINT, b BIGINT, note VARCHAR(255))`)
	require.NoError(b, err)
	return db
}

// latencyProxy forwards TCP to target, sleeping delay once per forwarded
// chunk in each direction. MySQL packets arrive as small chunks, so this
// approximates an extra RTT per request-response leg. Crude but sufficient
// for relative comparison between text and prepare-based execution.
func latencyProxy(target string, delay time.Duration) (string, func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	done := make(chan struct{})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				up, err := net.Dial("tcp", target)
				if err != nil {
					return
				}
				defer up.Close()
				quit := make(chan struct{}, 2)
				fwd := func(dst, src net.Conn) {
					defer func() { quit <- struct{}{} }()
					buf := make([]byte, 32*1024)
					for {
						n, err := src.Read(buf)
						if n > 0 {
							if delay > 0 {
								time.Sleep(delay)
							}
							if _, err := dst.Write(buf[:n]); err != nil {
								return
							}
						}
						if err != nil {
							return
						}
					}
				}
				go fwd(up, c)
				go fwd(c, up)
				select {
				case <-quit:
				case <-done:
				}
			}(c)
		}
	}()
	return ln.Addr().String(), func() { close(done); ln.Close() }
}

func benchSetup(b *testing.B, dsnExtra string) (*sql.DB, func()) {
	b.Helper()
	s, err := testx.SetupEmbeddedMysqlServer(benchAddr, benchPort)
	require.NoError(b, err)
	dsn := fmt.Sprintf("root:@tcp(%s:%d)/test%s", benchAddr, benchPort, dsnExtra)
	return openBenchDB(b, dsn), func() { s.Close() }
}

func BenchmarkMySQLInsertSingle(b *testing.B) {
	for _, tc := range []struct {
		name  string
		extra string
		args  bool
	}{
		{"inline", "", false},
		{"params", "", true},
		{"interpolate", "?interpolateParams=true", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			db, cleanup := benchSetup(b, tc.extra)
			defer cleanup()
			defer db.Close()
			text := `INSERT INTO bench (a,b,note) values (1,2,'O''Brien');`
			prep := `INSERT INTO bench (a,b,note) values (?,?,?);`
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				if tc.args {
					_, err = db.Exec(prep, 1, 2, "O'Brien")
				} else {
					_, err = db.Exec(text)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMySQLInsertBatch100(b *testing.B) {
	var textRows, argRows []string
	var args []any
	for i := 0; i < 100; i++ {
		textRows = append(textRows, fmt.Sprintf(`(%d,%d,'n%d''x')`, i, i, i))
		argRows = append(argRows, "(?,?,?)")
		args = append(args, i, i, fmt.Sprintf("n%d'x", i))
	}
	text := `INSERT INTO bench (a,b,note) values ` + strings.Join(textRows, ",") + ";"
	prep := `INSERT INTO bench (a,b,note) values ` + strings.Join(argRows, ",") + ";"
	for _, tc := range []struct {
		name  string
		extra string
		args  bool
	}{
		{"inline", "", false},
		{"params", "", true},
		{"interpolate", "?interpolateParams=true", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			db, cleanup := benchSetup(b, tc.extra)
			defer cleanup()
			defer db.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				if tc.args {
					_, err = db.Exec(prep, args...)
				} else {
					_, err = db.Exec(text)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMySQLUpdateSingle(b *testing.B) {
	for _, tc := range []struct {
		name  string
		extra string
		args  bool
	}{
		{"inline", "", false},
		{"params", "", true},
		{"interpolate", "?interpolateParams=true", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			db, cleanup := benchSetup(b, tc.extra)
			defer cleanup()
			defer db.Close()
			if _, err := db.Exec(`INSERT INTO bench (a,b,note) values (1,1,'init');`); err != nil {
				b.Fatal(err)
			}
			text := `UPDATE bench SET b=2 WHERE a = 1;`
			prep := `UPDATE bench SET b=? WHERE a = ?;`
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				if tc.args {
					_, err = db.Exec(prep, 2, 1)
				} else {
					_, err = db.Exec(text)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMySQLInsertSingleRTT2ms(b *testing.B) {
	s, err := testx.SetupEmbeddedMysqlServer(benchAddr, benchPort)
	require.NoError(b, err)
	defer s.Close()
	target := fmt.Sprintf("%s:%d", benchAddr, benchPort)
	proxy, stop := latencyProxy(target, time.Millisecond)
	defer stop()
	for _, tc := range []struct {
		name  string
		extra string
		args  bool
	}{
		{"inline", "", false},
		{"params", "", true},
		{"interpolate", "?interpolateParams=true", true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			db, err := sql.Open("mysql", fmt.Sprintf("root:@tcp(%s)/test%s", proxy, tc.extra))
			require.NoError(b, err)
			defer db.Close()
			db.SetMaxOpenConns(1)
			if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS bench (a BIGINT, b BIGINT, note VARCHAR(255))`); err != nil {
				b.Fatal(err)
			}
			text := `INSERT INTO bench (a,b,note) values (1,2,'O''Brien');`
			prep := `INSERT INTO bench (a,b,note) values (?,?,?);`
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				if tc.args {
					_, err = db.Exec(prep, 1, 2, "O'Brien")
				} else {
					_, err = db.Exec(text)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

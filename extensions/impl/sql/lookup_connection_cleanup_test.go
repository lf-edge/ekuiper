// Copyright 2024 EMQ Technologies Co., Ltd.
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

package sql

import (
	stdcontext "context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/sql/testx"
	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestLookupConnectionFailureCleanup(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	ctx := mockContext.NewMockContext("lookup_cleanup", "op1")

	failed := &SqlLookupSource{
		conf:  &SQLConf{DBUrl: "unknown-driver://database"},
		props: map[string]any{"dburl": "unknown-driver://database"},
	}
	require.Error(t, failed.Connect(ctx, nil))
	_, err := connection.GetConnectionDetail(ctx, "unknown-driver://database")
	require.Error(t, err, "a failed lookup connection must not remain in the pool")

	validURL := fmt.Sprintf("sqlite://%s/lookup.db", t.TempDir())
	valid := &SqlLookupSource{
		conf:  &SQLConf{DBUrl: validURL},
		props: map[string]any{"dburl": validURL},
	}
	require.NoError(t, valid.Connect(ctx, nil))
	require.NoError(t, valid.Close(ctx))
}

func TestLookupConnectionTimeoutDetachesAnonymousConnection(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	baseCtx := mockContext.NewMockContext("lookup_timeout", "op1")
	waitCtx, cancel := stdcontext.WithTimeout(baseCtx, 100*time.Millisecond)
	defer cancel()
	ctx := topoContext.WithContext(waitCtx)
	dbURL := "mysql://root:@127.0.0.1:33065/test"
	lookup := &SqlLookupSource{
		conf:  &SQLConf{DBUrl: dbURL},
		props: map[string]any{"dburl": dbURL},
	}
	err := lookup.Connect(ctx, nil)
	require.True(t, errors.Is(err, stdcontext.DeadlineExceeded), "got %v", err)
	_, err = connection.GetConnectionDetail(baseCtx, dbURL)
	require.Error(t, err, "timed-out anonymous connection must be removed from the pool")
}

func TestLookupNamedConnectionTimeoutThenRecovery(t *testing.T) {
	require.NoError(t, connection.InitConnectionManager4Test())
	baseCtx := mockContext.NewMockContext("lookup_recovery", "op1")
	const connectionID = "lookup-recovery-connection"
	const port = 33066
	dbURL := fmt.Sprintf("mysql://root:@%v:%v/test", address, port)
	props := map[string]any{"dburl": dbURL, "connectionSelector": connectionID}
	cw, err := connection.CreateNamedConnection(baseCtx, connectionID, "sql", map[string]any{"dburl": dbURL})
	require.NoError(t, err)
	defer func() { require.NoError(t, connection.DropNameConnection(baseCtx, connectionID)) }()

	waitCtx, cancel := stdcontext.WithTimeout(baseCtx, 100*time.Millisecond)
	first := &SqlLookupSource{conf: &SQLConf{DBUrl: dbURL}, props: props}
	err = first.Connect(topoContext.WithContext(waitCtx), nil)
	cancel()
	require.ErrorIs(t, err, stdcontext.DeadlineExceeded)
	meta, err := connection.GetConnectionDetail(baseCtx, connectionID)
	require.NoError(t, err)
	require.Equal(t, 0, meta.GetRefCount())

	s, err := testx.SetupEmbeddedMysqlServer(address, port)
	require.NoError(t, err)
	defer s.Close()
	recoveryCtx, recoveryCancel := stdcontext.WithTimeout(baseCtx, 15*time.Second)
	defer recoveryCancel()
	conn, err := cw.Wait(topoContext.WithContext(recoveryCtx))
	require.NoError(t, err)
	require.NotNil(t, conn)

	second := &SqlLookupSource{conf: &SQLConf{DBUrl: dbURL}, props: props}
	require.NoError(t, second.Connect(baseCtx, nil))
	require.Equal(t, 1, meta.GetRefCount())
	require.NoError(t, second.Close(baseCtx))
	require.Equal(t, 0, meta.GetRefCount())
}

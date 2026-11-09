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

package pgvector

import (
	"testing"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestLifecycleMethods(t *testing.T) {
	s := &lookupSource{}
	ctx := mockContext.NewMockContext("test", "tt")
	require.NoError(t, s.Provision(ctx, testProps()))
	assert.NoError(t, s.Open(ctx))
	assert.NoError(t, s.Close(ctx))
	assert.NotNil(t, GetLookupSource())
}

func TestValidIdentifier(t *testing.T) {
	assert.True(t, validIdentifier("fault_history"))
	assert.True(t, validIdentifier("t1"))
	assert.True(t, validIdentifier("public.t"))
	assert.True(t, validIdentifier("_x"))
	assert.False(t, validIdentifier(""))
	assert.False(t, validIdentifier("1t"))
	assert.False(t, validIdentifier("t x"))
	assert.False(t, validIdentifier("t-x"))
	assert.False(t, validIdentifier("t;drop"))
}

func TestValidateDimensionConfig(t *testing.T) {
	s := &lookupSource{}
	props := testProps()
	props["dimension"] = 2
	require.NoError(t, s.Validate(props))
	assert.Equal(t, 2, s.c.Dimension)
}

func TestPingUnreachableHost(t *testing.T) {
	s := &lookupSource{}
	err := s.Ping(mockContext.NewMockContext("test", "tt"), map[string]any{
		"url": "postgres://127.0.0.1:1/db?connect_timeout=1", "table": "t", "vectorField": "v",
	})
	assert.Error(t, err)
}

func TestConnectUnreachableHost(t *testing.T) {
	s := &lookupSource{}
	require.NoError(t, s.Provision(mockContext.NewMockContext("test", "tt"), map[string]any{
		"url": "postgres://127.0.0.1:1/db?connect_timeout=1", "table": "t", "vectorField": "v",
	}))
	var state string
	err := s.Connect(mockContext.NewMockContext("test", "tt"), func(st string, _ string) {
		state = st
	})
	assert.Error(t, err)
	assert.Equal(t, api.ConnectionDisconnected, state)
}

func TestLookupMismatchedKeysValues(t *testing.T) {
	s := &lookupSource{}
	require.NoError(t, s.Validate(testProps()))
	ctx := mockContext.NewMockContext("test", "tt")
	_, err := s.Lookup(ctx, nil, []string{"a"}, []any{})
	assert.Error(t, err)
}

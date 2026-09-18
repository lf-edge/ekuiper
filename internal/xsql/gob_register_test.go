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

package xsql

import (
	"bytes"
	"encoding/gob"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	topoContext "github.com/lf-edge/ekuiper/v2/internal/topo/context"
)

// legacyTupleWire mirrors the exported Tuple fields before its tracing context
// became transient checkpoint state.
type legacyTupleWire struct {
	Ctx       api.StreamContext
	Emitter   string
	Message   Message
	Timestamp time.Time
	Metadata  Metadata
	Props     map[string]string

	AffiliateRow
}

func TestTupleGobLegacyCompatibility(t *testing.T) {
	timestamp := time.UnixMilli(123456789)
	legacy := &legacyTupleWire{
		Emitter:   "source",
		Message:   Message{"id": int64(1)},
		Timestamp: timestamp,
		Metadata:  Metadata{"topic": "demo"},
		Props:     map[string]string{"format": "json"},
		AffiliateRow: AffiliateRow{
			CalCols:  map[string]interface{}{"calculated": int64(2)},
			AliasMap: map[string]interface{}{"alias": "value"},
		},
	}

	var legacyBytes bytes.Buffer
	require.NoError(t, gob.NewEncoder(&legacyBytes).Encode(legacy))
	var current Tuple
	require.NoError(t, gob.NewDecoder(&legacyBytes).Decode(&current))
	require.Nil(t, current.GetTracerCtx())
	require.Equal(t, legacy.Emitter, current.Emitter)
	require.Equal(t, legacy.Message, current.Message)
	require.Equal(t, legacy.Timestamp, current.Timestamp)
	require.Equal(t, legacy.Metadata, current.Metadata)
	require.Equal(t, legacy.Props, current.Props)
	require.Equal(t, legacy.CalCols, current.CalCols)
	require.Equal(t, legacy.AliasMap, current.AliasMap)

	traceCtx := topoContext.Background()
	current.SetTracerCtx(traceCtx)
	var currentBytes bytes.Buffer
	require.NoError(t, gob.NewEncoder(&currentBytes).Encode(&current))
	var decodedLegacy legacyTupleWire
	require.NoError(t, gob.NewDecoder(&currentBytes).Decode(&decodedLegacy))
	require.Nil(t, decodedLegacy.Ctx)
	require.Equal(t, current.Emitter, decodedLegacy.Emitter)
	require.Equal(t, current.Message, decodedLegacy.Message)
	require.Equal(t, current.Timestamp, decodedLegacy.Timestamp)
	require.Equal(t, current.Metadata, decodedLegacy.Metadata)
	require.Equal(t, current.Props, decodedLegacy.Props)
	require.Equal(t, current.CalCols, decodedLegacy.CalCols)
	require.Equal(t, current.AliasMap, decodedLegacy.AliasMap)
}

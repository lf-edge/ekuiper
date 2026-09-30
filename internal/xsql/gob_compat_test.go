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

	"github.com/stretchr/testify/require"
)

func TestTupleGobIgnoresLegacyContextField(t *testing.T) {
	// The old wire format includes an exported Ctx field. New decoders ignore it.
	type legacyTuple struct {
		Ctx     string
		Message Message
		Props   map[string]string
	}
	var encoded bytes.Buffer
	require.NoError(t, gob.NewEncoder(&encoded).Encode(&legacyTuple{
		Ctx:     "runtime only",
		Message: Message{"value": 1},
		Props:   map[string]string{"topic": "out"},
	}))
	var restored Tuple
	require.NoError(t, gob.NewDecoder(&encoded).Decode(&restored))
	require.Nil(t, restored.GetTracerCtx())
	require.Equal(t, 1, restored.Message["value"])
	require.Equal(t, "out", restored.Props["topic"])
}

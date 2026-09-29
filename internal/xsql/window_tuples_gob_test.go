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

func TestWindowTuplesGobRoundTrip(t *testing.T) {
	for _, tt := range []struct {
		name       string
		rangeValue *WindowRange
	}{
		{name: "nil"},
		{name: "nonempty", rangeValue: NewWindowRange(10, 20, 30)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var encoded bytes.Buffer
			window := &WindowTuples{
				Content:     []Row{&Tuple{Message: Message{"value": 1}}, &Tuple{Message: Message{"value": 2}}},
				WindowRange: tt.rangeValue,
			}
			if tt.rangeValue != nil {
				window.SetIsAgg(true)
			}
			var item any = window
			require.NoError(t, gob.NewEncoder(&encoded).Encode(&item))
			var got any
			require.NoError(t, gob.NewDecoder(&encoded).Decode(&got))
			restored := got.(*WindowTuples)
			require.Equal(t, tt.rangeValue, restored.WindowRange)
			require.Equal(t, 1, restored.Content[0].ToMap()["value"])
			if tt.rangeValue != nil {
				require.Len(t, restored.ToMaps(), 1)
			} else {
				require.Len(t, restored.ToMaps(), 2)
			}
		})
	}
}

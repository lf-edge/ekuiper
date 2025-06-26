// Copyright 2025 EMQ Technologies Co., Ltd.
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
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestEvent(t *testing.T) {
	var ev Event = &SliceTuple{
		Timestamp: time.UnixMilli(197777777),
	}
	ts := ev.GetTimestamp()
	require.Equal(t, time.UnixMilli(197777777), ts)
	isWatermark := ev.IsWatermark()
	require.Equal(t, false, isWatermark)

	fv, ok := ev.(*SliceTuple).FuncValue("event_time")
	require.True(t, ok)
	require.Equal(t, int64(197777777), fv)
	_, ok = ev.(*SliceTuple).FuncValue("et")
	require.False(t, ok)
}

func TestProps(t *testing.T) {
	origin := map[string]string{
		"foo": "bar",
		"baz": "qux",
	}
	var pp api.HasDynamicProps = &SliceTuple{
		Props: map[string]string{
			"foo": "bar",
			"baz": "qux",
		},
	}
	props := pp.AllProps()
	require.Equal(t, origin, props)
	r, ok := pp.DynamicProps("foo")
	require.True(t, ok)
	require.Equal(t, "bar", r)
	_, ok = pp.DynamicProps("foo1")
	require.False(t, ok)
}

func TestValIndex(t *testing.T) {
	tt := []struct {
		name        string
		sourceIndex int
		index       int
		result      any
	}{
		{
			"normal",
			2,
			0,
			"src2",
		},
		{
			"sink",
			-1,
			2,
			"snk2",
		},
		{
			"both",
			1,
			1,
			"src1",
		},
		{
			"wrong source",
			10,
			1,
			nil,
		},
		{
			"wrong sink",
			-1,
			10,
			nil,
		},
	}
	var ms model.IndexValuer = &SliceTuple{
		SourceContent: model.SliceVal{"src0", "src1", "src2"},
		SinkContent:   model.SliceVal{"snk0", "", "snk2"},
	}
	ms.SetByIndex(1, "snk1")
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := ms.ValueByIndex(tc.index, tc.sourceIndex)
			require.Equal(t, tc.result, r)
		})
	}
}

func TestTempIndex(t *testing.T) {
	tt := []struct {
		name   string
		index  int
		result any
	}{
		{
			"normal",
			2,
			2,
		},
		{
			"wrong index",
			10,
			nil,
		},
	}
	var ms model.IndexValuer = &SliceTuple{
		TempCalContent: model.SliceVal{0, 1, 2},
	}
	ms.SetByIndex(1, "snk1")
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			r := ms.TempByIndex(tc.index)
			require.Equal(t, tc.result, r)
		})
	}
}

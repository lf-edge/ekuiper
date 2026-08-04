// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

package node

import (
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/topotest/mockclock"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type legacyConvertWriter struct{}

func (*legacyConvertWriter) New(api.StreamContext) error             { return nil }
func (*legacyConvertWriter) Write(api.StreamContext, any) error      { return nil }
func (*legacyConvertWriter) Flush(api.StreamContext) ([]byte, error) { return nil, nil }

type passthroughConverter struct{}

func (*passthroughConverter) Encode(_ api.StreamContext, d any) ([]byte, error) {
	return d.([]byte), nil
}

func (*passthroughConverter) Decode(_ api.StreamContext, b []byte) (any, error) {
	return b, nil
}

func TestNewErr(t *testing.T) {
	ctx := mockContext.NewMockContext("testNewErr", "op1")
	_, err := NewBatchWriterOp(ctx, "op1", nil, nil, &SinkConf{Format: "nop"}, false)
	require.EqualError(t, err, "format type nop not supported")
}

func TestNewBatchWriterRawFallback(t *testing.T) {
	const format = "legacy_raw_test"
	oldConverter, hadConverter := modules.Converters[format]
	oldWriter, hadWriter := modules.ConvertWriters[format]
	modules.RegisterConverter(format, func(api.StreamContext, string, map[string]*ast.JsonStreamField, map[string]any) (message.Converter, error) {
		return &passthroughConverter{}, nil
	})
	modules.RegisterWriterConverter(format, func(api.StreamContext, string, map[string]*ast.JsonStreamField, map[string]any) (message.ConvertWriter, error) {
		return &legacyConvertWriter{}, nil
	})
	t.Cleanup(func() {
		if hadConverter {
			modules.Converters[format] = oldConverter
		} else {
			delete(modules.Converters, format)
		}
		if hadWriter {
			modules.ConvertWriters[format] = oldWriter
		} else {
			delete(modules.ConvertWriters, format)
		}
	})

	ctx := mockContext.NewMockContext("testRawFallback", "op1")
	rawOp, err := NewBatchWriterOp(ctx, "raw", &def.RuleOption{}, nil, &SinkConf{Format: format}, true)
	require.NoError(t, err)
	_, isLegacy := rawOp.writer.(*legacyConvertWriter)
	require.False(t, isLegacy)
	require.Implements(t, (*message.RawConvertWriter)(nil), rawOp.writer)

	commonOp, err := NewBatchWriterOp(ctx, "common", &def.RuleOption{}, nil, &SinkConf{Format: format}, false)
	require.NoError(t, err)
	require.IsType(t, &legacyConvertWriter{}, commonOp.writer)
}

func TestBatchWriterRun(t *testing.T) {
	testcases := []struct {
		name   string
		input  []any
		format string
		raw    bool
		err    string
		expect string
	}{
		{
			name:  "error type",
			input: []any{45}, // invalid input type
			err:   "unknown data type: int",
		},
		{
			name: "multiple single",
			input: []any{
				&xsql.Tuple{
					Emitter: "test",
					Message: map[string]any{
						"b": 12,
					},
				},
				&xsql.Tuple{
					Emitter: "test",
					Message: map[string]any{
						"a": "a",
						"b": 20,
						"c": "hello",
					},
				},
			},
			expect: "b\n12\n20",
		},
		{
			name: "multiple slice",
			input: []any{
				&xsql.SliceTuple{
					SourceContent: model.SliceVal{nil, 12},
				},
				&xsql.SliceTuple{
					SourceContent: model.SliceVal{"a", 20},
				},
			},
			expect: "\n,12\na,20",
		},
		{
			name: "multiple batch",
			input: []any{
				&xsql.TransformedTupleList{
					Maps: []map[string]any{
						{
							"a": "a",
							"b": 20,
							"c": "hello",
						},
						{
							"a": "a2",
						},
					},
				},
				&xsql.TransformedTupleList{
					Maps: []map[string]any{
						{
							"b": 12,
						},
						{
							"a": "a3",
							"b": 13,
						},
					},
				},
			},
			expect: "a,b,c\na,20,hello\na2,,\n12\n13",
		},
		{
			name: "raw tuple from JSON data template",
			input: []any{
				&xsql.RawTuple{Rawdata: []byte(`{"a":1}`)},
				&xsql.RawTuple{Rawdata: []byte(`{"a":2}`)},
			},
			format: "json",
			raw:    true,
			expect: `[{"a":1},{"a":2}]`,
		},
		{
			name: "raw tuple from delimited data template",
			input: []any{
				&xsql.RawTuple{Rawdata: []byte("1,test")},
				&xsql.RawTuple{Rawdata: []byte("2,demo")},
			},
			format: "delimited",
			raw:    true,
			expect: "1,test\n2,demo",
		},
		{
			name: "raw tuple from urlencoded data template",
			input: []any{
				&xsql.RawTuple{Rawdata: []byte("a=1")},
				&xsql.RawTuple{Rawdata: []byte("b=two+words")},
			},
			format: "urlencoded",
			raw:    true,
			expect: "a=1&b=two+words",
		},
	}
	mc := mockclock.GetMockClock()
	ctx := mockContext.NewMockContext("testNewErr", "op1")
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			format := tc.format
			if format == "" {
				format = "delimited"
			}
			// TODO sink schema does not work yet
			op, err := NewBatchWriterOp(ctx, "test", &def.RuleOption{BufferLength: 10, SendError: true}, map[string]*ast.JsonStreamField{
				"a": nil,
				"b": nil,
			}, &SinkConf{
				SendSingle: true,
				Format:     format,
				HasHeader:  true,
			}, tc.raw)
			require.NoError(t, err)
			out := make(chan any, 100)
			err = op.AddOutput(out, "test")
			require.NoError(t, err)
			errCh := make(chan error)
			op.Exec(ctx, errCh)
			for _, item := range tc.input {
				op.input <- item
				mc.Add(30 * time.Millisecond)
			}
			op.input <- xsql.BatchEOFTuple(time.Now())
			// wait for output
			result := <-out
			if tc.err != "" {
				e, ok := result.(error)
				if ok {
					assert.EqualError(t, e, tc.err)
				} else {
					assert.Fail(t, "expected error", tc.err)
				}
			} else {
				e, ok := result.(*xsql.RawTuple)
				assert.True(t, ok)
				assert.Equal(t, tc.expect, string(e.Raw()))
			}
		})
	}
}

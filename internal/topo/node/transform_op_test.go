package node

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/pkg/api"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"

	"github.com/lf-edge/ekuiper/v2/internal/xsql"
)

var commonCases = []any{
	&xsql.Tuple{Emitter: "test", Message: map[string]any{"a": 1, "b": 2}},                                       // common a,b
	&xsql.Tuple{Emitter: "test", Message: map[string]any{"a": 3, "b": 4, "c": "hello"}},                         // common a,b,c
	&xsql.Tuple{Emitter: "test", Message: map[string]any{"data": map[string]any{"a": 5, "b": 6, "c": "world"}}}, // nested data
	// &xsql.Tuple{Emitter: "test", Message: map[string]any{}},                                                     // empty tuple
	&xsql.WindowTuples{Content: []xsql.Row{&xsql.Tuple{Emitter: "test", Message: map[string]any{"a": 1, "b": 2}}, &xsql.Tuple{Emitter: "test", Message: map[string]any{"a": 3, "b": 4, "c": "hello"}}}},
	&xsql.WindowTuples{Content: []xsql.Row{&xsql.Tuple{Emitter: "test", Message: map[string]any{"data": map[string]any{"a": 5, "b": 6, "c": "world"}}}, &xsql.Tuple{Emitter: "test", Message: map[string]any{"a": 3, "b": 4, "c": "hello"}}}},
	&xsql.WindowTuples{Content: []xsql.Row{}}, // empty data should be omitted if omitempty is true
}

func TestTransformRun(t *testing.T) {
	testcases := []struct {
		name    string
		sc      *SinkConf
		cases   []any
		expects []any
	}{
		{
			name: "filed transform",
			sc: &SinkConf{
				Omitempty:    true,
				Fields:       []string{"a", "b"},
				DataField:    "data",
				Format:       "json",
				DataTemplate: "",
				SendSingle:   true,
			},
			cases: commonCases,
			expects: []any{
				errors.New("fail to TransItem data map[a:1 b:2] for error fail to decode data <nil> for error unsupported type <nil>"),
				errors.New("fail to TransItem data map[a:3 b:4 c:hello] for error fail to decode data <nil> for error unsupported type <nil>"),
				map[string]any{"a": 5, "b": 6},
				errors.New("fail to TransItem data map[a:1 b:2] for error fail to decode data <nil> for error unsupported type <nil>"),
				errors.New("fail to TransItem data map[a:3 b:4 c:hello] for error fail to decode data <nil> for error unsupported type <nil>"),
				map[string]any{"a": 5, "b": 6},
				errors.New("fail to TransItem data map[a:3 b:4 c:hello] for error fail to decode data <nil> for error unsupported type <nil>"),
			},
		},
		{
			name: "only fields without omit empty",
			sc: &SinkConf{
				Omitempty:    false,
				Fields:       []string{"a", "b"},
				Format:       "json",
				DataTemplate: "",
				SendSingle:   true,
			},
			cases: commonCases,
			expects: []any{
				map[string]any{"a": 1, "b": 2},
				map[string]any{"a": 3, "b": 4},
				map[string]any{"a": nil, "b": nil},

				map[string]any{"a": 1, "b": 2},
				map[string]any{"a": 3, "b": 4},

				map[string]any{"a": nil, "b": nil},
				map[string]any{"a": 3, "b": 4},

				// Even no omit empty, the empty data should be omitted due to sendSingle
				map[string]any{},
			},
		},
		{
			name: "allow empty",
			sc: &SinkConf{
				Omitempty:  false,
				Format:     "json",
				SendSingle: false,
			},
			expects: []any{
				[]map[string]any{{"a": 1, "b": 2}},
				[]map[string]any{{"a": 3, "b": 4, "c": "hello"}},
				[]map[string]any{{"a": 5, "b": 6}},
				[]map[string]any{{"a": 1, "b": 2}, {"a": 3, "b": 4, "c": "hello"}},
				[]map[string]any{{"data": map[string]any{"a": 5, "b": 6}}, {"a": 3, "b": 4, "c": "hello"}},
				[]map[string]any{},
			},
		},
		{
			name: "only data field",
			sc: &SinkConf{
				Omitempty:  false,
				DataField:  "data",
				Format:     "json",
				SendSingle: false,
			},
			cases: commonCases,
			expects: []any{
				nil,
				nil,
				map[string]any{"a": 5, "b": 6, "c": "world"},

				nil,

				map[string]any{"a": 5, "b": 6, "c": "world"},

				nil,
			},
		},
		{
			name: "data template with text format single",
			sc: &SinkConf{
				Omitempty:    true,
				Format:       "custom",
				DataTemplate: "{\"ab\":{{index . 0 \"a\"}}}",
				SendSingle:   false,
			},
			cases: commonCases,
			expects: []any{
				map[string]any{"ab": 1.0},
				map[string]any{"ab": 3.0},
				errors.New("fail to decode data {\"ab\":<no value>} after applying dataTemplate for error invalid character '<' looking for beginning of value"),

				map[string]any{"ab": 1.0},

				errors.New("fail to decode data {\"ab\":<no value>} after applying dataTemplate for error invalid character '<' looking for beginning of value"),
			},
		},
		{
			name: "data template collection",
			sc: &SinkConf{
				Omitempty:    true,
				Fields:       []string{"ab"},
				Format:       "json",
				DataTemplate: "{\"ab\":{{.a}},\"bb\":{{.b}}}",
				SendSingle:   true,
			},
			cases: commonCases,
			expects: []any{
				map[string]any{"ab": 1.0},
				map[string]any{"ab": 3.0},
				errors.New("fail to TransItem data map[data:map[a:5 b:6 c:world]] for error fail to decode data {\"ab\":<no value>,\"bb\":<no value>} for error invalid character '<' looking for beginning of value"),

				map[string]any{"ab": 1.0},
				map[string]any{"ab": 3.0},

				errors.New("fail to TransItem data map[data:map[a:5 b:6 c:world]] for error fail to decode data {\"ab\":<no value>,\"bb\":<no value>} for error invalid character '<' looking for beginning of value"),
				map[string]any{"ab": 3.0},
			},
		},
	}
	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			op, err := NewTransformOp("test", &api.RuleOption{BufferLength: 10, SendError: true}, tt.sc)
			assert.NoError(t, err)
			out := make(chan any, 100)
			err = op.AddOutput(out, "test")
			assert.NoError(t, err)
			ctx := mockContext.NewMockContext("test1", "transform_test")
			errCh := make(chan error)
			op.Exec(ctx, errCh)

			for i, c := range tt.cases {
				op.input <- c
				if i < len(tt.expects) {
					r := <-out
					assert.Equal(t, tt.expects[i], r, "case %d", i)
				}
			}
		})
	}
}

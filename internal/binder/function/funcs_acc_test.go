package function

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
)

func TestAccumulateAggCond(t *testing.T) {
	tests := []struct {
		name     string
		results  []interface{}
		testargs [][]interface{}
	}{
		{
			name: "acc_avg",
			testargs: [][]interface{}{
				{int64(1), false, false},
				{int64(1), true, false},
				{int64(1), false, false},
				{int64(1), false, true},
				{int64(1), false, false},
			},
			results: []interface{}{
				float64(0), float64(1), float64(1), float64(1), float64(0),
			},
		},
		{
			name: "acc_min",
			testargs: [][]interface{}{
				{int64(1), false, false},
				{int64(5), true, false},
				{int64(4), false, false},
				{int64(3), false, true},
				{int64(2), false, false},
			},
			results: []interface{}{
				float64(0), float64(5), float64(4), float64(3), float64(0),
			},
		},
		{
			name: "acc_sum",
			testargs: [][]interface{}{
				{int64(1), false, false},
				{int64(1), true, false},
				{int64(1), false, false},
				{int64(1), false, true},
				{int64(1), false, false},
			},
			results: []interface{}{
				float64(0), float64(1), float64(2), float64(3), float64(0),
			},
		},
		{
			name: "acc_count",
			testargs: [][]interface{}{
				{1, false, false},
				{1, true, false},
				{1, false, false},
				{1, false, true},
				{1, false, false},
			},
			results: []interface{}{
				int64(0), int64(1), int64(2), int64(3), int64(0),
			},
		},
	}
	for _, test := range tests {
		f, ok := builtins[test.name]
		require.True(t, ok)
		contextLogger := conf.Log.WithField("rule", "testExec")
		ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
		tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
		fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
		for i, arg := range test.testargs {
			newArg := append(arg, true, fmt.Sprintf("%s_key", test.name))
			result, _ := f.exec(fctx, newArg)
			require.Equal(t, test.results[i], result)
		}
	}
}

func TestAccumulateAgg(t *testing.T) {
	tests := []struct {
		name     string
		results  []interface{}
		testargs []interface{}
	}{
		{
			name: "acc_count",
			testargs: []interface{}{
				"1",
				float64(1),
				float32(1),
				1,
				int32(1),
				int64(1),
			},
			results: []interface{}{
				int64(1), int64(2), int64(3), int64(4), int64(5), int64(6),
			},
		},
		{
			name: "acc_avg",
			testargs: []interface{}{
				"1",
				float64(1),
				float64(1),
				float64(1),
				int64(1),
				int64(1),
			},
			results: []interface{}{
				fmt.Errorf("the value should be number"),
				float64(1),
				float64(1),
				float64(1),
				float64(1),
				float64(1),
			},
		},
		{
			name: "acc_max",
			testargs: []interface{}{
				"1",
				float64(1),
				float64(2),
				float64(3),
				int64(4),
				int64(5),
			},
			results: []interface{}{
				fmt.Errorf("the value should be number"),
				float64(1),
				float64(2),
				float64(3),
				float64(4),
				float64(5),
			},
		},
		{
			name: "acc_min",
			testargs: []interface{}{
				"1",
				float64(5),
				float64(4),
				float64(3),
				int64(2),
				int64(1),
			},
			results: []interface{}{
				fmt.Errorf("the value should be number"),
				float64(5),
				float64(4),
				float64(3),
				float64(2),
				float64(1),
			},
		},
		{
			name: "acc_sum",
			testargs: []interface{}{
				"1",
				float64(1),
				float64(1),
				int64(1),
				int64(1),
				int64(1),
			},
			results: []interface{}{
				fmt.Errorf("the value should be number"),
				float64(1),
				float64(2),
				float64(3),
				float64(4),
				float64(5),
			},
		},
	}
	for _, test := range tests {
		f, ok := builtins[test.name]
		require.True(t, ok)
		contextLogger := conf.Log.WithField("rule", "testExec")
		ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
		tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
		fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
		for i, arg := range test.testargs {
			result, _ := f.exec(fctx, []interface{}{arg, true, fmt.Sprintf("%s_key", test.name)})
			require.Equal(t, test.results[i], result)
		}
	}

	tests2 := []struct {
		name   string
		result interface{}
	}{
		{
			"acc_sum",
			float64(0),
		},
		{
			"acc_max",
			float64(0),
		},
		{
			"acc_min",
			float64(0),
		},
		{
			"acc_avg",
			float64(0),
		},
		{
			"acc_count",
			int64(0),
		},
	}
	for _, test := range tests2 {
		f, ok := builtins[test.name]
		require.True(t, ok)
		contextLogger := conf.Log.WithField("rule", "testExec")
		ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
		tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
		fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)
		result, b := f.exec(fctx, []interface{}{1, false, fmt.Sprintf("%s_key", test.name)})
		require.True(t, b)
		require.Equal(t, test.result, result)
	}
}

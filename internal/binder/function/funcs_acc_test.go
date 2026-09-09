package function

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/topo/checkpoint"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestAccAvgCheckpointRoundTrip(t *testing.T) {
	live := &accStatus{
		Value:    &accAvgStatus{Sum: 12, Count: 3, Avg: 4},
		HasBegin: true,
	}
	frozen, err := checkpoint.EncodeState(map[string]interface{}{"acc_avg": live})
	require.NoError(t, err)

	live.Value.(*accAvgStatus).Sum = 100
	live.Value.(*accAvgStatus).Avg = 100

	restored, err := checkpoint.DecodeState(frozen)
	require.NoError(t, err)
	status := restored["acc_avg"].(*accStatus)
	require.True(t, status.HasBegin)
	require.Equal(t, &accAvgStatus{Sum: 12, Count: 3, Avg: 4}, status.Value)
}

func TestAccCheckpointOmitsTransientError(t *testing.T) {
	live := &accStatus{
		Value:    int64(3),
		HasBegin: true,
		err:      errors.New("transient evaluation error"),
	}
	frozen, err := checkpoint.EncodeState(map[string]interface{}{"acc_count": live})
	require.NoError(t, err)

	restored, err := checkpoint.DecodeState(frozen)
	require.NoError(t, err)
	status := restored["acc_count"].(*accStatus)
	require.NoError(t, status.err)
	require.Equal(t, int64(3), status.Value)
	require.True(t, status.HasBegin)
}

func TestAccByAndMapCheckpointRoundTrip(t *testing.T) {
	tests := map[string]any{
		"acc_max_by": &accMaxByStatus{Value: "max", By: 12},
		"acc_min_by": &accMinByStatus{Value: "min", By: 3},
		"acc_map_agg": &accMapAggStatus{
			Entries: []accMapEntry{{Key: "key", Value: int64(42)}},
			Index:   map[string]int{"key": 0},
		},
	}
	for name, value := range tests {
		t.Run(name, func(t *testing.T) {
			frozen, err := checkpoint.EncodeState(map[string]interface{}{name: &accStatus{Value: value}})
			require.NoError(t, err)

			restored, err := checkpoint.DecodeState(frozen)
			require.NoError(t, err)
			require.Equal(t, value, restored[name].(*accStatus).Value)
		})
	}
}

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
		{
			name: "acc_collect",
			testargs: [][]interface{}{
				{int64(1), false, false},
				{int64(1), true, false},
				{int64(2), false, false},
				{int64(3), false, true},
				{int64(4), false, false},
			},
			results: []interface{}{
				[]interface{}{},
				[]interface{}{int64(1)},
				[]interface{}{int64(1), int64(2)},
				[]interface{}{int64(1), int64(2), int64(3)},
				[]interface{}{},
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
		{
			name: "acc_collect",
			testargs: []interface{}{
				int64(1),
				int64(2),
				nil,
				"hello",
				float64(3.14),
			},
			results: []interface{}{
				[]interface{}{int64(1)},
				[]interface{}{int64(1), int64(2)},
				[]interface{}{int64(1), int64(2)},
				[]interface{}{int64(1), int64(2), "hello"},
				[]interface{}{int64(1), int64(2), "hello", float64(3.14)},
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
		{
			"acc_collect",
			[]interface{}{},
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

func TestAccMaxBy(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testAccMaxBy")
	tctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("acc_max_by_test", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(tctx.WithMeta("mockRule0", "test", tempStore), 0)
	f := builtins["acc_max_by"]

	args := func(value, by int64) []interface{} { return []interface{}{value, by, true, "max_by"} }
	result, ok := f.exec(fctx, args(100, 10))
	require.True(t, ok)
	require.Equal(t, int64(100), result)
	result, ok = f.exec(fctx, args(200, 9))
	require.True(t, ok)
	require.Equal(t, int64(100), result)
	result, ok = f.exec(fctx, args(300, 10))
	require.True(t, ok)
	require.Equal(t, int64(300), result)
}

func TestAccMinBy(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testAccMinBy")
	tctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("acc_min_by_test", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(tctx.WithMeta("mockRule0", "test", tempStore), 0)
	f := builtins["acc_min_by"]

	args := func(value, by int64) []interface{} { return []interface{}{value, by, true, "min_by"} }
	result, ok := f.exec(fctx, args(100, 10))
	require.True(t, ok)
	require.Equal(t, int64(100), result)
	result, ok = f.exec(fctx, args(200, 11))
	require.True(t, ok)
	require.Equal(t, int64(100), result)
	result, ok = f.exec(fctx, args(300, 10))
	require.True(t, ok)
	require.Equal(t, int64(300), result)
}

func TestAccMinByEdgeCases(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testAccMinByEdgeCases")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("acc_min_by_edge_test", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 0)
	f := builtins["acc_min_by"]

	result, ok := f.exec(fctx, []interface{}{int64(1), nil, true, "nil_by"})
	require.True(t, ok)
	require.Nil(t, result)
	result, ok = f.exec(fctx, []interface{}{int64(1), math.NaN(), true, "nan_by"})
	require.True(t, ok)
	require.Nil(t, result)
	result, ok = f.exec(fctx, []interface{}{int64(2), int64(10), true, "nan_by"})
	require.True(t, ok)
	require.Equal(t, int64(2), result)
	result, ok = f.exec(fctx, []interface{}{int64(3), math.NaN(), true, "nan_by"})
	require.True(t, ok)
	require.Equal(t, int64(2), result)

	args := func(value, by int64, valid, begin, reset bool) []interface{} {
		return []interface{}{value, by, begin, reset, valid, "conditional_min_by"}
	}
	result, ok = f.exec(fctx, args(1, 10, true, false, false))
	require.True(t, ok)
	require.Nil(t, result)
	result, ok = f.exec(fctx, args(2, 20, true, true, false))
	require.True(t, ok)
	require.Equal(t, int64(2), result)
	result, ok = f.exec(fctx, args(3, 15, true, false, false))
	require.True(t, ok)
	require.Equal(t, int64(3), result)
	result, ok = f.exec(fctx, args(4, 20, true, false, true))
	require.True(t, ok)
	require.Equal(t, int64(3), result)
	result, ok = f.exec(fctx, args(5, 1, true, false, false))
	require.True(t, ok)
	require.Nil(t, result)
}

func TestAccMinByValidationAndState(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testAccMinByValidationAndState")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("acc_min_by_validation_test", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 0)
	f := builtins["acc_min_by"]

	// The validator accepts the normal and conditional forms only.
	require.NoError(t, f.val(fctx, []ast.Expr{nil, nil}))
	require.NoError(t, f.val(fctx, []ast.Expr{nil, nil, nil, nil}))
	require.Error(t, f.val(fctx, []ast.Expr{nil}))
	require.Error(t, f.val(fctx, []ast.Expr{nil, nil, nil}))
	require.Error(t, f.val(fctx, []ast.Expr{nil, nil, nil, nil, nil}))

	result, ok := f.exec(fctx, []interface{}{int64(1), int64(1)})
	require.False(t, ok)
	_, isErr := result.(error)
	require.True(t, isErr)
	result, ok = f.exec(fctx, []interface{}{int64(1), int64(1), "bad", true, true, "bad_conditions"})
	require.False(t, ok)
	_, isErr = result.(error)
	require.True(t, isErr)
	result, ok = f.exec(fctx, []interface{}{int64(1), int64(1), true, 123})
	require.False(t, ok)
	_, isErr = result.(error)
	require.True(t, isErr)

	// Invalid data does not replace the current result.
	result, ok = f.exec(fctx, []interface{}{int64(1), float64(2.5), true, "validity"})
	require.True(t, ok)
	require.Equal(t, int64(1), result)
	result, ok = f.exec(fctx, []interface{}{int64(2), float64(1.5), false, "validity"})
	require.True(t, ok)
	require.Equal(t, int64(1), result)
	result, ok = f.exec(fctx, []interface{}{int64(3), "invalid", true, "invalid_value"})
	require.False(t, ok)
	_, isErr = result.(error)
	require.True(t, isErr)

	// Recover from an incompatible or malformed accumulator state.
	require.NoError(t, fctx.PutState("wrong_type", &accStatus{Value: int64(1)}))
	result, ok = f.exec(fctx, []interface{}{int64(4), int64(4), true, "wrong_type"})
	require.True(t, ok)
	require.Equal(t, int64(4), result)
	require.NoError(t, fctx.PutState("nan_state", &accStatus{Value: &accMinByStatus{Value: int64(5), By: math.NaN()}}))
	result, ok = f.exec(fctx, []interface{}{int64(6), int64(6), true, "nan_state"})
	require.True(t, ok)
	require.Equal(t, int64(6), result)
}

func TestAccMapAgg(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testAccMapAgg")
	tctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("acc_map_agg_test", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(tctx.WithMeta("mockRule0", "test", tempStore), 0)
	f := builtins["acc_map_agg"]

	args := func(key int64, value interface{}) []interface{} {
		return []interface{}{key, value, true, "map_agg"}
	}
	result, ok := f.exec(fctx, args(18, map[string]interface{}{"max_temp": int64(28)}))
	require.True(t, ok)
	require.Equal(t, []map[string]interface{}{{"key": "18", "value": map[string]interface{}{"max_temp": int64(28)}}}, result)
	_, ok = f.exec(fctx, args(19, map[string]interface{}{"max_temp": int64(31)}))
	require.True(t, ok)
	result, ok = f.exec(fctx, args(18, map[string]interface{}{"max_temp": int64(30)}))
	require.True(t, ok)
	require.Equal(t, []map[string]interface{}{
		{"key": "18", "value": map[string]interface{}{"max_temp": int64(30)}},
		{"key": "19", "value": map[string]interface{}{"max_temp": int64(31)}},
	}, result)

	// Reinitialize an incompatible accumulator value instead of panicking.
	err := fctx.PutState("wrong_map_type", &accStatus{Value: int64(1)})
	require.NoError(t, err)
	result, ok = f.exec(fctx, []interface{}{20, map[string]interface{}{"max_temp": int64(33)}, true, "wrong_map_type"})
	require.True(t, ok)
	require.Equal(t, []map[string]interface{}{{"key": "20", "value": map[string]interface{}{"max_temp": int64(33)}}}, result)
}

func TestAccMaxByEdgeCases(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testAccMaxByEdgeCases")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("acc_max_by_edge_test", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 0)
	f := builtins["acc_max_by"]

	result, ok := f.exec(fctx, []interface{}{int64(1), nil, true, "nil_by"})
	require.True(t, ok)
	require.Nil(t, result)
	result, ok = f.exec(fctx, []interface{}{int64(1), "invalid", true, "invalid_by"})
	require.False(t, ok)
	_, isErr := result.(error)
	require.True(t, isErr)
	result, ok = f.exec(fctx, []interface{}{int64(1), int64(1), false, "invalid_data"})
	require.True(t, ok)
	require.Nil(t, result)
	result, ok = f.exec(fctx, []interface{}{int64(1), math.NaN(), true, "nan_by"})
	require.True(t, ok)
	require.Nil(t, result)
	result, ok = f.exec(fctx, []interface{}{int64(2), int64(10), true, "nan_by"})
	require.True(t, ok)
	require.Equal(t, int64(2), result)
	result, ok = f.exec(fctx, []interface{}{int64(3), math.NaN(), true, "nan_by"})
	require.True(t, ok)
	require.Equal(t, int64(2), result)

	// Conditional accumulation starts only after begin and resets after reset.
	args := func(value, by int64, valid, begin, reset bool) []interface{} {
		return []interface{}{value, by, begin, reset, valid, "conditional_max_by"}
	}
	result, ok = f.exec(fctx, args(1, 10, true, false, false))
	require.True(t, ok)
	require.Nil(t, result)
	result, ok = f.exec(fctx, args(2, 20, true, true, false))
	require.True(t, ok)
	require.Equal(t, int64(2), result)
	result, ok = f.exec(fctx, args(3, 15, true, false, false))
	require.True(t, ok)
	require.Equal(t, int64(2), result)
	result, ok = f.exec(fctx, args(4, 20, true, false, true))
	require.True(t, ok)
	require.Equal(t, int64(4), result)
	result, ok = f.exec(fctx, args(5, 1, true, false, false))
	require.True(t, ok)
	require.Nil(t, result)
}

func TestAccMapAggEdgeCases(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testAccMapAggEdgeCases")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("acc_map_agg_edge_test", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 0)
	f := builtins["acc_map_agg"]

	result, ok := f.exec(fctx, []interface{}{nil, "ignored", true, "nil_key"})
	require.True(t, ok)
	require.Equal(t, []map[string]interface{}{}, result)
	result, ok = f.exec(fctx, []interface{}{true, "bool-key", true, "converted_key"})
	require.True(t, ok)
	require.Equal(t, []map[string]interface{}{{"key": "true", "value": "bool-key"}}, result)
	result, ok = f.exec(fctx, []interface{}{"a", 1, false, "invalid_data_map"})
	require.True(t, ok)
	require.Equal(t, []map[string]interface{}{}, result)

	// Rebuild the index when loading a state created without the side index.
	err := fctx.PutState("rehydrate_map", &accStatus{Value: &accMapAggStatus{
		Entries: []accMapEntry{{Key: "a", Value: 1}},
	}})
	require.NoError(t, err)
	result, ok = f.exec(fctx, []interface{}{"a", 2, true, "rehydrate_map"})
	require.True(t, ok)
	require.Equal(t, []map[string]interface{}{{"key": "a", "value": 2}}, result)

	// Conditional accumulation starts only after begin and resets after reset.
	args := func(key string, value int64, valid, begin, reset bool) []interface{} {
		return []interface{}{key, value, begin, reset, valid, "conditional_map_agg"}
	}
	result, ok = f.exec(fctx, args("a", 1, true, false, false))
	require.True(t, ok)
	require.Equal(t, []map[string]interface{}{}, result)
	result, ok = f.exec(fctx, args("a", 2, true, true, false))
	require.True(t, ok)
	require.Equal(t, []map[string]interface{}{{"key": "a", "value": int64(2)}}, result)
	result, ok = f.exec(fctx, args("b", 3, true, false, true))
	require.True(t, ok)
	require.Equal(t, []map[string]interface{}{
		{"key": "a", "value": int64(2)},
		{"key": "b", "value": int64(3)},
	}, result)
	result, ok = f.exec(fctx, args("c", 4, true, false, false))
	require.True(t, ok)
	require.Equal(t, []map[string]interface{}{}, result)
}

func TestAccCollectFuncDirect(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)

	cf := accCollectFunc{}

	// Test accReset
	t.Run("accReset", func(t *testing.T) {
		s := &accStatus{Value: int64(42)}
		cf.accReset(s)
		require.Equal(t, []interface{}{}, s.Value)
	})

	// Test accFuncExec with nil Value (defense-in-depth guard)
	t.Run("accFuncExec_nilValue", func(t *testing.T) {
		s := &accStatus{Value: nil}
		cf.accFuncExec(fctx, int64(1), true, "k1", s, true)
		require.Equal(t, []interface{}{int64(1)}, s.Value)
		require.Nil(t, s.err)
	})

	// Test accFuncExec with nil value arg (should skip)
	t.Run("accFuncExec_nilArg", func(t *testing.T) {
		s := &accStatus{Value: []interface{}{int64(1)}}
		cf.accFuncExec(fctx, nil, true, "k2", s, true)
		require.Equal(t, []interface{}{int64(1)}, s.Value)
	})

	// Test accFuncExec with validData=false and skipStatusSave=true
	t.Run("accFuncExec_invalidData_skipSave", func(t *testing.T) {
		s := &accStatus{Value: []interface{}{int64(1)}}
		cf.accFuncExec(fctx, int64(2), false, "k3", s, true)
		require.Equal(t, []interface{}{int64(1)}, s.Value)
	})

	// Test accFuncExec with skipStatusSave=false (PutState path)
	t.Run("accFuncExec_withSave", func(t *testing.T) {
		s := &accStatus{Value: []interface{}{int64(1)}}
		cf.accFuncExec(fctx, int64(2), true, "k4", s, false)
		require.Equal(t, []interface{}{int64(1), int64(2)}, s.Value)
		require.Nil(t, s.err)
	})
}

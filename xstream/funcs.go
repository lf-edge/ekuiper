package xstream

import (
	"context"
	"engine/xstream/api"
	"engine/xstream/operators"
	"fmt"
	"reflect"
)

type unaryFuncForm byte

const (
	unaryFuncUnsupported unaryFuncForm = iota
	unaryFuncForm1
	unaryFuncForm2
)

// ProcessFunc returns a unary function which applies the specified
// user-defined function that processes data items from upstream and
// returns a result value. The provided function must be of type:
//   func(T) R
//   where T is the type of incoming item
//   R the type of returned processed item
func ProcessFunc(f interface{}) (operators.UnFunc, error) {
	fntype := reflect.TypeOf(f)

	funcForm, err := isUnaryFuncForm(fntype)
	if err != nil {
		return nil, err
	}
	if funcForm == unaryFuncUnsupported {
		return nil, fmt.Errorf("unsupported unary func type")
	}

	fnval := reflect.ValueOf(f)

	return operators.UnFunc(func(ctx api.StreamContext, data interface{}) interface{} {
		result := callOpFunc(fnval, ctx, data, funcForm)
		return result.Interface()
	}), nil
}

// FilterFunc returns a unary function (api.UnFunc) which applies the user-defined
// filtering to apply predicates that filters out data items from being included
// in the downstream.  The provided user-defined function must be of type:
//   func(T)bool - where T is the type of incoming data item, bool is the value of the predicate
// When the user-defined function returns false, the current processed data item will not
// be placed in the downstream processing.
func FilterFunc(f interface{}) (operators.UnFunc, error) {
	fntype := reflect.TypeOf(f)

	funcForm, err := isUnaryFuncForm(fntype)
	if err != nil {
		return nil, err
	}
	if funcForm == unaryFuncUnsupported {
		return nil, fmt.Errorf("unsupported unary func type")
	}

	// ensure bool ret type
	if fntype.Out(0).Kind() != reflect.Bool {
		return nil, fmt.Errorf("unary filter func must return bool")
	}

	fnval := reflect.ValueOf(f)
	return operators.UnFunc(func(ctx api.StreamContext, data interface{}) interface{} {
		result := callOpFunc(fnval, ctx, data, funcForm)
		predicate := result.Bool()
		if !predicate {
			return nil
		}
		return data
	}), nil
}

// MapFunc returns an unary function which applies the user-defined function which
// maps, one-to-one, the incomfing value to a new value.  The user-defined function
// must be of type:
//   func(T) R - where T is the incoming item, R is the type of the returned mapped item
func MapFunc(f interface{}) (operators.UnFunc, error) {
	return ProcessFunc(f)
}

// FlatMapFunc returns an unary function which applies a user-defined function which
// takes incoming comsite items and deconstruct them into individual items which can
// then be re-streamed.  The type for the user-defined function is:
//   func (T) R - where R is the original item, R is a slice of decostructed items
// The slice returned should be restreamed by placing each item onto the stream for
// downstream processing.
func FlatMapFunc(f interface{}) (operators.UnFunc, error) {
	fntype := reflect.TypeOf(f)

	funcForm, err := isUnaryFuncForm(fntype)
	if err != nil {
		return nil, err
	}
	if funcForm == unaryFuncUnsupported {
		return nil, fmt.Errorf("unsupported unary func type")
	}

	if fntype.Out(0).Kind() != reflect.Slice {
		return nil, fmt.Errorf("unary FlatMap func must return slice")
	}

	fnval := reflect.ValueOf(f)
	return operators.UnFunc(func(ctx api.StreamContext, data interface{}) interface{} {
		result := callOpFunc(fnval, ctx, data, funcForm)
		return result.Interface()
	}), nil
}

// isUnaryFuncForm ensures ftype is of supported function of
// form func(in) out or func(context, in) out
func isUnaryFuncForm(ftype reflect.Type) (unaryFuncForm, error) {
	if ftype.NumOut() != 1 {
		return unaryFuncUnsupported, fmt.Errorf("unary func must return one param")
	}

	switch ftype.Kind() {
	case reflect.Func:
		switch ftype.NumIn() {
		case 1:
			// f(in)out, ok
			return unaryFuncForm1, nil
		case 2:
			// func(context,in)out
			param0 := ftype.In(0)
			if param0.Kind() != reflect.Interface {
				return unaryFuncUnsupported, fmt.Errorf("unary must be type func(T)R or func(context.Context, T)R")
			}
			return unaryFuncForm2, nil
		}
	}
	return unaryFuncUnsupported, fmt.Errorf("unary func must be of type func(T)R or func(context.Context,T)R")
}

func callOpFunc(fnval reflect.Value, ctx context.Context, data interface{}, funcForm unaryFuncForm) reflect.Value {
	var result reflect.Value
	switch funcForm {
	case unaryFuncForm1:
		arg0 := reflect.ValueOf(data)
		result = fnval.Call([]reflect.Value{arg0})[0]
	case unaryFuncForm2:
		arg0 := reflect.ValueOf(ctx)
		arg1 := reflect.ValueOf(data)
		if !arg0.IsValid() {
			arg0 = reflect.ValueOf(context.Background())
		}
		result = fnval.Call([]reflect.Value{arg0, arg1})[0]
	}
	return result
}

func isArgContext(val reflect.Value) bool {
	_, ok := val.Interface().(context.Context)
	return ok
}

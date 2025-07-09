package api

import (
	"fmt"
	"time"
)

type DatumType int

const (
	UnknownVal DatumType = iota
	I64Val
	F64Val
	BoolVal
	StringVal
	SliceVal
	MapVal
	DurationVal
)

type Datum struct {
	Kind        DatumType
	I64Val      int64
	F64Val      float64
	BoolVal     bool
	StrVal      string
	SliceVal    []*Datum
	MapVal      map[string]*Datum
	DurationVal time.Duration
}

func (d *Datum) DurVal() (time.Duration, error) {
	switch d.Kind {
	case DurationVal:
		return d.DurationVal, nil
	default:
		return 0, fmt.Errorf("datum kind %v is not duration", d.Kind)
	}
}

func (d *Datum) GetI64Val() (int64, error) {
	switch d.Kind {
	case I64Val:
		return d.I64Val, nil
	default:
		return 0, fmt.Errorf("datum kind %v is not int", d.Kind)
	}
}

func (d *Datum) ToF64Val() (float64, error) {
	switch d.Kind {
	case I64Val:
		return float64(d.I64Val), nil
	case F64Val:
		return d.F64Val, nil
	default:
		return 0, fmt.Errorf("datum kind %v is not int", d.Kind)
	}
}

func NewI64Datum(v int64) *Datum {
	return &Datum{Kind: I64Val, I64Val: v}
}

func NewF64Datum(v float64) *Datum {
	return &Datum{Kind: F64Val, F64Val: v}
}

func NewDurDatum(v time.Duration) *Datum {
	return &Datum{Kind: DurationVal, DurationVal: v}
}

func interfaceToDatum(val interface{}) (*Datum, error) {
	var err error
	switch v := val.(type) {
	case nil:
		return nil, nil
	case int64:
		return &Datum{Kind: I64Val, I64Val: v}, nil
	case float64:
		return &Datum{Kind: F64Val, F64Val: v}, nil
	case bool:
		return &Datum{Kind: BoolVal, BoolVal: v}, nil
	case string:
		dt, err := time.ParseDuration(v)
		if err == nil {
			return &Datum{Kind: DurationVal, DurationVal: dt}, nil
		}
		return &Datum{Kind: StringVal, StrVal: v}, nil
	case []interface{}:
		slice := make([]*Datum, len(v))
		for i, item := range v {
			slice[i], err = interfaceToDatum(item)
			if err != nil {
				return &Datum{}, err
			}
		}
		return &Datum{Kind: SliceVal, SliceVal: slice}, nil
	case map[string]interface{}:
		m := make(map[string]*Datum)
		for k, item := range v {
			m[k], err = interfaceToDatum(item)
			if err != nil {
				return nil, err
			}
		}
		return &Datum{Kind: MapVal, MapVal: m}, nil
	default:
		return nil, fmt.Errorf("unknown type")
	}
}

func datumToInterface(d *Datum) interface{} {
	if d == nil {
		return nil
	}
	switch d.Kind {
	case UnknownVal:
		return nil
	case I64Val:
		return d.I64Val
	case F64Val:
		return d.F64Val
	case BoolVal:
		return d.BoolVal
	case StringVal:
		return d.StrVal
	case SliceVal:
		slice := make([]interface{}, len(d.SliceVal))
		for i, item := range d.SliceVal {
			slice[i] = datumToInterface(item)
		}
		return slice
	case MapVal:
		m := make(map[string]interface{})
		for k, v := range d.MapVal {
			m[k] = datumToInterface(v)
		}
		return m
	case DurationVal:
		return d.DurationVal.String()
	default:
		return nil
	}
}

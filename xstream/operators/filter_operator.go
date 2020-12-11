package operators

import (
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
)

type FilterOp struct {
	Condition xsql.Expr
}

/**
 *  input: *xsql.Tuple from preprocessor | xsql.WindowTuplesSet from windowOp | xsql.JoinTupleSets from joinOp
 *  output: *xsql.Tuple | xsql.WindowTuplesSet | xsql.JoinTupleSets
 */
func (p *FilterOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) interface{} {
	log := ctx.GetLogger()
	log.Debugf("filter plan receive %s", data)
	switch input := data.(type) {
	case error:
		return input
	case xsql.Valuer:
		ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(input, fv)}
		result := ve.Eval(p.Condition)
		switch r := result.(type) {
		case error:
			return fmt.Errorf("run Where error: %s", r)
		case bool:
			if r {
				return input
			}
		default:
			return fmt.Errorf("run Where error: invalid condition that returns non-bool value %[1]T(%[1]v)", r)
		}
	case xsql.WindowTuplesSet:
		if len(input) != 1 {
			return fmt.Errorf("run Where error: the input WindowTuplesSet with multiple tuples cannot be evaluated")
		}
		ms := input[0].Tuples
		r := ms[:0]
		for _, v := range ms {
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(&v, fv)}
			result := ve.Eval(p.Condition)
			switch val := result.(type) {
			case error:
				return fmt.Errorf("run Where error: %s", val)
			case bool:
				if val {
					r = append(r, v)
				}
			default:
				return fmt.Errorf("run Where error: invalid condition that returns non-bool value %[1]T(%[1]v)", val)
			}
		}
		if len(r) > 0 {
			input[0].Tuples = r
			return input
		}
	case xsql.JoinTupleSets:
		ms := input
		r := ms[:0]
		for _, v := range ms {
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(&v, fv)}
			result := ve.Eval(p.Condition)
			switch val := result.(type) {
			case error:
				return fmt.Errorf("run Where error: %s", val)
			case bool:
				if val {
					r = append(r, v)
				}
			default:
				return fmt.Errorf("run Where error: invalid condition that returns non-bool value %[1]T(%[1]v)", val)
			}
		}
		if len(r) > 0 {
			return r
		}
	default:
		return fmt.Errorf("run Where error: invalid input %[1]T(%[1]v)", input)
	}
	return nil
}

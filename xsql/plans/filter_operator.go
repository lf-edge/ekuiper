package plans

import (
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
)

type FilterPlan struct {
	Condition xsql.Expr
}

/**
 *  input: *xsql.Tuple from preprocessor | xsql.WindowTuplesSet from windowOp | xsql.JoinTupleSets from joinOp
 *  output: *xsql.Tuple | xsql.WindowTuplesSet | xsql.JoinTupleSets
 */
func (p *FilterPlan) Apply(ctx api.StreamContext, data interface{}) interface{} {
	log := ctx.GetLogger()
	log.Debugf("filter plan receive %s", data)
	switch input := data.(type) {
	case error:
		return input
	case xsql.Valuer:
		ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(input, &xsql.FunctionValuer{})}
		result := ve.Eval(p.Condition)
		switch r := result.(type) {
		case error:
			return r
		case bool:
			if r {
				return input
			}
		default:
			return fmt.Errorf("invalid condition that returns non-bool value")
		}
	case xsql.WindowTuplesSet:
		if len(input) != 1 {
			return fmt.Errorf("WindowTuplesSet with multiple tuples cannot be evaluated")
		}
		ms := input[0].Tuples
		r := ms[:0]
		for _, v := range ms {
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(&v, &xsql.FunctionValuer{})}
			result := ve.Eval(p.Condition)
			switch val := result.(type) {
			case error:
				return val
			case bool:
				if val {
					r = append(r, v)
				}
			default:
				return fmt.Errorf("invalid condition that returns non-bool value")
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
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(&v, &xsql.FunctionValuer{})}
			result := ve.Eval(p.Condition)
			switch val := result.(type) {
			case error:
				return val
			case bool:
				if val {
					r = append(r, v)
				}
			default:
				return fmt.Errorf("invalid condition that returns non-bool value")
			}
		}
		if len(r) > 0 {
			return r
		}
	default:
		return fmt.Errorf("Expect xsql.Valuer or its array type.")
	}
	return nil
}

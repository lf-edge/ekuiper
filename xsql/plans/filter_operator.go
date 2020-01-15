package plans

import (
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
	case xsql.Valuer:
		ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(input, &xsql.FunctionValuer{})}
		result, ok := ve.Eval(p.Condition).(bool)
		if ok {
			if result {
				return input
			}
		} else {
			log.Errorf("invalid condition that returns non-bool value")
		}
	case xsql.WindowTuplesSet:
		if len(input) != 1 {
			log.Infof("WindowTuplesSet with multiple tuples cannot be evaluated")
			return nil
		}
		ms := input[0].Tuples
		r := ms[:0]
		for _, v := range ms {
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(&v, &xsql.FunctionValuer{})}
			result, ok := ve.Eval(p.Condition).(bool)
			if ok {
				if result {
					r = append(r, v)
				}
			} else {
				log.Errorf("invalid condition that returns non-bool value")
				return nil
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
			result, ok := ve.Eval(p.Condition).(bool)
			if ok {
				if result {
					r = append(r, v)
				}
			} else {
				log.Errorf("invalid condition that returns non-bool value")
				return nil
			}
		}
		if len(r) > 0 {
			return r
		}
	default:
		log.Errorf("Expect xsql.Valuer or its array type.")
		return nil
	}
	return nil
}

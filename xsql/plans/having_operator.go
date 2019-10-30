package plans

import (
	"context"
	"engine/common"
	"engine/xsql"
)

type HavingPlan struct {
	Condition xsql.Expr
}

func (p *HavingPlan) Apply(ctx context.Context, data interface{}) interface{} {
	log := common.GetLogger(ctx)
	log.Debugf("having plan receive %s", data)
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
			//ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(&v, &xsql.FunctionValuer{})}
			ve := &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(input, &v, &xsql.FunctionValuer{}, &xsql.AggregateFunctionValuer{Data: input}, &xsql.WildcardValuer{Data: &v})}
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
			//ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(&v, &xsql.FunctionValuer{})}
			ve := &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(input, &v, &xsql.FunctionValuer{}, &xsql.AggregateFunctionValuer{Data: input}, &xsql.WildcardValuer{Data: &v})}
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
		if len(r) > 0{
			return r
		}
	default:
		log.Errorf("Expect xsql.Valuer or its array type.")
		return nil
	}
	return nil
}

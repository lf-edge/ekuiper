package plans

import (
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
)

type HavingPlan struct {
	Condition xsql.Expr
}

func (p *HavingPlan) Apply(ctx api.StreamContext, data interface{}) interface{} {
	log := ctx.GetLogger()
	log.Debugf("having plan receive %s", data)
	switch input := data.(type) {
	case error:
		return input
	case xsql.GroupedTuplesSet:
		r := xsql.GroupedTuplesSet{}
		for _, v := range input {
			ve := &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(v, v[0], &xsql.FunctionValuer{}, &xsql.AggregateFunctionValuer{Data: v}, &xsql.WildcardValuer{Data: v[0]})}
			result := ve.Eval(p.Condition)
			switch val := result.(type) {
			case error:
				return fmt.Errorf("run Having error: %s", val)
			case bool:
				if val {
					r = append(r, v)
				}
			default:
				return fmt.Errorf("run Having error: invalid condition that returns non-bool value %[1]T(%[1]v)", val)
			}
		}
		if len(r) > 0 {
			return r
		}
	case xsql.WindowTuplesSet:
		if len(input) != 1 {
			return fmt.Errorf("run Having error: input WindowTuplesSet with multiple tuples cannot be evaluated")
		}
		ms := input[0].Tuples
		r := ms[:0]
		for _, v := range ms {
			ve := &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(input, &v, &xsql.FunctionValuer{}, &xsql.AggregateFunctionValuer{Data: input}, &xsql.WildcardValuer{Data: &v})}
			result := ve.Eval(p.Condition)
			switch val := result.(type) {
			case error:
				return fmt.Errorf("run Having error: %s", val)
			case bool:
				if val {
					r = append(r, v)
				}
			default:
				return fmt.Errorf("run Having error: invalid condition that returns non-bool value %[1]T(%[1]v)", val)
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
			ve := &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(input, &v, &xsql.FunctionValuer{}, &xsql.AggregateFunctionValuer{Data: input}, &xsql.WildcardValuer{Data: &v})}
			result := ve.Eval(p.Condition)
			switch val := result.(type) {
			case error:
				return fmt.Errorf("run Having error: %s", val)
			case bool:
				if val {
					r = append(r, v)
				}
			default:
				return fmt.Errorf("run Having error: invalid condition that returns non-bool value %[1]T(%[1]v)", val)
			}
		}
		if len(r) > 0 {
			return r
		}
	default:
		return fmt.Errorf("run Having error: invalid input %[1]T(%[1]v)", input)
	}
	return nil
}

package plans

import (
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
)

type AggregatePlan struct {
	Dimensions xsql.Dimensions
}

/**
 *  input: *xsql.Tuple from preprocessor | xsql.WindowTuplesSet from windowOp | xsql.JoinTupleSets from joinOp
 *  output: xsql.GroupedTuplesSet
 */
func (p *AggregatePlan) Apply(ctx api.StreamContext, data interface{}) interface{} {
	log := ctx.GetLogger()
	log.Debugf("aggregate plan receive %s", data)
	var ms []xsql.DataValuer
	switch input := data.(type) {
	case error:
		return input
	case xsql.DataValuer:
		ms = append(ms, input)
	case xsql.WindowTuplesSet:
		if len(input) != 1 {
			log.Infof("WindowTuplesSet with multiple tuples cannot be evaluated")
			return nil
		}
		ms = make([]xsql.DataValuer, len(input[0].Tuples))
		for i, m := range input[0].Tuples {
			//this is needed or it will always point to the last
			t := m
			ms[i] = &t
		}
	case xsql.JoinTupleSets:
		ms = make([]xsql.DataValuer, len(input))
		for i, m := range input {
			t := m
			ms[i] = &t
		}
	default:
		return fmt.Errorf("expect xsql.Valuer or its array type.")
	}

	result := make(map[string]xsql.GroupedTuples)
	for _, m := range ms {
		var name string
		ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(m, &xsql.FunctionValuer{})}
		for _, d := range p.Dimensions {
			r := ve.Eval(d.Expr)
			if _, ok := r.(error); ok {
				return r
			} else {
				name += fmt.Sprintf("%v,", r)
			}
		}
		if ts, ok := result[name]; !ok {
			result[name] = xsql.GroupedTuples{m}
		} else {
			result[name] = append(ts, m)
		}
	}
	if len(result) > 0 {
		g := make([]xsql.GroupedTuples, 0, len(result))
		for _, v := range result {
			g = append(g, v)
		}
		return xsql.GroupedTuplesSet(g)
	} else {
		return nil
	}
}

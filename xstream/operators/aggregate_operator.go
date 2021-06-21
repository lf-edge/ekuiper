package operators

import (
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
)

type AggregateOp struct {
	Dimensions xsql.Dimensions
}

/**
 *  input: *xsql.Tuple from preprocessor | xsql.WindowTuplesSet from windowOp | xsql.JoinTupleSets from joinOp
 *  output: xsql.GroupedTuplesSet
 */
func (p *AggregateOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) interface{} {
	log := ctx.GetLogger()
	log.Debugf("aggregate plan receive %s", data)
	grouped := data
	if p.Dimensions != nil {
		var ms []xsql.DataValuer
		switch input := data.(type) {
		case error:
			return input
		case xsql.DataValuer:
			ms = append(ms, input)
		case xsql.WindowTuplesSet:
			if len(input) != 1 {
				return fmt.Errorf("run Group By error: the input WindowTuplesSet with multiple tuples cannot be evaluated")
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
			return fmt.Errorf("run Group By error: invalid input %[1]T(%[1]v)", input)
		}

		result := make(map[string]xsql.GroupedTuples)
		for _, m := range ms {
			var name string
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(m, fv)}
			for _, d := range p.Dimensions {
				r := ve.Eval(d.Expr)
				if _, ok := r.(error); ok {
					return fmt.Errorf("run Group By error: %s", r)
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
			grouped = xsql.GroupedTuplesSet(g)
		} else {
			grouped = nil
		}
	}
	return grouped
}

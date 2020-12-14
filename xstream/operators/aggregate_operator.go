package operators

import (
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
)

type AggregateOp struct {
	Dimensions xsql.Dimensions
	Alias      xsql.Fields
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
	if len(p.Alias) != 0 {
		switch input := grouped.(type) {
		case *xsql.Tuple:
			if err := p.calculateAlias(input, input, fv, afv); err != nil {
				return fmt.Errorf("run Aggregate function alias error: %s", err)
			}
		case xsql.GroupedTuplesSet:
			for _, v := range input {
				if err := p.calculateAlias(v[0], v, fv, afv); err != nil {
					return fmt.Errorf("run Aggregate function alias error: %s", err)
				}
			}
		case xsql.WindowTuplesSet:
			if len(input) != 1 {
				return fmt.Errorf("run Aggregate function alias error: the input WindowTuplesSet with multiple tuples cannot be evaluated)")
			}
			if err := p.calculateAlias(&input[0].Tuples[0], input, fv, afv); err != nil {
				return fmt.Errorf("run Aggregate function alias error: %s", err)
			}
		case xsql.JoinTupleSets:
			if err := p.calculateAlias(&input[0], input, fv, afv); err != nil {
				return fmt.Errorf("run Aggregate function alias error: %s", err)
			}
		default:
			return fmt.Errorf("run Aggregate function alias error: invalid input %[1]T(%[1]v)", input)
		}
	}

	return grouped
}

func (p *AggregateOp) calculateAlias(tuple xsql.DataValuer, agg xsql.AggregateData, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) error {
	afv.SetData(agg)
	ve := &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(agg, fv, tuple, fv, afv, &xsql.WildcardValuer{Data: tuple})}
	for _, f := range p.Alias {
		v := ve.Eval(f.Expr)
		err := setTuple(tuple, f.AName, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func setTuple(tuple xsql.DataValuer, name string, value interface{}) error {
	switch t := tuple.(type) {
	case *xsql.Tuple:
		t.Message[name] = value
	case *xsql.JoinTuple:
		t.Tuples[0].Message[name] = value
	default:
		return fmt.Errorf("invalid tuple to set alias")
	}
	return nil
}

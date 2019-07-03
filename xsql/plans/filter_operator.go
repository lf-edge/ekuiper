package plans

import (
	"context"
	"engine/common"
	"engine/xsql"
)

type FilterPlan struct {
	Condition xsql.Expr
}

func (p *FilterPlan) Apply(ctx context.Context, data interface{}) interface{} {
	log := common.Log
	var input map[string]interface{}
	if d, ok := data.(map[string]interface{}); !ok {
		log.Errorf("Expect map[string]interface{} type.\n")
		return nil
	} else {
		input = d
	}
	log.Infof("filterplan receive %s", input)
	ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(xsql.MapValuer(input), &xsql.FunctionValuer{})}
	result, ok := ve.Eval(p.Condition).(bool)
	if ok {
		if result {
			return input
		}
	} else {
		log.Errorf("invalid condition that returns non-bool value")
	}
	return nil
}
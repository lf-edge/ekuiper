package plans

import (
	"context"
	"engine/common"
	"engine/xsql"
)

type JoinPlan struct {
	Joins xsql.Joins
}

func (jp *JoinPlan) Apply(ctx context.Context, data interface{}) interface{} {
	var log = common.Log
	var input xsql.MultiEmitterTuples
	if d, ok := data.(xsql.MultiEmitterTuples ); !ok {
		log.Errorf("Expect MultiEmitterTuples type.\n")
		return nil
	} else {
		input = d
	}

	result := xsql.MergedEmitterTupleSets{}

	for _, join := range jp.Joins {
		ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(input, &xsql.FunctionValuer{}), JoinType: join.JoinType}
		v := ve.Eval(join.Expr)
		if v1, ok := v.(xsql.MergedEmitterTupleSets); ok {
			result = jp.mergeSet(v1, result)
		}
	}
	return result
}


func (jp *JoinPlan) mergeSet(set1 xsql.MergedEmitterTupleSets, set2 xsql.MergedEmitterTupleSets) xsql.MergedEmitterTupleSets {
	return set1
}



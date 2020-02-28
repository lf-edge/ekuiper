package plans

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
)

//TODO join expr should only be the equal op between 2 streams like tb1.id = tb2.id
type JoinPlan struct {
	From  *xsql.Table
	Joins xsql.Joins
}

// input:  xsql.WindowTuplesSet from windowOp, window is required for join
// output: xsql.JoinTupleSets
func (jp *JoinPlan) Apply(ctx api.StreamContext, data interface{}) interface{} {
	log := ctx.GetLogger()
	var input xsql.WindowTuplesSet
	if d, ok := data.(xsql.WindowTuplesSet); !ok {
		log.Errorf("Expect WindowTuplesSet type.\n")
		return nil
	} else {
		log.Debugf("join plan receive %v", d)
		input = d
	}

	result := xsql.JoinTupleSets{}

	for i, join := range jp.Joins {
		if i == 0 {
			v, err := jp.evalSet(input, join)
			if err != nil {
				fmt.Println(err)
				return nil
			}
			result = v
		} else {
			r1, _ := jp.evalJoinSets(&result, input, join)
			if v1, ok := r1.(xsql.JoinTupleSets); ok {
				result = v1
			}
		}
	}
	if result.Len() <= 0 {
		log.Debugf("join plan yields nothing")
		return nil
	}
	return result
}

func getStreamNames(join *xsql.Join) ([]string, error) {
	var srcs []string
	xsql.WalkFunc(join, func(node xsql.Node) {
		if f, ok := node.(*xsql.FieldRef); ok {
			if string(f.StreamName) == "" {
				return
			}
			srcs = append(srcs, string(f.StreamName))
		}
	})
	if len(srcs) != 2 {
		return nil, fmt.Errorf("Not correct join expression, it requires exactly 2 sources at ON expression.")
	}
	return srcs, nil
}

func (jp *JoinPlan) evalSet(input xsql.WindowTuplesSet, join xsql.Join) (xsql.JoinTupleSets, error) {
	var leftStream, rightStream string

	if join.JoinType != xsql.CROSS_JOIN {
		streams, err := getStreamNames(&join)
		if err != nil {
			return nil, err
		}
		leftStream = streams[0]
		rightStream = streams[1]
	} else {
		if jp.From.Alias == "" {
			leftStream = jp.From.Name
		} else {
			leftStream = jp.From.Alias
		}

		if join.Alias == "" {
			rightStream = join.Name
		} else {
			rightStream = join.Alias
		}
	}

	var lefts, rights []xsql.Tuple

	lefts = input.GetBySrc(leftStream)
	rights = input.GetBySrc(rightStream)

	sets := xsql.JoinTupleSets{}

	if join.JoinType == xsql.RIGHT_JOIN {
		return jp.evalSetWithRightJoin(input, join, false)
	}
	for _, left := range lefts {
		merged := &xsql.JoinTuple{}
		if join.JoinType == xsql.LEFT_JOIN || join.JoinType == xsql.FULL_JOIN || join.JoinType == xsql.CROSS_JOIN {
			merged.AddTuple(left)
		}
		for _, right := range rights {
			if join.JoinType == xsql.CROSS_JOIN {
				merged.AddTuple(right)
			} else {
				temp := &xsql.JoinTuple{}
				temp.AddTuple(left)
				temp.AddTuple(right)
				ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(temp, &xsql.FunctionValuer{})}
				if r, ok := ve.Eval(join.Expr).(bool); ok {
					if r {
						if join.JoinType == xsql.INNER_JOIN {
							merged.AddTuple(left)
							merged.AddTuple(right)
							sets = append(sets, *merged)
							merged = &xsql.JoinTuple{}
						} else {
							merged.AddTuple(right)
						}
					}
				} else {
					common.Log.Infoln("Evaluation error for set.")
				}
			}
		}
		if len(merged.Tuples) > 0 {
			sets = append(sets, *merged)
		}
	}

	if join.JoinType == xsql.FULL_JOIN {
		if rightJoinSet, err := jp.evalSetWithRightJoin(input, join, true); err == nil && len(rightJoinSet) > 0 {
			for _, jt := range rightJoinSet {
				sets = append(sets, jt)
			}
		}
	}
	return sets, nil
}

func (jp *JoinPlan) evalSetWithRightJoin(input xsql.WindowTuplesSet, join xsql.Join, excludeJoint bool) (xsql.JoinTupleSets, error) {
	streams, err := getStreamNames(&join)
	if err != nil {
		return nil, err
	}
	leftStream := streams[0]
	rightStream := streams[1]
	var lefts, rights []xsql.Tuple

	lefts = input.GetBySrc(leftStream)
	rights = input.GetBySrc(rightStream)

	sets := xsql.JoinTupleSets{}

	for _, right := range rights {
		merged := &xsql.JoinTuple{}
		merged.AddTuple(right)
		isJoint := false

		for _, left := range lefts {
			temp := &xsql.JoinTuple{}
			temp.AddTuple(right)
			temp.AddTuple(left)
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(temp, &xsql.FunctionValuer{})}
			if r, ok := ve.Eval(join.Expr).(bool); ok {
				if r {
					merged.AddTuple(left)
					isJoint = true
				}
			} else {
				common.Log.Infoln("Evaluation error for set.")
			}
		}
		if excludeJoint {
			if len(merged.Tuples) > 0 && (!isJoint) {
				sets = append(sets, *merged)
			}
		} else {
			if len(merged.Tuples) > 0 {
				sets = append(sets, *merged)
			}
		}
	}
	return sets, nil
}

func (jp *JoinPlan) evalJoinSets(set *xsql.JoinTupleSets, input xsql.WindowTuplesSet, join xsql.Join) (interface{}, error) {
	var rightStream string
	if join.Alias == "" {
		rightStream = join.Name
	} else {
		rightStream = join.Alias
	}

	rights := input.GetBySrc(rightStream)

	newSets := xsql.JoinTupleSets{}
	if join.JoinType == xsql.RIGHT_JOIN {
		return jp.evalRightJoinSets(set, input, join, false)
	}
	for _, left := range *set {
		merged := &xsql.JoinTuple{}
		if join.JoinType == xsql.LEFT_JOIN || join.JoinType == xsql.FULL_JOIN || join.JoinType == xsql.CROSS_JOIN {
			merged.AddTuples(left.Tuples)
		}
		innerAppend := false
		for _, right := range rights {
			if join.JoinType == xsql.CROSS_JOIN {
				merged.AddTuple(right)
			} else {
				ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(&left, &right, &xsql.FunctionValuer{})}
				if r, ok := ve.Eval(join.Expr).(bool); ok {
					if r {
						if join.JoinType == xsql.INNER_JOIN && !innerAppend {
							merged.AddTuples(left.Tuples)
							innerAppend = true
						}
						merged.AddTuple(right)
					}
				}
			}
		}

		if len(merged.Tuples) > 0 {
			newSets = append(newSets, *merged)
		}
	}

	if join.JoinType == xsql.FULL_JOIN {
		if rightJoinSet, err := jp.evalRightJoinSets(set, input, join, true); err == nil && len(rightJoinSet) > 0 {
			for _, jt := range rightJoinSet {
				newSets = append(newSets, jt)
			}
		}
	}

	return newSets, nil
}

func (jp *JoinPlan) evalRightJoinSets(set *xsql.JoinTupleSets, input xsql.WindowTuplesSet, join xsql.Join, excludeJoint bool) (xsql.JoinTupleSets, error) {
	var rightStream string
	if join.Alias == "" {
		rightStream = join.Name
	} else {
		rightStream = join.Alias
	}
	rights := input.GetBySrc(rightStream)

	newSets := xsql.JoinTupleSets{}

	for _, right := range rights {
		merged := &xsql.JoinTuple{}
		merged.AddTuple(right)
		isJoint := false
		for _, left := range *set {
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(&right, &left, &xsql.FunctionValuer{})}
			if r, ok := ve.Eval(join.Expr).(bool); ok {
				if r {
					isJoint = true
					merged.AddTuples(left.Tuples)
				}
			}
		}

		if excludeJoint {
			if len(merged.Tuples) > 0 && (!isJoint) {
				newSets = append(newSets, *merged)
			}
		} else {
			if len(merged.Tuples) > 0 {
				newSets = append(newSets, *merged)
			}
		}
	}
	return newSets, nil
}

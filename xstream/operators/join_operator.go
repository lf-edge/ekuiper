package operators

import (
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
)

//TODO join expr should only be the equal op between 2 streams like tb1.id = tb2.id
type JoinOp struct {
	From  *xsql.Table
	Joins xsql.Joins
}

// input:  xsql.WindowTuplesSet from windowOp, window is required for join
// output: xsql.JoinTupleSets
func (jp *JoinOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	log := ctx.GetLogger()
	var input xsql.WindowTuplesSet
	switch v := data.(type) {
	case error:
		return v
	case xsql.WindowTuplesSet:
		input = v
		log.Debugf("join plan receive %v", data)
	default:
		return fmt.Errorf("run Join error: join is only supported in window")
	}
	result := xsql.JoinTupleSets{}
	for i, join := range jp.Joins {
		if i == 0 {
			v, err := jp.evalSet(input, join, fv)
			if err != nil {
				return fmt.Errorf("run Join error: %s", err)
			}
			result = v
		} else {
			r1, err := jp.evalJoinSets(&result, input, join, fv)
			if err != nil {
				return fmt.Errorf("run Join error: %s", err)
			}
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

func (jp *JoinOp) getStreamNames(join *xsql.Join) ([]string, error) {
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
		if jp.From.Alias != "" {
			srcs = append(srcs, jp.From.Alias)
		} else {
			srcs = append(srcs, jp.From.Name)
		}
		if join.Alias != "" {
			srcs = append(srcs, join.Alias)
		} else {
			srcs = append(srcs, join.Name)
		}
	}

	return srcs, nil
}

func (jp *JoinOp) evalSet(input xsql.WindowTuplesSet, join xsql.Join, fv *xsql.FunctionValuer) (xsql.JoinTupleSets, error) {
	var leftStream, rightStream string

	if join.JoinType != xsql.CROSS_JOIN {
		streams, err := jp.getStreamNames(&join)
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
		return jp.evalSetWithRightJoin(input, join, false, fv)
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
				ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(temp, fv)}
				result := evalOn(join, ve, &left, &right)

				switch val := result.(type) {
				case error:
					return nil, val
				case bool:
					if val {
						if join.JoinType == xsql.INNER_JOIN {
							merged.AddTuple(left)
							merged.AddTuple(right)
							sets = append(sets, *merged)
							merged = &xsql.JoinTuple{}
						} else {
							merged.AddTuple(right)
						}
					}
				default:
					return nil, fmt.Errorf("invalid join condition that returns non-bool value %[1]T(%[1]v)", val)
				}
			}
		}
		if len(merged.Tuples) > 0 {
			sets = append(sets, *merged)
		}
	}

	if join.JoinType == xsql.FULL_JOIN {
		if rightJoinSet, err := jp.evalSetWithRightJoin(input, join, true, fv); err == nil {
			if len(rightJoinSet) > 0 {
				for _, jt := range rightJoinSet {
					sets = append(sets, jt)
				}
			}
		} else {
			return nil, err
		}
	}
	return sets, nil
}

func evalOn(join xsql.Join, ve *xsql.ValuerEval, left interface{}, right *xsql.Tuple) interface{} {
	var result interface{}
	if join.Expr != nil {
		result = ve.Eval(join.Expr)
	} else if join.JoinType == xsql.INNER_JOIN { // if no on expression
		result = left != nil && right != nil
	} else {
		result = true
	}
	return result
}

func (jp *JoinOp) evalSetWithRightJoin(input xsql.WindowTuplesSet, join xsql.Join, excludeJoint bool, fv *xsql.FunctionValuer) (xsql.JoinTupleSets, error) {
	streams, err := jp.getStreamNames(&join)
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
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(temp, fv)}
			result := evalOn(join, ve, &left, &right)
			switch val := result.(type) {
			case error:
				return nil, val
			case bool:
				if val {
					merged.AddTuple(left)
					isJoint = true
				}
			default:
				return nil, fmt.Errorf("invalid join condition that returns non-bool value %[1]T(%[1]v)", val)
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

func (jp *JoinOp) evalJoinSets(set *xsql.JoinTupleSets, input xsql.WindowTuplesSet, join xsql.Join, fv *xsql.FunctionValuer) (interface{}, error) {
	var rightStream string
	if join.Alias == "" {
		rightStream = join.Name
	} else {
		rightStream = join.Alias
	}

	rights := input.GetBySrc(rightStream)

	newSets := xsql.JoinTupleSets{}
	if join.JoinType == xsql.RIGHT_JOIN {
		return jp.evalRightJoinSets(set, input, join, false, fv)
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
				ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(&left, &right, fv)}
				result := evalOn(join, ve, &left, &right)
				switch val := result.(type) {
				case error:
					return nil, val
				case bool:
					if val {
						if join.JoinType == xsql.INNER_JOIN && !innerAppend {
							merged.AddTuples(left.Tuples)
							innerAppend = true
						}
						merged.AddTuple(right)
					}
				default:
					return nil, fmt.Errorf("invalid join condition that returns non-bool value %[1]T(%[1]v)", val)
				}
			}
		}

		if len(merged.Tuples) > 0 {
			newSets = append(newSets, *merged)
		}
	}

	if join.JoinType == xsql.FULL_JOIN {
		if rightJoinSet, err := jp.evalRightJoinSets(set, input, join, true, fv); err == nil && len(rightJoinSet) > 0 {
			for _, jt := range rightJoinSet {
				newSets = append(newSets, jt)
			}
		}
	}

	return newSets, nil
}

func (jp *JoinOp) evalRightJoinSets(set *xsql.JoinTupleSets, input xsql.WindowTuplesSet, join xsql.Join, excludeJoint bool, fv *xsql.FunctionValuer) (xsql.JoinTupleSets, error) {
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
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(&right, &left, fv)}
			result := evalOn(join, ve, &left, &right)
			switch val := result.(type) {
			case error:
				return nil, val
			case bool:
				if val {
					isJoint = true
					merged.AddTuples(left.Tuples)
				}
			default:
				return nil, fmt.Errorf("invalid join condition that returns non-bool value %[1]T(%[1]v)", val)
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

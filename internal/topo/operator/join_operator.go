// Copyright 2021-2022 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

// JoinOp TODO join expr should only be the equal op between 2 streams like tb1.id = tb2.id
type JoinOp struct {
	From  *ast.Table
	Joins ast.Joins
}

// Apply
// input:  MergedCollection, the Row must be a Tuple
// output: Collection
func (jp *JoinOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	log := ctx.GetLogger()
	var input xsql.MergedCollection
	switch v := data.(type) {
	case error:
		return v
	case xsql.MergedCollection:
		input = v
		log.Debugf("join plan receive %v", data)
	default:
		return fmt.Errorf("run Join error: join is only supported in window")
	}
	result := &xsql.JoinTuples{Content: make([]*xsql.JoinTuple, 0)}
	for i, join := range jp.Joins {
		if i == 0 {
			v, err := jp.evalSet(input, join, fv)
			if err != nil {
				return fmt.Errorf("run Join error: %s", err)
			}
			result = v
		} else {
			r1, err := jp.evalJoinSets(result, input, join, fv)
			if err != nil {
				return fmt.Errorf("run Join error: %s", err)
			}
			if v1, ok := r1.(*xsql.JoinTuples); ok {
				result = v1
			}
		}
	}
	if result.Len() <= 0 {
		log.Debugf("join plan yields nothing")
		return nil
	}
	result.WindowRange = input.GetWindowRange()
	return result
}

func (jp *JoinOp) getStreamNames(join *ast.Join) ([]string, error) {
	var srcs []string
	keys := make(map[ast.StreamName]bool)
	ast.WalkFunc(join, func(node ast.Node) bool {
		if f, ok := node.(*ast.FieldRef); ok {
			for _, v := range f.RefSources() {
				// Exclude default stream as it is a virtual stream name.
				if v == ast.DefaultStream {
					continue
				}
				if _, ok := keys[v]; !ok {
					srcs = append(srcs, string(v))
					keys[v] = true
				}
			}
		}
		return true
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

func (jp *JoinOp) evalSet(input xsql.MergedCollection, join ast.Join, fv *xsql.FunctionValuer) (*xsql.JoinTuples, error) {
	var leftStream, rightStream string

	if join.JoinType != ast.CROSS_JOIN {
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

	var lefts, rights []xsql.Row

	lefts = input.GetBySrc(leftStream)
	rights = input.GetBySrc(rightStream)

	sets := &xsql.JoinTuples{Content: make([]*xsql.JoinTuple, 0)}

	if join.JoinType == ast.RIGHT_JOIN {
		return jp.evalSetWithRightJoin(input, join, false, fv)
	}
	for _, left := range lefts {
		leftJoined := false
		for index, right := range rights {
			tupleJoined := false
			merged := &xsql.JoinTuple{}
			if join.JoinType == ast.LEFT_JOIN || join.JoinType == ast.FULL_JOIN || join.JoinType == ast.CROSS_JOIN {
				merged.AddTuple(left)
			}
			if join.JoinType == ast.CROSS_JOIN {
				tupleJoined = true
				merged.AddTuple(right)
			} else {
				temp := &xsql.JoinTuple{}
				temp.AddTuple(left)
				temp.AddTuple(right)
				ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(temp, fv)}
				result := evalOn(join, ve, left, right)
				merged.AliasMap = temp.AliasMap
				switch val := result.(type) {
				case error:
					return nil, val
				case bool:
					if val {
						leftJoined = true
						tupleJoined = true
						if join.JoinType == ast.INNER_JOIN {
							merged.AddTuple(left)
							merged.AddTuple(right)
						} else {
							merged.AddTuple(right)
						}
					}
				default:
					return nil, fmt.Errorf("invalid join condition that returns non-bool value %[1]T(%[1]v)", val)
				}
			}
			if tupleJoined || (!leftJoined && index == len(rights)-1 && len(merged.Tuples) > 0) {
				leftJoined = true
				sets.Content = append(sets.Content, merged)
			}
		}
		// If no messages in the right
		if !leftJoined && join.JoinType != ast.INNER_JOIN && join.JoinType != ast.CROSS_JOIN {
			merged := &xsql.JoinTuple{}
			merged.AddTuple(left)
			sets.Content = append(sets.Content, merged)
		}
	}

	if join.JoinType == ast.FULL_JOIN {
		if rightJoinSet, err := jp.evalSetWithRightJoin(input, join, true, fv); err == nil {
			if len(rightJoinSet.Content) > 0 {
				for _, jt := range rightJoinSet.Content {
					sets.Content = append(sets.Content, jt)
				}
			}
		} else {
			return nil, err
		}
	}
	return sets, nil
}

func evalOn(join ast.Join, ve *xsql.ValuerEval, left interface{}, right xsql.Row) interface{} {
	var result interface{}
	if join.Expr != nil {
		result = ve.Eval(join.Expr)
	} else if join.JoinType == ast.INNER_JOIN { // if no on expression
		result = left != nil && right != nil
	} else {
		result = true
	}
	return result
}

func (jp *JoinOp) evalSetWithRightJoin(input xsql.MergedCollection, join ast.Join, excludeJoint bool, fv *xsql.FunctionValuer) (*xsql.JoinTuples, error) {
	streams, err := jp.getStreamNames(&join)
	if err != nil {
		return nil, err
	}
	leftStream := streams[0]
	rightStream := streams[1]
	var lefts, rights []xsql.Row

	lefts = input.GetBySrc(leftStream)
	rights = input.GetBySrc(rightStream)

	sets := &xsql.JoinTuples{Content: make([]*xsql.JoinTuple, 0)}

	for _, right := range rights {
		isJoint := false
		for index, left := range lefts {
			tupleJoined := false
			merged := &xsql.JoinTuple{}
			merged.AddTuple(right)
			temp := &xsql.JoinTuple{}
			temp.AddTuple(right)
			temp.AddTuple(left)
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(temp, fv)}
			result := evalOn(join, ve, left, right)
			merged.AliasMap = temp.AliasMap
			switch val := result.(type) {
			case error:
				return nil, val
			case bool:
				if val {
					merged.AddTuple(left)
					isJoint = true
					tupleJoined = true
				}
			default:
				return nil, fmt.Errorf("invalid join condition that returns non-bool value %[1]T(%[1]v)", val)
			}
			if !excludeJoint && (tupleJoined || (!isJoint && index == len(lefts)-1 && len(merged.Tuples) > 0)) {
				isJoint = true
				sets.Content = append(sets.Content, merged)
			}
		}
		if !isJoint {
			merged := &xsql.JoinTuple{}
			merged.AddTuple(right)
			sets.Content = append(sets.Content, merged)
		}
	}
	return sets, nil
}

func (jp *JoinOp) evalJoinSets(set *xsql.JoinTuples, input xsql.MergedCollection, join ast.Join, fv *xsql.FunctionValuer) (interface{}, error) {
	var rightStream string
	if join.Alias == "" {
		rightStream = join.Name
	} else {
		rightStream = join.Alias
	}

	rights := input.GetBySrc(rightStream)

	newSets := &xsql.JoinTuples{Content: make([]*xsql.JoinTuple, 0)}
	if join.JoinType == ast.RIGHT_JOIN {
		return jp.evalRightJoinSets(set, input, join, false, fv)
	}
	for _, left := range set.Content {
		leftJoined := false
		for index, right := range rights {
			tupleJoined := false
			merged := &xsql.JoinTuple{}
			if join.JoinType == ast.LEFT_JOIN || join.JoinType == ast.FULL_JOIN || join.JoinType == ast.CROSS_JOIN {
				merged.AddTuples(left.Tuples)
			}
			if join.JoinType == ast.CROSS_JOIN {
				tupleJoined = true
				merged.AddTuple(right)
			} else {
				temp := &xsql.JoinTuple{}
				temp.AddTuples(left.Tuples)
				temp.AddTuple(right)

				ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(temp, fv)}
				result := evalOn(join, ve, left, right)
				merged.AliasMap = left.AliasMap
				switch val := result.(type) {
				case error:
					return nil, val
				case bool:
					if val {
						leftJoined = true
						tupleJoined = true
						if join.JoinType == ast.INNER_JOIN {
							merged.AddTuples(left.Tuples)
						}
						merged.AddTuple(right)
					}
				default:
					return nil, fmt.Errorf("invalid join condition that returns non-bool value %[1]T(%[1]v)", val)
				}
			}
			if tupleJoined || (!leftJoined && index == len(rights)-1 && len(merged.Tuples) > 0) {
				leftJoined = true
				newSets.Content = append(newSets.Content, merged)
			}
		}

		if !leftJoined && join.JoinType != ast.INNER_JOIN && join.JoinType != ast.CROSS_JOIN {
			merged := &xsql.JoinTuple{}
			merged.AddTuples(left.Tuples)
			newSets.Content = append(newSets.Content, merged)
		}
	}

	if join.JoinType == ast.FULL_JOIN {
		if rightJoinSet, err := jp.evalRightJoinSets(set, input, join, true, fv); err == nil && len(rightJoinSet.Content) > 0 {
			for _, jt := range rightJoinSet.Content {
				newSets.Content = append(newSets.Content, jt)
			}
		}
	}

	return newSets, nil
}

func (jp *JoinOp) evalRightJoinSets(set *xsql.JoinTuples, input xsql.MergedCollection, join ast.Join, excludeJoint bool, fv *xsql.FunctionValuer) (*xsql.JoinTuples, error) {
	var rightStream string
	if join.Alias == "" {
		rightStream = join.Name
	} else {
		rightStream = join.Alias
	}
	rights := input.GetBySrc(rightStream)

	newSets := &xsql.JoinTuples{Content: make([]*xsql.JoinTuple, 0)}

	for _, right := range rights {
		isJoint := false
		for index, left := range set.Content {
			tupleJoined := false
			merged := &xsql.JoinTuple{}
			merged.AddTuple(right)

			temp := &xsql.JoinTuple{}
			temp.AddTuples(left.Tuples)
			temp.AddTuple(right)
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(temp, fv)}
			result := evalOn(join, ve, left, right)
			merged.AliasMap = left.AliasMap
			switch val := result.(type) {
			case error:
				return nil, val
			case bool:
				if val {
					isJoint = true
					tupleJoined = true
					merged.AddTuples(left.Tuples)
				}
			default:
				return nil, fmt.Errorf("invalid join condition that returns non-bool value %[1]T(%[1]v)", val)
			}
			if !excludeJoint && (tupleJoined || (!isJoint && index == len(set.Content)-1 && len(merged.Tuples) > 0)) {
				isJoint = true
				newSets.Content = append(newSets.Content, merged)
			}
		}

		if !isJoint {
			merged := &xsql.JoinTuple{}
			merged.AddTuple(right)
			newSets.Content = append(newSets.Content, merged)
		}
	}
	return newSets, nil
}

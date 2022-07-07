// Copyright 2022 EMQ Technologies Co., Ltd.
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

package planner

import (
	"errors"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/topo"
	"github.com/lf-edge/ekuiper/internal/topo/graph"
	"github.com/lf-edge/ekuiper/internal/topo/node"
	"github.com/lf-edge/ekuiper/internal/topo/operator"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
	"strings"
)

// PlanByGraph returns a topo.Topo object by a graph
// TODO in the future, graph may also be converted to a plan and get optimized
func PlanByGraph(rule *api.Rule) (*topo.Topo, error) {
	graph := rule.Graph
	if graph == nil {
		return nil, errors.New("no graph")
	}
	tp, err := topo.NewWithNameAndQos(rule.Id, rule.Options.Qos, rule.Options.CheckpointInterval)
	if err != nil {
		return nil, err
	}
	var (
		nodeMap = make(map[string]api.TopNode)
		sinks   = make(map[string]bool)
		sources = make(map[string]bool)
	)
	for nodeName, gn := range graph.Nodes {
		switch gn.Type {
		case "source":
			sourceType, ok := gn.Props["source_type"]
			if !ok {
				sourceType = "stream"
			}
			st, ok := sourceType.(string)
			if !ok {
				return nil, fmt.Errorf("source_type %v is not string", sourceType)
			}
			st = strings.ToLower(st)
			sourceOption := &ast.Options{}
			err := cast.MapToStruct(gn.Props, sourceOption)
			if err != nil {
				return nil, err
			}
			sourceOption.TYPE = gn.NodeType
			switch st {
			case "stream":
				// TODO deal with conf key
				pp, err := operator.NewPreprocessor(true, nil, true, nil, rule.Options.IsEventTime, sourceOption.TIMESTAMP, sourceOption.TIMESTAMP_FORMAT, strings.EqualFold(sourceOption.FORMAT, message.FormatBinary), sourceOption.STRICT_VALIDATION)
				if err != nil {
					return nil, err
				}
				srcNode := node.NewSourceNode(nodeName, ast.TypeStream, pp, sourceOption, rule.Options.SendError)
				nodeMap[nodeName] = srcNode
				tp.AddSrc(srcNode)
			case "table":
				// TODO add table
			default:
				return nil, fmt.Errorf("unknown source type %s", st)
			}
			sources[nodeName] = true
		case "sink":
			if _, ok := graph.Topo.Edges[nodeName]; ok {
				return nil, fmt.Errorf("sink %s has edge", nodeName)
			}
			nodeMap[nodeName] = node.NewSinkNode(nodeName, gn.NodeType, gn.Props)
			sinks[nodeName] = true
		case "operator":
			switch gn.NodeType {
			case "function":
				fop, err := parseFunc(gn.Props)
				if err != nil {
					return nil, err
				}
				op := Transform(fop, nodeName, rule.Options)
				nodeMap[nodeName] = op
			case "filter":
				fop, err := parseFilter(gn.Props)
				if err != nil {
					return nil, err
				}
				op := Transform(fop, nodeName, rule.Options)
				nodeMap[nodeName] = op
			case "pick":
				pop, err := parsePick(gn.Props)
				if err != nil {
					return nil, err
				}
				op := Transform(pop, nodeName, rule.Options)
				nodeMap[nodeName] = op
			default: // TODO other node type
				return nil, fmt.Errorf("unknown operator type %s", gn.NodeType)
			}
		default:
			return nil, fmt.Errorf("unknown node type %s", gn.Type)
		}
	}
	// validate source node
	for _, nodeName := range graph.Topo.Sources {
		if _, ok := sources[nodeName]; !ok {
			return nil, fmt.Errorf("source %s is not a source type node", nodeName)
		}
	}
	// reverse edges
	reversedEdges := make(map[string][]string)
	for fromNode, toNodes := range graph.Topo.Edges {
		for _, toNode := range toNodes {
			reversedEdges[toNode] = append(reversedEdges[toNode], fromNode)
		}
	}
	// add the linkages
	for nodeName, fromNodes := range reversedEdges {
		inputs := make([]api.Emitter, len(fromNodes))
		for i, fromNode := range fromNodes {
			inputs[i] = nodeMap[fromNode].(api.Emitter)
		}
		n := nodeMap[nodeName]
		if _, ok := sinks[nodeName]; ok {
			tp.AddSink(inputs, n.(*node.SinkNode))
		} else {
			tp.AddOperator(inputs, n.(node.OperatorNode))
		}
	}
	return tp, nil
}

func parsePick(props map[string]interface{}) (*operator.ProjectOp, error) {
	n := &graph.Select{}
	cast.MapToStruct(props, n)
	stmt, err := xsql.NewParser(strings.NewReader("select " + strings.Join(n.Fields, ",") + " from nonexist")).Parse()
	if err != nil {
		return nil, err
	}
	t := ProjectPlan{
		fields:      stmt.Fields,
		isAggregate: xsql.IsAggStatement(stmt),
	}.Init()
	return &operator.ProjectOp{ColNames: t.colNames, AliasNames: t.aliasNames, AliasFields: t.aliasFields, ExprFields: t.exprFields, IsAggregate: t.isAggregate, AllWildcard: t.allWildcard, WildcardEmitters: t.wildcardEmitters, ExprNames: t.exprNames, SendMeta: t.sendMeta}, nil
}

func parseFunc(props map[string]interface{}) (*operator.FuncOp, error) {
	m, ok := props["expr"]
	if !ok {
		return nil, errors.New("no expr")
	}
	funcExpr, ok := m.(string)
	if !ok {
		return nil, fmt.Errorf("expr %v is not string", m)
	}
	stmt, err := xsql.NewParser(strings.NewReader("select " + funcExpr + " from nonexist")).Parse()
	if err != nil {
		return nil, err
	}
	f := stmt.Fields[0]
	c, ok := f.Expr.(*ast.Call)
	if !ok {
		// never happen
		return nil, fmt.Errorf("expr %v is not ast.Call", stmt.Fields[0].Expr)
	}
	var name string
	if f.AName != "" {
		name = f.AName
	} else {
		name = f.Name
	}
	return &operator.FuncOp{CallExpr: c, Name: name}, nil
}

func parseFilter(props map[string]interface{}) (*operator.FilterOp, error) {
	m, ok := props["expr"]
	if !ok {
		return nil, errors.New("no expr")
	}
	conditionExpr, ok := m.(string)
	if !ok {
		return nil, fmt.Errorf("expr %v is not string", m)
	}
	p := xsql.NewParser(strings.NewReader("where " + conditionExpr))
	if exp, err := p.ParseCondition(); err != nil {
		return nil, err
	} else {
		if exp != nil {
			return &operator.FilterOp{Condition: exp}, nil
		}
	}
	return nil, fmt.Errorf("expr %v is not a condition", m)
}

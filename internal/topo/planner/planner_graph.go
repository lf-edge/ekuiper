// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/binder/function"
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

type genNodeFunc func(name string, props map[string]interface{}, options *api.RuleOption) (api.TopNode, error)

var extNodes = map[string]genNodeFunc{}

// PlanByGraph returns a topo.Topo object by a graph
func PlanByGraph(rule *api.Rule) (*topo.Topo, error) {
	ruleGraph := rule.Graph
	if ruleGraph == nil {
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
	for nodeName, gn := range ruleGraph.Nodes {
		switch gn.Type {
		case "source":
			if _, ok := ruleGraph.Topo.Edges[nodeName]; !ok {
				return nil, fmt.Errorf("no edge defined for source node %s", nodeName)
			}
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
				return nil, fmt.Errorf("table source is not supported yet")
			default:
				return nil, fmt.Errorf("unknown source type %s", st)
			}
			sources[nodeName] = true
		case "sink":
			if _, ok := ruleGraph.Topo.Edges[nodeName]; ok {
				return nil, fmt.Errorf("sink %s has edge", nodeName)
			}
			nodeMap[nodeName] = node.NewSinkNode(nodeName, gn.NodeType, gn.Props)
			sinks[nodeName] = true
		case "operator":
			if _, ok := ruleGraph.Topo.Edges[nodeName]; !ok {
				return nil, fmt.Errorf("no edge defined for operator node %s", nodeName)
			}
			nt := strings.ToLower(gn.NodeType)
			switch nt {
			case "function":
				fop, err := parseFunc(gn.Props)
				if err != nil {
					return nil, err
				}
				op := Transform(fop, nodeName, rule.Options)
				nodeMap[nodeName] = op
			case "aggfunc":
				fop, err := parseFunc(gn.Props)
				if err != nil {
					return nil, err
				}
				fop.IsAgg = true
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
			case "window":
				wconf, err := parseWindow(gn.Props)
				if err != nil {
					return nil, err
				}
				op, err := node.NewWindowOp(nodeName, *wconf, ruleGraph.Topo.Sources, rule.Options)
				if err != nil {
					return nil, err
				}
				nodeMap[nodeName] = op
			case "join":
				jop, err := parseJoin(gn.Props)
				if err != nil {
					return nil, err
				}
				op := Transform(jop, nodeName, rule.Options)
				nodeMap[nodeName] = op
			case "groupby":
				gop, err := parseGroupBy(gn.Props)
				if err != nil {
					return nil, err
				}
				op := Transform(gop, nodeName, rule.Options)
				nodeMap[nodeName] = op
			case "orderby":
				oop, err := parseOrderBy(gn.Props)
				if err != nil {
					return nil, err
				}
				op := Transform(oop, nodeName, rule.Options)
				nodeMap[nodeName] = op
			case "switch":
				sconf, err := parseSwitch(gn.Props)
				if err != nil {
					return nil, fmt.Errorf("parse switch %s error: %v", nodeName, err)
				}
				op, err := node.NewSwitchNode(nodeName, sconf, rule.Options)
				if err != nil {
					return nil, fmt.Errorf("create switch %s error: %v", nodeName, err)
				}
				nodeMap[nodeName] = op
			default:
				gnf, ok := extNodes[nt]
				if !ok {
					return nil, fmt.Errorf("unknown operator type %s", gn.NodeType)
				}
				op, err := gnf(nodeName, gn.Props, rule.Options)
				if err != nil {
					return nil, err
				}
				nodeMap[nodeName] = op
			}
		default:
			return nil, fmt.Errorf("unknown node type %s", gn.Type)
		}
	}

	// validate source node
	for _, nodeName := range ruleGraph.Topo.Sources {
		if _, ok := sources[nodeName]; !ok {
			return nil, fmt.Errorf("source %s is not a source type node", nodeName)
		}
	}

	// reverse edges, value is a 2-dim array. Only switch node will have the second dim
	reversedEdges := make(map[string][][]string)
	rclone := make(map[string][]string)
	for fromNode, toNodes := range ruleGraph.Topo.Edges {
		if _, ok := ruleGraph.Nodes[fromNode]; !ok {
			return nil, fmt.Errorf("node %s is not defined", fromNode)
		}
		for i, toNode := range toNodes {
			switch tn := toNode.(type) {
			case string:
				if _, ok := ruleGraph.Nodes[tn]; !ok {
					return nil, fmt.Errorf("node %s is not defined", tn)
				}
				if _, ok := reversedEdges[tn]; !ok {
					reversedEdges[tn] = make([][]string, 1)
				}
				reversedEdges[tn][0] = append(reversedEdges[tn][0], fromNode)
				rclone[tn] = append(rclone[tn], fromNode)
			case []interface{}:
				for _, tni := range tn {
					tnn, ok := tni.(string)
					if !ok { // never happen
						return nil, fmt.Errorf("invalid edge toNode %v", toNode)
					}
					if _, ok := ruleGraph.Nodes[tnn]; !ok {
						return nil, fmt.Errorf("node %s is not defined", tnn)
					}
					for len(reversedEdges[tnn]) <= i {
						reversedEdges[tnn] = append(reversedEdges[tnn], []string{})
					}
					reversedEdges[tnn][i] = append(reversedEdges[tnn][i], fromNode)
					rclone[tnn] = append(rclone[tnn], fromNode)
				}
			}
		}
	}
	// sort the nodes by topological order
	nodesInOrder := make([]string, len(ruleGraph.Nodes))
	i := 0
	genNodesInOrder(ruleGraph.Topo.Sources, ruleGraph.Topo.Edges, rclone, nodesInOrder, i)

	// validate the typo
	// the map is to record the output for each node
	dataFlow := make(map[string]*graph.IOType)
	for _, n := range nodesInOrder {
		gn := ruleGraph.Nodes[n]
		if gn == nil {
			return nil, fmt.Errorf("can't find node %s", n)
		}
		if gn.Type == "source" {
			dataFlow[n] = &graph.IOType{
				Type:           graph.IOINPUT_TYPE_ROW,
				RowType:        graph.IOROW_TYPE_SINGLE,
				CollectionType: graph.IOCOLLECTION_TYPE_ANY,
				AllowMulti:     false,
			}
		} else if gn.Type == "sink" {
			continue
		} else {
			nodeIO, ok := graph.OpIO[strings.ToLower(gn.NodeType)]
			if !ok {
				return nil, fmt.Errorf("can't find the io definiton for node type %s", gn.NodeType)
			}
			dataInCondition := nodeIO[0]
			indim := reversedEdges[n]
			var innodes []string
			for _, in := range indim {
				innodes = append(innodes, in...)
			}
			if len(innodes) > 1 {
				if dataInCondition.AllowMulti {
					for _, innode := range innodes {
						_, err = graph.Fit(dataFlow[innode], dataInCondition)
						if err != nil {
							return nil, fmt.Errorf("node %s output does not match node %s input: %v", innode, n, err)
						}
					}
				} else {
					return nil, fmt.Errorf("operator %s of type %s does not allow multiple inputs", n, gn.NodeType)
				}
			} else if len(innodes) == 1 {
				_, err := graph.Fit(dataFlow[innodes[0]], dataInCondition)
				if err != nil {
					return nil, fmt.Errorf("node %s output does not match node %s input: %v", innodes[0], n, err)
				}
			} else {
				return nil, fmt.Errorf("operator %s of type %s has no input", n, gn.NodeType)
			}
			out := nodeIO[1]
			in := dataFlow[innodes[0]]
			dataFlow[n] = graph.MapOut(in, out)
			// convert filter to having if the input is aggregated
			if gn.NodeType == "filter" && in.Type == graph.IOINPUT_TYPE_COLLECTION && in.CollectionType == graph.IOCOLLECTION_TYPE_GROUPED {
				fop, err := parseHaving(gn.Props)
				if err != nil {
					return nil, err
				}
				op := Transform(fop, n, rule.Options)
				nodeMap[n] = op
			}
		}
	}
	// add the linkages
	for nodeName, fromNodes := range reversedEdges {
		totalLen := 0
		for _, fromNode := range fromNodes {
			totalLen += len(fromNode)
		}
		inputs := make([]api.Emitter, 0, totalLen)
		for i, fromNode := range fromNodes {
			for _, from := range fromNode {
				if i == 0 {
					inputs = append(inputs, nodeMap[from].(api.Emitter))
				} else {
					switch sn := nodeMap[from].(type) {
					case *node.SwitchNode:
						inputs = append(inputs, sn.GetEmitter(i))
					default:
						return nil, fmt.Errorf("node %s is not a switch node but have multiple output", from)
					}
				}
			}
		}
		n := nodeMap[nodeName]
		if n == nil {
			return nil, fmt.Errorf("node %s is not defined", nodeName)
		}
		if _, ok := sinks[nodeName]; ok {
			tp.AddSink(inputs, n.(*node.SinkNode))
		} else {
			tp.AddOperator(inputs, n.(node.OperatorNode))
		}
	}
	return tp, nil
}

func genNodesInOrder(toNodes []string, edges map[string][]interface{}, flatReversedEdges map[string][]string, nodesInOrder []string, i int) int {
	for _, src := range toNodes {
		if len(flatReversedEdges[src]) > 1 {
			flatReversedEdges[src] = flatReversedEdges[src][1:]
			continue
		}
		nodesInOrder[i] = src
		i++
		tns := make([]string, 0, len(edges[src]))
		for _, toNode := range edges[src] {
			switch toNode.(type) {
			case string:
				tns = append(tns, toNode.(string))
			case []interface{}:
				for _, tni := range toNode.([]interface{}) {
					tns = append(tns, tni.(string))
				}
			}
		}
		i = genNodesInOrder(tns, edges, flatReversedEdges, nodesInOrder, i)
	}
	return i
}

func parseOrderBy(props map[string]interface{}) (*operator.OrderOp, error) {
	n := &graph.Orderby{}
	err := cast.MapToStruct(props, n)
	if err != nil {
		return nil, err
	}
	stmt := "SELECT * FROM unknown ORDER BY"
	for _, s := range n.Sorts {
		stmt += " " + s.Field + " "
		if s.Desc {
			stmt += "DESC"
		}
	}
	p, err := xsql.NewParser(strings.NewReader(stmt)).Parse()
	if err != nil {
		return nil, fmt.Errorf("invalid order by statement error: %v", err)
	}
	if len(p.SortFields) == 0 {
		return nil, fmt.Errorf("order by statement is empty")
	}
	return &operator.OrderOp{
		SortFields: p.SortFields,
	}, nil
}

func parseGroupBy(props map[string]interface{}) (*operator.AggregateOp, error) {
	n := &graph.Groupby{}
	err := cast.MapToStruct(props, n)
	if err != nil {
		return nil, err
	}
	if len(n.Dimensions) == 0 {
		return nil, fmt.Errorf("groupby must have at least one dimension")
	}
	stmt := "SELECT * FROM unknown Group By " + strings.Join(n.Dimensions, ",")
	p, err := xsql.NewParser(strings.NewReader(stmt)).Parse()
	if err != nil {
		return nil, fmt.Errorf("invalid join statement error: %v", err)
	}
	return &operator.AggregateOp{Dimensions: p.Dimensions}, nil
}

func parseJoin(props map[string]interface{}) (*operator.JoinOp, error) {
	n := &graph.Join{}
	err := cast.MapToStruct(props, n)
	if err != nil {
		return nil, err
	}
	stmt := "SELECT * FROM " + n.From
	for _, join := range n.Joins {
		stmt += " " + join.Type + " JOIN ON " + join.On
	}
	p, err := xsql.NewParser(strings.NewReader(stmt)).Parse()
	if err != nil {
		return nil, fmt.Errorf("invalid join statement error: %v", err)
	}
	return &operator.JoinOp{Joins: p.Joins, From: p.Sources[0].(*ast.Table)}, nil
}

func parseWindow(props map[string]interface{}) (*node.WindowConfig, error) {
	n := &graph.Window{}
	err := cast.MapToStruct(props, n)
	if err != nil {
		return nil, err
	}
	if n.Size <= 0 {
		return nil, fmt.Errorf("window size %d is invalid", n.Size)
	}
	var (
		wt       ast.WindowType
		length   int
		interval int
	)
	switch strings.ToLower(n.Type) {
	case "tumblingwindow":
		wt = ast.TUMBLING_WINDOW
		if n.Interval != 0 && n.Interval != n.Size {
			return nil, fmt.Errorf("tumbling window interval must equal to size")
		}
	case "hoppingwindow":
		wt = ast.HOPPING_WINDOW
		if n.Interval <= 0 {
			return nil, fmt.Errorf("hopping window interval must be greater than 0")
		}
		if n.Interval > n.Size {
			return nil, fmt.Errorf("hopping window interval must be less than size")
		}
	case "sessionwindow":
		wt = ast.SESSION_WINDOW
		if n.Interval <= 0 {
			return nil, fmt.Errorf("hopping window interval must be greater than 0")
		}
	case "slidingwindow":
		wt = ast.SLIDING_WINDOW
		if n.Interval != 0 && n.Interval != n.Size {
			return nil, fmt.Errorf("tumbling window interval must equal to size")
		}
	case "countwindow":
		wt = ast.COUNT_WINDOW
		if n.Interval < 0 {
			return nil, fmt.Errorf("count window interval must be greater or equal to 0")
		}
		if n.Interval > n.Size {
			return nil, fmt.Errorf("count window interval must be less than size")
		}
		if n.Interval == 0 {
			n.Interval = n.Size
		}
	default:
		return nil, fmt.Errorf("unknown window type %s", n.Type)
	}
	if wt == ast.COUNT_WINDOW {
		length = n.Size
		interval = n.Interval
	} else {
		var unit = 1
		switch strings.ToLower(n.Unit) {
		case "dd":
			unit = 24 * 3600 * 1000
		case "hh":
			unit = 3600 * 1000
		case "mi":
			unit = 60 * 1000
		case "ss":
			unit = 1000
		case "ms":
			unit = 1
		default:
			return nil, fmt.Errorf("Invalid unit %s", n.Unit)
		}
		length = n.Size * unit
		interval = n.Interval * unit
	}

	return &node.WindowConfig{
		Type:     wt,
		Length:   length,
		Interval: interval,
	}, nil
}

func parsePick(props map[string]interface{}) (*operator.ProjectOp, error) {
	n := &graph.Select{}
	err := cast.MapToStruct(props, n)
	if err != nil {
		return nil, err
	}
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
		return nil, fmt.Errorf("expr %s is not ast.Call", funcExpr)
	}
	var name string
	if f.AName != "" {
		name = f.AName
	} else {
		name = f.Name
	}
	return &operator.FuncOp{CallExpr: c, Name: name, IsAgg: function.IsAggFunc(name)}, nil
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

func parseHaving(props map[string]interface{}) (*operator.HavingOp, error) {
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
			return &operator.HavingOp{Condition: exp}, nil
		}
	}
	return nil, fmt.Errorf("expr %v is not a condition", m)
}

func parseSwitch(props map[string]interface{}) (*node.SwitchConfig, error) {
	n := &graph.Switch{}
	err := cast.MapToStruct(props, n)
	if err != nil {
		return nil, err
	}
	if len(n.Cases) == 0 {
		return nil, fmt.Errorf("switch node must have at least one case")
	}
	caseExprs := make([]ast.Expr, len(n.Cases))
	for i, c := range n.Cases {
		p := xsql.NewParser(strings.NewReader("where " + c))
		if exp, err := p.ParseCondition(); err != nil {
			return nil, fmt.Errorf("parse case %d error: %v", i, err)
		} else {
			if exp != nil {
				caseExprs[i] = exp
			}
		}
	}
	return &node.SwitchConfig{
		Cases:            caseExprs,
		StopAtFirstMatch: n.StopAtFirstMatch,
	}, nil
}

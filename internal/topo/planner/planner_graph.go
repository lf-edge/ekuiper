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
	"strings"

	"github.com/lf-edge/ekuiper/internal/binder/function"
	store2 "github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/topo"
	"github.com/lf-edge/ekuiper/internal/topo/graph"
	"github.com/lf-edge/ekuiper/internal/topo/node"
	"github.com/lf-edge/ekuiper/internal/topo/operator"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/kv"
	"github.com/lf-edge/ekuiper/pkg/message"
)

type genNodeFunc func(name string, props map[string]interface{}, options *api.RuleOption) (api.TopNode, error)

var extNodes = map[string]genNodeFunc{}

type sourceType int

const (
	ILLEGAL sourceType = iota
	STREAM
	SCANTABLE
	LOOKUPTABLE
)

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
		nodeMap             = make(map[string]api.TopNode)
		sinks               = make(map[string]bool)
		sources             = make(map[string]bool)
		store               kv.KeyValue
		lookupTableChildren = make(map[string]*ast.Options)
		scanTableEmitters   []string
		sourceNames         []string
		streamEmitters      = make(map[string]struct{})
	)
	for _, srcName := range ruleGraph.Topo.Sources {
		gn, ok := ruleGraph.Nodes[srcName]
		if !ok {
			return nil, fmt.Errorf("source node %s not defined", srcName)
		}
		if _, ok := ruleGraph.Topo.Edges[srcName]; !ok {
			return nil, fmt.Errorf("no edge defined for source node %s", srcName)
		}
		srcNode, srcType, name, err := parseSource(srcName, gn, rule, store, lookupTableChildren)
		if err != nil {
			return nil, fmt.Errorf("parse source %s with %v error: %w", srcName, gn.Props, err)
		}
		switch srcType {
		case STREAM:
			streamEmitters[name] = struct{}{}
			sourceNames = append(sourceNames, name)
		case SCANTABLE:
			scanTableEmitters = append(scanTableEmitters, name)
			sourceNames = append(sourceNames, name)
		case LOOKUPTABLE:
			sourceNames = append(sourceNames, name)
		}
		if srcNode != nil {
			nodeMap[srcName] = srcNode
			tp.AddSrc(srcNode)
		}
		sources[srcName] = true
	}
	for nodeName, gn := range ruleGraph.Nodes {
		switch gn.Type {
		case "source": // handled above,
			continue
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
			case "watermark":
				n, err := parseWatermark(gn.Props, streamEmitters)
				if err != nil {
					return nil, fmt.Errorf("parse watermark %s with %v error: %w", nodeName, gn.Props, err)
				}
				op := node.NewWatermarkOp(nodeName, n.SendWatermark, n.Emitters, rule.Options)
				nodeMap[nodeName] = op
			case "function":
				fop, err := parseFunc(gn.Props, sourceNames)
				if err != nil {
					return nil, fmt.Errorf("parse function %s with %v error: %w", nodeName, gn.Props, err)
				}
				op := Transform(fop, nodeName, rule.Options)
				nodeMap[nodeName] = op
			case "aggfunc":
				fop, err := parseFunc(gn.Props, sourceNames)
				if err != nil {
					return nil, fmt.Errorf("parse aggfunc %s with %v error: %w", nodeName, gn.Props, err)
				}
				fop.IsAgg = true
				op := Transform(fop, nodeName, rule.Options)
				nodeMap[nodeName] = op
			case "filter":
				fop, err := parseFilter(gn.Props, sourceNames)
				if err != nil {
					return nil, fmt.Errorf("parse filter %s with %v error: %w", nodeName, gn.Props, err)
				}
				op := Transform(fop, nodeName, rule.Options)
				nodeMap[nodeName] = op
			case "pick":
				pop, err := parsePick(gn.Props, sourceNames)
				if err != nil {
					return nil, fmt.Errorf("parse pick %s with %v error: %w", nodeName, gn.Props, err)
				}
				op := Transform(pop, nodeName, rule.Options)
				nodeMap[nodeName] = op
			case "window":
				wconf, err := parseWindow(gn.Props)
				if err != nil {
					return nil, fmt.Errorf("parse window conf %s with %v error: %w", nodeName, gn.Props, err)
				}
				op, err := node.NewWindowOp(nodeName, *wconf, rule.Options)
				if err != nil {
					return nil, fmt.Errorf("parse window %s with %v error: %w", nodeName, gn.Props, err)
				}
				nodeMap[nodeName] = op
			case "join":
				stmt, err := parseJoinAst(gn.Props, sourceNames)
				if err != nil {
					return nil, fmt.Errorf("parse join %s with %v error: %w", nodeName, gn.Props, err)
				}
				fromNode := stmt.Sources[0].(*ast.Table)
				if _, ok := streamEmitters[fromNode.Name]; !ok {
					return nil, fmt.Errorf("parse join %s with %v error: join source %s is not a stream", nodeName, gn.Props, fromNode.Name)
				}
				hasLookup := false
				if stmt.Joins != nil {
					if len(lookupTableChildren) > 0 {
						var joins []ast.Join
						for _, join := range stmt.Joins {
							if hasLookup {
								return nil, fmt.Errorf("parse join %s with %v error: only support to join one lookup table with one stream", nodeName, gn.Props)
							}
							if streamOpt, ok := lookupTableChildren[join.Name]; ok {
								hasLookup = true
								lookupPlan := LookupPlan{
									joinExpr: join,
									options:  streamOpt,
								}
								if !lookupPlan.validateAndExtractCondition() {
									return nil, fmt.Errorf("parse join %s with %v error: join condition %s is invalid, at least one equi-join predicate is required", nodeName, gn.Props, join.Expr)
								}
								op, err := node.NewLookupNode(lookupPlan.joinExpr.Name, lookupPlan.fields, lookupPlan.keys, lookupPlan.joinExpr.JoinType, lookupPlan.valvars, lookupPlan.options, rule.Options)
								if err != nil {
									return nil, fmt.Errorf("parse join %s with %v error: fail to create lookup node", nodeName, gn.Props)
								}
								nodeMap[nodeName] = op
							} else {
								joins = append(joins, join)
							}
						}
						stmt.Joins = joins
					}
					// Not all joins are lookup joins, so we need to create a join plan for the remaining joins
					if len(stmt.Joins) > 0 && !hasLookup {
						if len(scanTableEmitters) > 0 {
							return nil, fmt.Errorf("parse join %s with %v error: do not support scan table %s yet", nodeName, gn.Props, scanTableEmitters)
						}
						jop := &operator.JoinOp{Joins: stmt.Joins, From: fromNode}
						op := Transform(jop, nodeName, rule.Options)
						nodeMap[nodeName] = op
					}
				}
			case "groupby":
				gop, err := parseGroupBy(gn.Props, sourceNames)
				if err != nil {
					return nil, fmt.Errorf("parse groupby %s with %v error: %w", nodeName, gn.Props, err)
				}
				op := Transform(gop, nodeName, rule.Options)
				nodeMap[nodeName] = op
			case "orderby":
				oop, err := parseOrderBy(gn.Props, sourceNames)
				if err != nil {
					return nil, fmt.Errorf("parse orderby %s with %v error: %w", nodeName, gn.Props, err)
				}
				op := Transform(oop, nodeName, rule.Options)
				nodeMap[nodeName] = op
			case "switch":
				sconf, err := parseSwitch(gn.Props, sourceNames)
				if err != nil {
					return nil, fmt.Errorf("parse switch %s with %v error: %w", nodeName, gn.Props, err)
				}
				op, err := node.NewSwitchNode(nodeName, sconf, rule.Options)
				if err != nil {
					return nil, fmt.Errorf("create switch %s with %v error: %w", nodeName, gn.Props, err)
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
				return nil, fmt.Errorf("can't find the io definition for node type %s", gn.NodeType)
			}
			dataInCondition := nodeIO[0]
			indim := reversedEdges[n]
			var innodes []string
			for _, in := range indim {
				innodes = append(innodes, in...)
			}
			if len(innodes) > 1 {
				if dataInCondition.AllowMulti {
					// special case for join which does not allow multiple streams
					if gn.NodeType == "join" {
						joinStreams := 0
						for _, innode := range innodes {
							if _, isLookup := lookupTableChildren[innode]; !isLookup {
								joinStreams++
							}
							if joinStreams > 1 {
								return nil, fmt.Errorf("join node %s does not allow multiple stream inputs", n)
							}
						}
					}
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
				fop, err := parseHaving(gn.Props, sourceNames)
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
					if src, ok := nodeMap[from].(api.Emitter); ok {
						inputs = append(inputs, src)
					}
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

func parseSource(nodeName string, gn *api.GraphNode, rule *api.Rule, store kv.KeyValue, lookupTableChildren map[string]*ast.Options) (*node.SourceNode, sourceType, string, error) {
	sourceMeta := &api.SourceMeta{
		SourceType: "stream",
	}
	err := cast.MapToStruct(gn.Props, sourceMeta)
	if err != nil {
		return nil, ILLEGAL, "", err
	}
	if sourceMeta.SourceType != "stream" && sourceMeta.SourceType != "table" {
		return nil, ILLEGAL, "", fmt.Errorf("source type %s not supported", sourceMeta.SourceType)
	}
	// If source name is specified, find the created stream/table from store
	if sourceMeta.SourceName != "" {
		if store == nil {
			store, err = store2.GetKV("stream")
			if err != nil {
				return nil, ILLEGAL, "", err
			}
		}
		streamStmt, e := xsql.GetDataSource(store, sourceMeta.SourceName)
		if e != nil {
			return nil, ILLEGAL, "", fmt.Errorf("fail to get stream %s, please check if stream is created", sourceMeta.SourceName)
		}
		if streamStmt.StreamType == ast.TypeStream && sourceMeta.SourceType == "table" {
			return nil, ILLEGAL, "", fmt.Errorf("stream %s is not a table", sourceMeta.SourceName)
		} else if streamStmt.StreamType == ast.TypeTable && sourceMeta.SourceType == "stream" {
			return nil, ILLEGAL, "", fmt.Errorf("table %s is not a stream", sourceMeta.SourceName)
		}
		st := streamStmt.Options.TYPE
		if st == "" {
			st = "mqtt"
		}
		if st != gn.NodeType {
			return nil, ILLEGAL, "", fmt.Errorf("source type %s does not match the stream type %s", gn.NodeType, st)
		}
		sInfo, err := convertStreamInfo(streamStmt)
		if err != nil {
			return nil, ILLEGAL, "", err
		}
		if sInfo.stmt.StreamType == ast.TypeTable && sInfo.stmt.Options.KIND == ast.StreamKindLookup {
			lookupTableChildren[string(sInfo.stmt.Name)] = sInfo.stmt.Options
			return nil, LOOKUPTABLE, string(sInfo.stmt.Name), nil
		} else {
			// Use the plan to calculate the schema and other meta info
			p := DataSourcePlan{
				name:         sInfo.stmt.Name,
				streamStmt:   sInfo.stmt,
				streamFields: sInfo.schema.ToJsonSchema(),
				isSchemaless: sInfo.schema == nil,
				iet:          rule.Options.IsEventTime,
				allMeta:      rule.Options.SendMetaToSink,
			}.Init()

			if sInfo.stmt.StreamType == ast.TypeStream {
				err = p.PruneColumns(nil)
				if err != nil {
					return nil, ILLEGAL, "", err
				}
				srcNode, e := transformSourceNode(p, nil, rule.Options)
				if e != nil {
					return nil, ILLEGAL, "", e
				}
				return srcNode, STREAM, string(sInfo.stmt.Name), nil
			} else {
				return nil, SCANTABLE, string(sInfo.stmt.Name), nil
			}
		}
	} else {
		sourceOption := &ast.Options{}
		err = cast.MapToStruct(gn.Props, sourceOption)
		if err != nil {
			return nil, ILLEGAL, "", err
		}
		sourceOption.TYPE = gn.NodeType
		switch sourceMeta.SourceType {
		case "stream":
			pp, err := operator.NewPreprocessor(true, nil, true, nil, rule.Options.IsEventTime, sourceOption.TIMESTAMP, sourceOption.TIMESTAMP_FORMAT, strings.EqualFold(sourceOption.FORMAT, message.FormatBinary), sourceOption.STRICT_VALIDATION)
			if err != nil {
				return nil, ILLEGAL, "", err
			}
			srcNode := node.NewSourceNode(nodeName, ast.TypeStream, pp, sourceOption, rule.Options.SendError, nil)
			return srcNode, STREAM, nodeName, nil
		case "table":
			return nil, ILLEGAL, "", fmt.Errorf("anonymouse table source is not supported, please create it prior to the rule")
		}
	}
	return nil, ILLEGAL, "", errors.New("invalid source node")
}

func parseOrderBy(props map[string]interface{}, sourceNames []string) (*operator.OrderOp, error) {
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
	p, err := xsql.NewParserWithSources(strings.NewReader(stmt), sourceNames).Parse()
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

func parseGroupBy(props map[string]interface{}, sourceNames []string) (*operator.AggregateOp, error) {
	n := &graph.Groupby{}
	err := cast.MapToStruct(props, n)
	if err != nil {
		return nil, err
	}
	if len(n.Dimensions) == 0 {
		return nil, fmt.Errorf("groupby must have at least one dimension")
	}
	stmt := "SELECT * FROM unknown Group By " + strings.Join(n.Dimensions, ",")
	p, err := xsql.NewParserWithSources(strings.NewReader(stmt), sourceNames).Parse()
	if err != nil {
		return nil, fmt.Errorf("invalid join statement error: %v", err)
	}
	return &operator.AggregateOp{Dimensions: p.Dimensions}, nil
}

func parseJoinAst(props map[string]interface{}, sourceNames []string) (*ast.SelectStatement, error) {
	n := &graph.Join{}
	err := cast.MapToStruct(props, n)
	if err != nil {
		return nil, err
	}
	stmt := "SELECT * FROM " + n.From
	for _, join := range n.Joins {
		stmt += " " + join.Type + " JOIN " + join.Name + " ON " + join.On
	}
	return xsql.NewParserWithSources(strings.NewReader(stmt), sourceNames).Parse()
}

func parseWatermark(props map[string]interface{}, streamEmitters map[string]struct{}) (*graph.Watermark, error) {
	n := &graph.Watermark{}
	err := cast.MapToStruct(props, n)
	if err != nil {
		return nil, err
	}
	if len(n.Emitters) == 0 {
		return nil, fmt.Errorf("watermark must have at least one emitter")
	}
	for _, e := range n.Emitters {
		if _, ok := streamEmitters[e]; !ok {
			return nil, fmt.Errorf("emitter %s does not exist", e)
		}
	}
	return n, nil
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
		wt          ast.WindowType
		length      int
		interval    int
		rawInterval int
	)
	switch strings.ToLower(n.Type) {
	case "tumblingwindow":
		wt = ast.TUMBLING_WINDOW
		if n.Interval != 0 && n.Interval != n.Size {
			return nil, fmt.Errorf("tumbling window interval must equal to size")
		}
		rawInterval = n.Size
	case "hoppingwindow":
		wt = ast.HOPPING_WINDOW
		if n.Interval <= 0 {
			return nil, fmt.Errorf("hopping window interval must be greater than 0")
		}
		if n.Interval > n.Size {
			return nil, fmt.Errorf("hopping window interval must be less than size")
		}
		rawInterval = n.Interval
	case "sessionwindow":
		wt = ast.SESSION_WINDOW
		if n.Interval <= 0 {
			return nil, fmt.Errorf("hopping window interval must be greater than 0")
		}
		rawInterval = n.Size
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
	var timeUnit ast.Token
	if wt == ast.COUNT_WINDOW {
		length = n.Size
		interval = n.Interval
	} else {
		unit := 1
		switch strings.ToLower(n.Unit) {
		case "dd":
			unit = 24 * 3600 * 1000
			timeUnit = ast.DD
		case "hh":
			unit = 3600 * 1000
			timeUnit = ast.HH
		case "mi":
			unit = 60 * 1000
			timeUnit = ast.MI
		case "ss":
			unit = 1000
			timeUnit = ast.SS
		case "ms":
			unit = 1
			timeUnit = ast.MS
		default:
			return nil, fmt.Errorf("Invalid unit %s", n.Unit)
		}
		length = n.Size * unit
		interval = n.Interval * unit
	}
	return &node.WindowConfig{
		RawInterval: rawInterval,
		Type:        wt,
		Length:      int64(length),
		Interval:    int64(interval),
		TimeUnit:    timeUnit,
	}, nil
}

func parsePick(props map[string]interface{}, sourceNames []string) (*operator.ProjectOp, error) {
	n := &graph.Select{}
	err := cast.MapToStruct(props, n)
	if err != nil {
		return nil, err
	}
	stmt, err := xsql.NewParserWithSources(strings.NewReader("select "+strings.Join(n.Fields, ",")+" from nonexist"), sourceNames).Parse()
	if err != nil {
		return nil, err
	}
	t := ProjectPlan{
		fields:      stmt.Fields,
		isAggregate: xsql.IsAggStatement(stmt),
	}.Init()
	return &operator.ProjectOp{ColNames: t.colNames, AliasNames: t.aliasNames, AliasFields: t.aliasFields, ExprFields: t.exprFields, IsAggregate: t.isAggregate, AllWildcard: t.allWildcard, WildcardEmitters: t.wildcardEmitters, ExprNames: t.exprNames, SendMeta: t.sendMeta}, nil
}

func parseFunc(props map[string]interface{}, sourceNames []string) (*operator.FuncOp, error) {
	m, ok := props["expr"]
	if !ok {
		return nil, errors.New("no expr")
	}
	funcExpr, ok := m.(string)
	if !ok {
		return nil, fmt.Errorf("expr %v is not string", m)
	}
	stmt, err := xsql.NewParserWithSources(strings.NewReader("select "+funcExpr+" from nonexist"), sourceNames).Parse()
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

func parseFilter(props map[string]interface{}, sourceNames []string) (*operator.FilterOp, error) {
	m, ok := props["expr"]
	if !ok {
		return nil, errors.New("no expr")
	}
	conditionExpr, ok := m.(string)
	if !ok {
		return nil, fmt.Errorf("expr %v is not string", m)
	}
	p := xsql.NewParserWithSources(strings.NewReader("where "+conditionExpr), sourceNames)
	if exp, err := p.ParseCondition(); err != nil {
		return nil, err
	} else {
		if exp != nil {
			return &operator.FilterOp{Condition: exp}, nil
		}
	}
	return nil, fmt.Errorf("expr %v is not a condition", m)
}

func parseHaving(props map[string]interface{}, sourceNames []string) (*operator.HavingOp, error) {
	m, ok := props["expr"]
	if !ok {
		return nil, errors.New("no expr")
	}
	conditionExpr, ok := m.(string)
	if !ok {
		return nil, fmt.Errorf("expr %v is not string", m)
	}
	p := xsql.NewParserWithSources(strings.NewReader("where "+conditionExpr), sourceNames)
	if exp, err := p.ParseCondition(); err != nil {
		return nil, err
	} else {
		if exp != nil {
			return &operator.HavingOp{Condition: exp}, nil
		}
	}
	return nil, fmt.Errorf("expr %v is not a condition", m)
}

func parseSwitch(props map[string]interface{}, sourceNames []string) (*node.SwitchConfig, error) {
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
		p := xsql.NewParserWithSources(strings.NewReader("where "+c), sourceNames)
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

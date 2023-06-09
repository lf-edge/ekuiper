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

	"github.com/lf-edge/ekuiper/internal/conf"
	store2 "github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/topo"
	"github.com/lf-edge/ekuiper/internal/topo/node"
	"github.com/lf-edge/ekuiper/internal/topo/operator"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/kv"
)

func Plan(rule *api.Rule) (*topo.Topo, error) {
	if rule.Sql != "" {
		return PlanSQLWithSourcesAndSinks(rule, nil, nil)
	} else {
		return PlanByGraph(rule)
	}
}

// PlanSQLWithSourcesAndSinks For test only
func PlanSQLWithSourcesAndSinks(rule *api.Rule, sources []*node.SourceNode, sinks []*node.SinkNode) (*topo.Topo, error) {
	sql := rule.Sql

	conf.Log.Infof("Init rule with options %+v", rule.Options)
	stmt, err := xsql.GetStatementFromSql(sql)
	if err != nil {
		return nil, err
	}
	// validation
	streamsFromStmt := xsql.GetStreams(stmt)
	//if len(sources) > 0 && len(sources) != len(streamsFromStmt) {
	//	return nil, fmt.Errorf("Invalid parameter sources or streams, the length cannot match the statement, expect %d sources.", len(streamsFromStmt))
	//}
	if rule.Options.SendMetaToSink && (len(streamsFromStmt) > 1 || stmt.Dimensions != nil) {
		return nil, fmt.Errorf("Invalid option sendMetaToSink, it can not be applied to window")
	}
	store, err := store2.GetKV("stream")
	if err != nil {
		return nil, err
	}
	// Create logical plan and optimize. Logical plans are a linked list
	lp, err := createLogicalPlan(stmt, rule.Options, store)
	if err != nil {
		return nil, err
	}
	tp, err := createTopo(rule, lp, sources, sinks, streamsFromStmt)
	if err != nil {
		return nil, err
	}
	return tp, nil
}

func createTopo(rule *api.Rule, lp LogicalPlan, sources []*node.SourceNode, sinks []*node.SinkNode, streamsFromStmt []string) (*topo.Topo, error) {
	// Create topology
	tp, err := topo.NewWithNameAndQos(rule.Id, rule.Options.Qos, rule.Options.CheckpointInterval)
	if err != nil {
		return nil, err
	}

	input, _, err := buildOps(lp, tp, rule.Options, sources, streamsFromStmt, 0)
	if err != nil {
		return nil, err
	}
	inputs := []api.Emitter{input}
	// Add actions
	if len(sinks) > 0 { // For use of mock sink in testing
		for _, sink := range sinks {
			tp.AddSink(inputs, sink)
		}
	} else {
		for i, m := range rule.Actions {
			for name, action := range m {
				props, ok := action.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("expect map[string]interface{} type for the action properties, but found %v", action)
				}
				tp.AddSink(inputs, node.NewSinkNode(fmt.Sprintf("%s_%d", name, i), name, props))
			}
		}
	}

	return tp, nil
}

func buildOps(lp LogicalPlan, tp *topo.Topo, options *api.RuleOption, sources []*node.SourceNode, streamsFromStmt []string, index int) (api.Emitter, int, error) {
	var inputs []api.Emitter
	newIndex := index
	for _, c := range lp.Children() {
		input, ni, err := buildOps(c, tp, options, sources, streamsFromStmt, newIndex)
		if err != nil {
			return nil, 0, err
		}
		newIndex = ni
		inputs = append(inputs, input)
	}
	newIndex++
	var (
		op  api.Emitter
		err error
	)
	switch t := lp.(type) {
	case *DataSourcePlan:
		srcNode, err := transformSourceNode(t, sources, options)
		if err != nil {
			return nil, 0, err
		}
		tp.AddSrc(srcNode)
		inputs = []api.Emitter{srcNode}
		op = srcNode
	case *AnalyticFuncsPlan:
		op = Transform(&operator.AnalyticFuncsOp{Funcs: t.funcs}, fmt.Sprintf("%d_analytic", newIndex), options)
	case *WindowPlan:
		if t.condition != nil {
			wfilterOp := Transform(&operator.FilterOp{Condition: t.condition}, fmt.Sprintf("%d_windowFilter", newIndex), options)
			wfilterOp.SetConcurrency(options.Concurrency)
			tp.AddOperator(inputs, wfilterOp)
			inputs = []api.Emitter{wfilterOp}
		}
		l, i := convertFromDuration(t)
		var rawInterval int
		switch t.wtype {
		case ast.TUMBLING_WINDOW, ast.SESSION_WINDOW:
			rawInterval = t.length
		case ast.HOPPING_WINDOW:
			rawInterval = t.interval
		}
		op, err = node.NewWindowOp(fmt.Sprintf("%d_window", newIndex), node.WindowConfig{
			Type:        t.wtype,
			Length:      l,
			Interval:    i,
			RawInterval: rawInterval,
			TimeUnit:    t.timeUnit,
		}, streamsFromStmt, options)
		if err != nil {
			return nil, 0, err
		}
	case *LookupPlan:
		op, err = node.NewLookupNode(t.joinExpr.Name, t.fields, t.keys, t.joinExpr.JoinType, t.valvars, t.options, options)
	case *JoinAlignPlan:
		op, err = node.NewJoinAlignNode(fmt.Sprintf("%d_join_aligner", newIndex), t.Emitters, options)
	case *JoinPlan:
		op = Transform(&operator.JoinOp{Joins: t.joins, From: t.from}, fmt.Sprintf("%d_join", newIndex), options)
	case *FilterPlan:
		op = Transform(&operator.FilterOp{Condition: t.condition}, fmt.Sprintf("%d_filter", newIndex), options)
	case *AggregatePlan:
		op = Transform(&operator.AggregateOp{Dimensions: t.dimensions}, fmt.Sprintf("%d_aggregate", newIndex), options)
	case *HavingPlan:
		op = Transform(&operator.HavingOp{Condition: t.condition}, fmt.Sprintf("%d_having", newIndex), options)
	case *OrderPlan:
		op = Transform(&operator.OrderOp{SortFields: t.SortFields}, fmt.Sprintf("%d_order", newIndex), options)
	case *ProjectPlan:
		op = Transform(&operator.ProjectOp{ColNames: t.colNames, AliasNames: t.aliasNames, AliasFields: t.aliasFields, ExprFields: t.exprFields, IsAggregate: t.isAggregate, AllWildcard: t.allWildcard, WildcardEmitters: t.wildcardEmitters, ExprNames: t.exprNames, SendMeta: t.sendMeta}, fmt.Sprintf("%d_project", newIndex), options)
	case *ProjectSetPlan:
		op = Transform(&operator.ProjectSetOperator{SrfMapping: t.SrfMapping}, fmt.Sprintf("%d_projectset", newIndex), options)
	default:
		err = fmt.Errorf("unknown logical plan %v", t)
	}
	if err != nil {
		return nil, 0, err
	}
	if uop, ok := op.(*node.UnaryOperator); ok {
		uop.SetConcurrency(options.Concurrency)
	}
	if onode, ok := op.(node.OperatorNode); ok {
		tp.AddOperator(inputs, onode)
	}
	return op, newIndex, nil
}

func convertFromDuration(t *WindowPlan) (int64, int64) {
	var unit int64 = 1
	switch t.timeUnit {
	case ast.DD:
		unit = 24 * 3600 * 1000
	case ast.HH:
		unit = 3600 * 1000
	case ast.MI:
		unit = 60 * 1000
	case ast.SS:
		unit = 1000
	case ast.MS:
		unit = 1
	}
	return int64(t.length) * unit, int64(t.interval) * unit
}

func transformSourceNode(t *DataSourcePlan, sources []*node.SourceNode, options *api.RuleOption) (*node.SourceNode, error) {
	isSchemaless := t.isSchemaless
	switch t.streamStmt.StreamType {
	case ast.TypeStream:
		var (
			pp  node.UnOperation
			err error
		)
		if t.iet || (!isSchemaless && (t.streamStmt.Options.STRICT_VALIDATION || t.isBinary)) {
			pp, err = operator.NewPreprocessor(isSchemaless, t.streamFields, t.allMeta, t.metaFields, t.iet, t.timestampField, t.timestampFormat, t.isBinary, t.streamStmt.Options.STRICT_VALIDATION)
			if err != nil {
				return nil, err
			}
		}
		var srcNode *node.SourceNode
		if len(sources) == 0 {
			var sourceNode *node.SourceNode
			schema := t.streamFields
			if t.isSchemaless {
				schema = nil
			}
			sourceNode = node.NewSourceNode(string(t.name), t.streamStmt.StreamType, pp, t.streamStmt.Options, options.SendError, schema)
			srcNode = sourceNode
		} else {
			srcNode = getMockSource(sources, string(t.name))
			if srcNode == nil {
				return nil, fmt.Errorf("can't find predefined source %s", t.name)
			}
		}
		return srcNode, nil
	case ast.TypeTable:
		pp, err := operator.NewTableProcessor(isSchemaless, string(t.name), t.streamFields, t.streamStmt.Options)
		if err != nil {
			return nil, err
		}
		var srcNode *node.SourceNode
		if len(sources) > 0 {
			srcNode = getMockSource(sources, string(t.name))
		}
		if srcNode == nil {
			schema := t.streamFields
			if t.isSchemaless {
				schema = nil
			}
			srcNode = node.NewSourceNode(string(t.name), t.streamStmt.StreamType, pp, t.streamStmt.Options, options.SendError, schema)
		}
		return srcNode, nil
	}
	return nil, fmt.Errorf("unknown stream type %d", t.streamStmt.StreamType)
}

func getMockSource(sources []*node.SourceNode, name string) *node.SourceNode {
	for _, source := range sources {
		if name == source.GetName() {
			return source
		}
	}
	return nil
}

func createLogicalPlan(stmt *ast.SelectStatement, opt *api.RuleOption, store kv.KeyValue) (LogicalPlan, error) {
	dimensions := stmt.Dimensions
	var (
		p        LogicalPlan
		children []LogicalPlan
		// If there are tables, the plan graph will be different for join/window
		lookupTableChildren map[string]*ast.Options
		scanTableChildren   []LogicalPlan
		scanTableEmitters   []string
		w                   *ast.Window
		ds                  ast.Dimensions
	)

	streamStmts, analyticFuncs, err := decorateStmt(stmt, store)
	if err != nil {
		return nil, err
	}

	for _, sInfo := range streamStmts {
		if sInfo.stmt.StreamType == ast.TypeTable && sInfo.stmt.Options.KIND == ast.StreamKindLookup {
			if lookupTableChildren == nil {
				lookupTableChildren = make(map[string]*ast.Options)
			}
			lookupTableChildren[string(sInfo.stmt.Name)] = sInfo.stmt.Options
		} else {
			p = DataSourcePlan{
				name:         sInfo.stmt.Name,
				streamStmt:   sInfo.stmt,
				streamFields: sInfo.schema.ToJsonSchema(),
				isSchemaless: sInfo.schema == nil,
				iet:          opt.IsEventTime,
				allMeta:      opt.SendMetaToSink,
			}.Init()
			if sInfo.stmt.StreamType == ast.TypeStream {
				children = append(children, p)
			} else {
				scanTableChildren = append(scanTableChildren, p)
				scanTableEmitters = append(scanTableEmitters, string(sInfo.stmt.Name))
			}
		}
	}
	if len(analyticFuncs) > 0 {
		p = AnalyticFuncsPlan{
			funcs: analyticFuncs,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}
	if dimensions != nil {
		w = dimensions.GetWindow()
		if w != nil {
			if len(children) == 0 {
				return nil, errors.New("cannot run window for TABLE sources")
			}
			wp := WindowPlan{
				wtype:       w.WindowType,
				length:      w.Length.Val,
				isEventTime: opt.IsEventTime,
			}.Init()
			if w.Interval != nil {
				wp.interval = w.Interval.Val
			} else if w.WindowType == ast.COUNT_WINDOW {
				// if no interval value is set, and it's a count window, then set interval to length value.
				wp.interval = w.Length.Val
			}
			if w.TimeUnit != nil {
				wp.timeUnit = w.TimeUnit.Val
			}
			if w.Filter != nil {
				wp.condition = w.Filter
			}
			// TODO calculate limit
			// TODO incremental aggregate
			wp.SetChildren(children)
			children = []LogicalPlan{wp}
			p = wp
		}
	}
	if stmt.Joins != nil {
		if len(lookupTableChildren) == 0 && len(scanTableChildren) == 0 && w == nil {
			return nil, errors.New("a time window or count window is required to join multiple streams")
		}
		if len(lookupTableChildren) > 0 {
			var joins []ast.Join
			for _, join := range stmt.Joins {
				if streamOpt, ok := lookupTableChildren[join.Name]; ok {
					lookupPlan := LookupPlan{
						joinExpr: join,
						options:  streamOpt,
					}
					if !lookupPlan.validateAndExtractCondition() {
						return nil, fmt.Errorf("join condition %s is invalid, at least one equi-join predicate is required", join.Expr)
					}
					p = lookupPlan.Init()
					p.SetChildren(children)
					children = []LogicalPlan{p}
					delete(lookupTableChildren, join.Name)
				} else {
					joins = append(joins, join)
				}
			}
			if len(lookupTableChildren) > 0 {
				return nil, fmt.Errorf("cannot find lookup table %v in any join", lookupTableChildren)
			}
			stmt.Joins = joins
		}
		// Not all joins are lookup joins, so we need to create a join plan for the remaining joins
		if len(stmt.Joins) > 0 {
			if len(scanTableChildren) > 0 {
				p = JoinAlignPlan{
					Emitters: scanTableEmitters,
				}.Init()
				p.SetChildren(append(children, scanTableChildren...))
				children = []LogicalPlan{p}
			}
			// TODO extract on filter
			p = JoinPlan{
				from:  stmt.Sources[0].(*ast.Table),
				joins: stmt.Joins,
			}.Init()
			p.SetChildren(children)
			children = []LogicalPlan{p}
		}
	}
	if stmt.Condition != nil {
		p = FilterPlan{
			condition: stmt.Condition,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}
	if dimensions != nil {
		ds = dimensions.GetGroups()
		if ds != nil && len(ds) > 0 {
			p = AggregatePlan{
				dimensions: ds,
			}.Init()
			p.SetChildren(children)
			children = []LogicalPlan{p}
		}
	}

	if stmt.Having != nil {
		p = HavingPlan{
			condition: stmt.Having,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}

	if stmt.SortFields != nil {
		p = OrderPlan{
			SortFields: stmt.SortFields,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}

	if stmt.Fields != nil {
		p = ProjectPlan{
			fields:      stmt.Fields,
			isAggregate: xsql.IsAggStatement(stmt),
			sendMeta:    opt.SendMetaToSink,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}

	srfMapping := extractSRFMapping(stmt)
	if len(srfMapping) > 0 {
		p = ProjectSetPlan{
			SrfMapping: srfMapping,
		}.Init()
		p.SetChildren(children)
	}

	return optimize(p)
}

// extractSRFMapping extracts the set-returning-function in the field
func extractSRFMapping(stmt *ast.SelectStatement) map[string]struct{} {
	m := make(map[string]struct{})
	for _, field := range stmt.Fields {
		var curExpr ast.Expr
		var name string
		if len(field.AName) > 0 {
			curExpr = field.Expr.(*ast.FieldRef).AliasRef.Expression
			name = field.AName
		} else {
			curExpr = field.Expr
			name = field.Name
		}
		if f, ok := curExpr.(*ast.Call); ok && f.FuncType == ast.FuncTypeSrf {
			m[name] = struct{}{}
		}
	}
	return m
}

func Transform(op node.UnOperation, name string, options *api.RuleOption) *node.UnaryOperator {
	unaryOperator := node.New(name, options)
	unaryOperator.SetOperation(op)
	return unaryOperator
}

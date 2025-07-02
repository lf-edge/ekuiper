// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/binder/function"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	store2 "github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/topo"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node"
	"github.com/lf-edge/ekuiper/v2/internal/topo/operator"
	"github.com/lf-edge/ekuiper/v2/internal/topo/schema"
	"github.com/lf-edge/ekuiper/v2/internal/xsql"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

func Plan(rule *def.Rule) (*topo.Topo, error) {
	if rule.Sql != "" {
		tp, _, err := PlanSQLWithSourcesAndSinks(rule, nil)
		return tp, err
	} else {
		return PlanByGraph(rule)
	}
}

// PlanSQLWithSourcesAndSinks For test only
func PlanSQLWithSourcesAndSinks(rule *def.Rule, mockSourcesProp map[string]map[string]any) (*topo.Topo, *ast.SelectStatement, error) {
	sql := rule.Sql
	if rule.Actions == nil {
		rule.Actions = []map[string]any{
			{
				"logToMemory": map[string]any{},
			},
		}
	}
	conf.Log.Infof("Init rule with options %+v", rule.Options)
	stmt, err := xsql.GetStatementFromSql(sql)
	if err != nil {
		return nil, stmt, err
	}
	// validation
	streamsFromStmt := xsql.GetStreams(stmt)
	// validate stmt
	if err := validateStmt(stmt); err != nil {
		return nil, stmt, err
	}
	//if len(sources) > 0 && len(sources) != len(streamsFromStmt) {
	//	return nil, fmt.Errorf("Invalid parameter sources or streams, the length cannot match the statement, expect %d sources.", len(streamsFromStmt))
	//}
	if rule.Options.SendMetaToSink && (len(streamsFromStmt) > 1 || stmt.Dimensions != nil) {
		return nil, stmt, fmt.Errorf("Invalid option sendMetaToSink, it can not be applied to window")
	}
	store, err := store2.GetKV("stream")
	if err != nil {
		return nil, stmt, err
	}
	// Create the logical plan and optimize. Logical plans are a linked list
	lp, af, aff, err := createLogicalPlanFull(stmt, rule.Options, store)
	if err != nil {
		return nil, stmt, err
	}
	tp, err := createTopo(rule, lp, mockSourcesProp, streamsFromStmt, getSinkSchema(stmt))
	if err != nil {
		return nil, stmt, err
	}
	if rule.Options.Experiment != nil && rule.Options.Experiment.UseSliceTuple {
		updateFieldIndex(tp.GetContext(), stmt, af, aff)
	}
	return tp, stmt, nil
}

func updateFieldIndex(ctx api.StreamContext, stmt *ast.SelectStatement, af []*ast.Call, aff []*ast.Call) {
	var (
		fieldExprs      []ast.Node
		invisibleFields []*ast.FieldRef
		aliasIndex      = make(map[string]int)
	)
	count := 0
	for _, f := range stmt.Fields {
		ast.WalkFunc(f.Expr, func(n ast.Node) bool {
			switch nf := n.(type) {
			case *ast.FieldRef:
				if nf.IsColumn() {
					sc := schema.GetStreamSchemaIndex(string(nf.StreamName))
					if sc != nil {
						if si, ok := sc[nf.Name]; ok {
							nf.SourceIndex = si
							ctx.GetLogger().Debugf("update field source index %s to %d", nf.Name, nf.SourceIndex)
						}
					} else {
						panic("field not found in schema index")
					}
				} else {
					nf.SourceIndex = -1
					if aindex, ok := aliasIndex[nf.Name]; ok && nf.IsAlias() {
						nf.Index = aindex
					} else {
						fieldExprs = append(fieldExprs, nf.Expression)
					}
				}
				if f.Invisible {
					invisibleFields = append(invisibleFields, nf)
				} else {
					nf.Index = count
					if nf.IsAlias() {
						aliasIndex[nf.Name] = nf.Index
					}
					ctx.GetLogger().Debugf("update field sink index %s to %d", nf.Name, nf.Index)
					count++
				}
			}
			return true
		})
	}
	for _, nf := range invisibleFields {
		nf.Index = count
		if nf.IsAlias() {
			aliasIndex[nf.Name] = nf.Index
		}
		ctx.GetLogger().Infof("update invisibe field sink index %s to %d", nf.Name, nf.Index)
		count++
	}
	index := len(stmt.Fields)
	for _, fieldExpr := range fieldExprs {
		index = doUpdateIndex(ctx, fieldExpr, index, aliasIndex)
	}
	// Add sink index for other non-select fields
	index = doUpdateIndex(ctx, stmt, index, aliasIndex)
	ctx.GetLogger().Infof("assign %d field index", index)
	// Set temp index for analytic funcs
	index = 0
	for i := range aff {
		aff[i].CacheIndex = index
		index++
	}
	for i := range af {
		af[i].CacheIndex = index
		index++
	}
	ctx.GetLogger().Infof("assign %d temp index", index)
}

func doUpdateIndex(ctx api.StreamContext, root ast.Node, index int, aliasIndex map[string]int) int {
	ast.WalkFunc(root, func(n ast.Node) bool {
		switch nf := n.(type) {
		case *ast.FieldRef:
			nf.HasIndex = true
			if nf.IsColumn() {
				sc := schema.GetStreamSchemaIndex(string(nf.StreamName))
				if sc != nil {
					if si, ok := sc[nf.Name]; ok {
						nf.SourceIndex = si
						ctx.GetLogger().Debugf("update field source index %s to %d", nf.Name, nf.SourceIndex)
					}
				} else {
					panic("field not found in schema index")
				}
			} else if nf.IsAlias() {
				ai, ok := aliasIndex[nf.Name]
				if !ok {
					panic(fmt.Sprintf("alias %s not found in schema index", nf.Name))
				}
				nf.SourceIndex = -1
				nf.Index = ai
			} else {
				nf.SourceIndex = -1
				if nf.Index < 0 {
					nf.Index = index
					index++
					ctx.GetLogger().Debugf("update field sink index %s to %d", nf.Name, nf.Index)
				}
			}
		}
		return true
	})
	return index
}

func getSinkSchema(stmt *ast.SelectStatement) map[string]*ast.JsonStreamField {
	s := make(map[string]*ast.JsonStreamField, len(stmt.Fields))
	i := 0
	for _, field := range stmt.Fields {
		if field.GetName() != "*" && !field.Invisible {
			s[field.GetName()] = &ast.JsonStreamField{Index: i, HasIndex: true}
			i++
		}
	}
	return s
}

func validateStmt(stmt *ast.SelectStatement) error {
	var vErr error
	ast.WalkFunc(stmt, func(n ast.Node) bool {
		if validateAbleExpr, ok := n.(ast.ValidateAbleExpr); ok {
			if err := validateAbleExpr.ValidateExpr(); err != nil {
				vErr = err
				return false
			}
		}
		return true
	})
	return vErr
}

func createTopo(rule *def.Rule, lp LogicalPlan, mockSourcesProp map[string]map[string]any, streamsFromStmt []string, schema map[string]*ast.JsonStreamField) (t *topo.Topo, err error) {
	defer func() {
		if err != nil {
			err = errorx.NewWithCode(errorx.ExecutorError, err.Error())
		}
	}()

	// Create topology
	tp, err := topo.NewWithNameAndOptions(rule.Id, rule.Options)
	if err != nil {
		return nil, err
	}
	tp.SetStreams(streamsFromStmt)

	input, _, err := buildOps(lp, tp, rule.Options, mockSourcesProp, streamsFromStmt, 0)
	if err != nil {
		return nil, err
	}
	inputs := []node.Emitter{input}
	// Add actions
	err = buildActions(tp, rule, inputs, len(streamsFromStmt), schema)
	if err != nil {
		return nil, err
	}

	return tp, nil
}

func GetExplainInfoFromLogicalPlan(rule *def.Rule) (string, error) {
	sql := rule.Sql

	conf.Log.Infof("Init rule with options %+v", rule.Options)
	stmt, err := xsql.GetStatementFromSql(sql)
	if err != nil {
		return "", err
	}
	// validation
	streamsFromStmt := xsql.GetStreams(stmt)

	if rule.Options.SendMetaToSink && (len(streamsFromStmt) > 1 || stmt.Dimensions != nil) {
		return "", fmt.Errorf("invalid option sendMetaToSink, it can not be applied to window")
	}
	store, err := store2.GetKV("stream")
	if err != nil {
		return "", err
	}
	// Create logical plan and optimize. Logical plans are a linked list
	lp, err := CreateLogicalPlan(stmt, rule.Options, store)
	if err != nil {
		return "", err
	}
	return ExplainFromLogicalPlan(lp, rule.Id)
}

func ExplainFromLogicalPlan(lp LogicalPlan, ruleID string) (string, error) {
	var setId func(p LogicalPlan, id int64)
	setId = func(p LogicalPlan, id int64) {
		p.SetID(id)
		children := p.Children()
		for i := 0; i < len(children); i++ {
			id++
			setId(children[i], id)
		}
	}
	setId(lp, 0)
	var getExplainInfo func(p LogicalPlan, level int) string
	getExplainInfo = func(p LogicalPlan, level int) string {
		tmp := ""
		res := ""
		for i := 0; i < level; i++ {
			tmp += "\t"
		}
		p.BuildExplainInfo()
		if info, ok := p.(RuleRuntimeInfo); ok {
			info.BuildSchemaInfo(ruleID)
		}
		// Build the explainInfo of the current layer
		res += tmp + strings.TrimSuffix(p.Explain(), "\n")
		if len(p.Children()) != 0 {
			res += "\n"
			for _, v := range p.Children() {
				res += tmp + getExplainInfo(v, level+1)
				res += "\n"
			}
		}
		return res
	}
	res := getExplainInfo(lp, 0)
	return strings.Trim(res, "\n"), nil
}

// return the last schema if there are multiple sources
func buildOps(lp LogicalPlan, tp *topo.Topo, options *def.RuleOption, sources map[string]map[string]any, streamsFromStmt []string, index int) (node.Emitter, int, error) {
	var inputs []node.Emitter
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
		op  node.Emitter
		err error
	)
	switch t := lp.(type) {
	case *DataSourcePlan:
		srcNode, emitters, indexInc, err := transformSourceNode(tp.GetContext(), t, sources, tp.GetName(), options, newIndex)
		if err != nil {
			return nil, 0, err
		}
		tp.AddSrc(srcNode)
		inputs = []node.Emitter{srcNode}
		op = srcNode
		if len(emitters) > 0 {
			for i, e := range emitters {
				if i < len(emitters)-1 {
					tp.AddOperator(inputs, e)
					inputs = []node.Emitter{e}
				}
				op = e
				newIndex++
			}
		} else {
			newIndex += indexInc
		}
	case *WatermarkPlan:
		op = node.NewWatermarkOp(fmt.Sprintf("%d_watermark", newIndex), t.SendWatermark, t.Emitters, options)
	case *AnalyticFuncsPlan:
		op = Transform(&operator.AnalyticFuncsOp{Funcs: t.funcs, FieldFuncs: t.fieldFuncs}, fmt.Sprintf("%d_analytic", newIndex), options)
	case *IncWindowPlan:
		if t.Condition != nil {
			wfilterOp := Transform(&operator.FilterOp{Condition: t.Condition}, fmt.Sprintf("%d_windowFilter", newIndex), options)
			tp.AddOperator(inputs, wfilterOp)
			inputs = []node.Emitter{wfilterOp}
		}
		l, i, d := convertFromDuration(t.TimeUnit, t.Length, t.Interval, t.Delay)
		var rawInterval int
		switch t.WType {
		case ast.TUMBLING_WINDOW, ast.SESSION_WINDOW:
			rawInterval = t.Length
		case ast.HOPPING_WINDOW:
			rawInterval = t.Interval
		}
		op, err = node.NewWindowIncAggOp(fmt.Sprintf("%d_inc_agg_window", newIndex), &node.WindowConfig{
			Type:             t.WType,
			Delay:            d,
			Length:           l,
			Interval:         i,
			RawInterval:      rawInterval,
			CountLength:      t.Length,
			TriggerCondition: t.TriggerCondition,
			TimeUnit:         t.TimeUnit,
		}, t.Dimensions, t.IncAggFuncs, options)
		if err != nil {
			return nil, 0, err
		}
	case *WindowPlan:
		if t.condition != nil {
			wfilterOp := Transform(&operator.FilterOp{Condition: t.condition}, fmt.Sprintf("%d_windowFilter", newIndex), options)
			tp.AddOperator(inputs, wfilterOp)
			inputs = []node.Emitter{wfilterOp}
		}
		l, i, d := convertFromDuration(t.timeUnit, t.length, t.interval, t.delay)
		var rawInterval int
		switch t.wtype {
		case ast.TUMBLING_WINDOW, ast.SESSION_WINDOW:
			rawInterval = t.length
		case ast.HOPPING_WINDOW:
			rawInterval = t.interval
		}
		t.ExtractStateFunc()
		wc := node.WindowConfig{
			Type:             t.wtype,
			Delay:            d,
			Length:           l,
			Interval:         i,
			CountInterval:    t.interval,
			CountLength:      t.length,
			RawInterval:      rawInterval,
			TimeUnit:         t.timeUnit,
			TriggerCondition: t.triggerCondition,
			BeginCondition:   t.beginCondition,
			EmitCondition:    t.emitCondition,
			StateFuncs:       t.stateFuncs,
		}
		if options.PlanOptimizeStrategy.GetWindowVersion() == "v2" {
			op, err = node.NewWindowV2Op(fmt.Sprintf("%d_window", newIndex), wc, options)
			if err != nil {
				return nil, 0, err
			}
		} else {
			op, err = node.NewWindowOp(fmt.Sprintf("%d_window", newIndex), wc, options)
			if err != nil {
				return nil, 0, err
			}
		}
	case *DedupTriggerPlan:
		op = node.NewDedupTriggerNode(fmt.Sprintf("%d_dedup_trigger", newIndex), options, t.aliasName, t.startField.Name, t.endField.Name, t.nowField.Name, t.expire)
	case *LookupPlan:
		op, err = planLookupSource(tp.GetContext(), t, options)
	case *JoinAlignPlan:
		op, err = node.NewJoinAlignNode(fmt.Sprintf("%d_join_aligner", newIndex), t.Emitters, t.Sizes, options)
	case *JoinPlan:
		op = Transform(&operator.JoinOp{Joins: t.joins, From: t.from}, fmt.Sprintf("%d_join", newIndex), options)
	case *FilterPlan:
		t.ExtractStateFunc()
		op = Transform(&operator.FilterOp{Condition: t.condition, StateFuncs: t.stateFuncs}, fmt.Sprintf("%d_filter", newIndex), options)
	case *AggregatePlan:
		op = Transform(&operator.AggregateOp{Dimensions: t.dimensions}, fmt.Sprintf("%d_aggregate", newIndex), options)
	case *HavingPlan:
		t.ExtractStateFunc()
		op = Transform((&operator.HavingOp{Condition: t.condition, StateFuncs: t.stateFuncs, IsIncAgg: t.IsIncAgg}), fmt.Sprintf("%d_having", newIndex), options)
	case *OrderPlan:
		op = Transform(&operator.OrderOp{SortFields: t.SortFields}, fmt.Sprintf("%d_order", newIndex), options)
	case *ProjectPlan:
		op = Transform(&operator.ProjectOp{Fields: t.fields, FieldLen: t.fieldLen, ColNames: t.colNames, AliasFields: t.aliasFields, ExprFields: t.exprFields, ExceptNames: t.exceptNames, IsAggregate: t.isAggregate, AllWildcard: t.allWildcard, WildcardEmitters: t.wildcardEmitters, SendMeta: t.sendMeta, SendNil: t.sendNil, LimitCount: t.limitCount, EnableLimit: t.enableLimit}, fmt.Sprintf("%d_project", newIndex), options)
	case *ProjectSetPlan:
		op = Transform(&operator.ProjectSetOperator{SrfMapping: t.SrfMapping, LimitCount: t.limitCount, EnableLimit: t.enableLimit}, fmt.Sprintf("%d_projectset", newIndex), options)
	case *WindowFuncPlan:
		op = Transform(&operator.WindowFuncOperator{WindowFuncField: t.windowFuncField}, fmt.Sprintf("%d_windowFunc", newIndex), options)
	default:
		err = fmt.Errorf("unknown logical plan %v", t)
	}
	if err != nil {
		return nil, 0, err
	}
	if onode, ok := op.(node.OperatorNode); ok {
		tp.AddOperator(inputs, onode)
	}
	return op, newIndex, nil
}

func convertFromDuration(timeUnit ast.Token, length, interval int, delay int64) (time.Duration, time.Duration, time.Duration) {
	var unit time.Duration
	switch timeUnit {
	case ast.DD:
		unit = 24 * time.Hour
	case ast.HH:
		unit = time.Hour
	case ast.MI:
		unit = time.Minute
	case ast.SS:
		unit = time.Second
	case ast.MS:
		unit = time.Millisecond
	}
	return time.Duration(length) * unit, time.Duration(interval) * unit, time.Duration(delay) * unit
}

func CreateLogicalPlan(stmt *ast.SelectStatement, opt *def.RuleOption, store kv.KeyValue) (LogicalPlan, error) {
	lp, _, _, err := createLogicalPlanFull(stmt, opt, store)
	return lp, err
}

func checkSharedSourceOption(streams []*streamInfo, opt *def.RuleOption) error {
	if !opt.DisableBufferFullDiscard {
		return nil
	}
	for _, stream := range streams {
		if stream.stmt.Options.SHARED {
			return fmt.Errorf("disableBufferFullDiscard can't be enabled with shared stream %v", stream.stmt.Name)
		}
	}
	return nil
}

func createLogicalPlanFull(stmt *ast.SelectStatement, opt *def.RuleOption, store kv.KeyValue) (LogicalPlan, []*ast.Call, []*ast.Call, error) {
	dimensions := stmt.Dimensions
	var (
		p        LogicalPlan
		children []LogicalPlan
		// If there are tables, the plan graph will be different for join/window
		lookupTableChildren map[string]*ast.Options
		scanTableChildren   []LogicalPlan
		scanTableEmitters   []string
		scanTableSizes      []int
		streamEmitters      []string
		w                   *ast.Window
		ds                  ast.Dimensions
	)

	streamStmts, analyticFuncs, analyticFieldFuncs, err := decorateStmt(stmt, store, opt)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := checkSharedSourceOption(streamStmts, opt); err != nil {
		return nil, nil, nil, err
	}

	rewriteRes := rewriteStmt(stmt, opt)

	for _, sInfo := range streamStmts {
		if sInfo.stmt.StreamType == ast.TypeTable && sInfo.stmt.Options.KIND == ast.StreamKindLookup {
			if opt.Experiment != nil && opt.Experiment.UseSliceTuple {
				return nil, nil, nil, fmt.Errorf("slice tuple mode do not support table yet %s", sInfo.stmt.Name)
			}
			if lookupTableChildren == nil {
				lookupTableChildren = make(map[string]*ast.Options)
			}
			lookupTableChildren[string(sInfo.stmt.Name)] = sInfo.stmt.Options
		} else {
			p = DataSourcePlan{
				name:            sInfo.stmt.Name,
				streamStmt:      sInfo.stmt,
				streamFields:    sInfo.schema.ToJsonSchema(),
				isSchemaless:    sInfo.schema == nil,
				iet:             opt.IsEventTime,
				allMeta:         opt.SendMetaToSink,
				colAliasMapping: rewriteRes.dsColAliasMapping[sInfo.stmt.Name],
				useSliceTuple:   opt.Experiment != nil && opt.Experiment.UseSliceTuple,
			}.Init()
			if sInfo.stmt.StreamType == ast.TypeStream {
				children = append(children, p)
				streamEmitters = append(streamEmitters, string(sInfo.stmt.Name))
			} else {
				scanTableChildren = append(scanTableChildren, p)
				scanTableEmitters = append(scanTableEmitters, string(sInfo.stmt.Name))
				tableSize := sInfo.stmt.Options.RETAIN_SIZE
				if tableSize == 0 {
					switch sInfo.stmt.Options.TYPE {
					// If retainSize is not set, file table will try to read all the content in it
					case "", "file":
						// TODO use interface to determine if the table is batch like file
						tableSize = MaxRetainSize
					default:
						tableSize = DefaultRetainSize
					}
				}
				scanTableSizes = append(scanTableSizes, tableSize)
			}
		}
	}
	hasWindow := dimensions != nil && dimensions.GetWindow() != nil
	if opt.IsEventTime {
		if opt.Experiment != nil && opt.Experiment.UseSliceTuple {
			return nil, nil, nil, errors.New("slice tuple mode do not support event time yet")
		}
		p = WatermarkPlan{
			SendWatermark: hasWindow,
			Emitters:      streamEmitters,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}
	if len(analyticFuncs) > 0 || len(analyticFieldFuncs) > 0 {
		p = AnalyticFuncsPlan{
			funcs:      analyticFuncs,
			fieldFuncs: analyticFieldFuncs,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}
	if dimensions != nil {
		//if opt.Experiment != nil && opt.Experiment.UseSliceTuple {
		//	return nil, nil, nil, errors.New("slice tuple mode do not support dimensions or window yet")
		//}
		w = dimensions.GetWindow()
		if w != nil {
			if len(children) == 0 {
				return nil, nil, nil, errors.New("cannot run window for TABLE sources")
			}
			if len(rewriteRes.incAggFields) > 0 {
				incWp := IncWindowPlan{
					WType:            w.WindowType,
					Length:           int(w.Length.Val),
					Dimensions:       dimensions.GetGroups(),
					IncAggFuncs:      rewriteRes.incAggFields,
					Condition:        w.Filter,
					TriggerCondition: w.TriggerCondition,
				}.Init()
				if w.Length != nil {
					incWp.Length = int(w.Length.Val)
				}
				if w.Interval != nil {
					incWp.Interval = int(w.Interval.Val)
				}
				if w.Delay != nil {
					incWp.Delay = w.Delay.Val
				}
				if w.TimeUnit != nil {
					incWp.TimeUnit = w.TimeUnit.Val
				}
				incWp = incWp.Init()
				incWp.SetChildren(children)
				children = []LogicalPlan{incWp}
				p = incWp
			} else {
				wp := WindowPlan{
					wtype:          w.WindowType,
					isEventTime:    opt.IsEventTime,
					beginCondition: w.BeginCondition,
					emitCondition:  w.EmitCondition,
				}.Init()
				if w.Length != nil {
					wp.length = int(w.Length.Val)
				}
				if w.Delay != nil {
					wp.delay = w.Delay.Val
				}
				if w.Interval != nil {
					wp.interval = int(w.Interval.Val)
				} else if w.WindowType == ast.COUNT_WINDOW {
					// if no interval value is set, and it's a count window, then set interval to length value.
					wp.interval = int(w.Length.Val)
				}
				if w.TimeUnit != nil {
					wp.timeUnit = w.TimeUnit.Val
				}
				if w.Filter != nil {
					wp.condition = w.Filter
				}
				if w.TriggerCondition != nil {
					wp.triggerCondition = w.TriggerCondition
				}
				// TODO calculate limit
				// TODO incremental aggregate
				wp.SetChildren(children)
				children = []LogicalPlan{wp}
				p = wp
			}
		}
	}
	if stmt.Joins != nil {
		if opt.Experiment != nil && opt.Experiment.UseSliceTuple {
			return nil, nil, nil, errors.New("slice tuple mode do not support join yet")
		}
		if len(lookupTableChildren) == 0 && len(scanTableChildren) == 0 && w == nil {
			return nil, nil, nil, errors.New("a time window or count window is required to join multiple streams")
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
						return nil, nil, nil, fmt.Errorf("join condition %s is invalid, at least one equi-join predicate is required", join.Expr)
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
				return nil, nil, nil, fmt.Errorf("cannot find lookup table %v in any join", lookupTableChildren)
			}
			stmt.Joins = joins
		}
		// Not all joins are lookup joins, so we need to create a join plan for the remaining joins
		if len(stmt.Joins) > 0 {
			if len(scanTableChildren) > 0 {
				p = JoinAlignPlan{
					Emitters: scanTableEmitters,
					Sizes:    scanTableSizes,
				}.Init()
				p.SetChildren(append(children, scanTableChildren...))
				children = []LogicalPlan{p}
			}
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
	if dimensions != nil && len(rewriteRes.incAggFields) < 1 {
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
		if opt.Experiment != nil && opt.Experiment.UseSliceTuple {
			return nil, nil, nil, errors.New("slice tuple mode do not support having yet")
		}
		p = HavingPlan{
			condition: stmt.Having,
			IsIncAgg:  len(rewriteRes.incAggFields) > 0,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}
	if len(rewriteRes.windowFuncFields) > 0 {
		for _, wf := range rewriteRes.windowFuncFields {
			p = WindowFuncPlan{
				windowFuncField: wf,
			}.Init()
			p.SetChildren(children)
			children = []LogicalPlan{p}
		}
	}
	if stmt.SortFields != nil {
		if opt.Experiment != nil && opt.Experiment.UseSliceTuple {
			return nil, nil, nil, errors.New("slice tuple mode do not support sort yet")
		}
		p = OrderPlan{
			SortFields: stmt.SortFields,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}
	srfMapping := extractSRFMapping(stmt)
	if stmt.Fields != nil {
		// extract dedup trigger op
		fields := make([]ast.Field, 0, len(stmt.Fields))
		fieldLen := 0
		for _, field := range stmt.Fields {
			if field.Expr != nil {
				var (
					exp  *ast.Expr
					name string
					fc   *ast.Call
				)
				if f, ok := field.Expr.(*ast.FieldRef); ok {
					if f.AliasRef != nil && f.AliasRef.Expression != nil {
						if wf, ok := f.AliasRef.Expression.(*ast.Call); ok && wf.FuncType == ast.FuncTypeTrigger {
							exp = &f.AliasRef.Expression
							name = field.AName
							fc = wf
						}
					}
				} else if f, ok := field.Expr.(*ast.Call); ok && f.FuncType == ast.FuncTypeTrigger {
					name = field.Name
					exp = &field.Expr
					fc = f
				}
				if exp != nil {
					p = DedupTriggerPlan{
						aliasName:  name,
						startField: fc.Args[0].(*ast.FieldRef),
						endField:   fc.Args[1].(*ast.FieldRef),
						nowField:   fc.Args[2].(*ast.FieldRef),
						expire:     fc.Args[3].(*ast.IntegerLiteral).Val,
					}.Init()
					p.SetChildren(children)
					children = []LogicalPlan{p}

					*exp = &ast.FieldRef{
						StreamName: ast.DefaultStream,
						Name:       name,
					}
				}
			}
			fields = append(fields, field)
			if opt.Experiment != nil && opt.Experiment.UseSliceTuple && !field.Invisible {
				fieldLen++
			}
		}
		enableLimit := false
		limitCount := 0
		if stmt.Limit != nil && len(srfMapping) == 0 {
			enableLimit = true
			limitCount = int(stmt.Limit.(*ast.LimitExpr).LimitCount.Val)
		}
		p = ProjectPlan{
			fields:      fields,
			fieldLen:    fieldLen,
			isAggregate: xsql.WithAggFields(stmt) && len(rewriteRes.incAggFields) < 1,
			sendMeta:    opt.SendMetaToSink,
			sendNil:     opt.SendNil,
			enableLimit: enableLimit,
			limitCount:  limitCount,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}

	if len(srfMapping) > 0 {
		if opt.Experiment != nil && opt.Experiment.UseSliceTuple {
			return nil, nil, nil, errors.New("slice tuple mode do not support project set yet")
		}
		enableLimit := false
		limitCount := 0
		if stmt.Limit != nil {
			enableLimit = true
			limitCount = int(stmt.Limit.(*ast.LimitExpr).LimitCount.Val)
		}
		p = ProjectSetPlan{
			SrfMapping:  srfMapping,
			enableLimit: enableLimit,
			limitCount:  limitCount,
		}.Init()
		p.SetChildren(children)
	}

	lp, err := optimize(p, opt)
	return lp, analyticFuncs, analyticFieldFuncs, err
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

func Transform(op node.UnOperation, name string, options *def.RuleOption) *node.UnaryOperator {
	unaryOperator := node.New(name, options)
	unaryOperator.SetOperation(op)
	return unaryOperator
}

func extractWindowFuncFields(stmt *ast.SelectStatement) []*ast.Field {
	windowFuncFields := make([]*ast.Field, 0)
	windowFunctionCount := 0
	ast.WalkFunc(stmt.Fields, func(n ast.Node) bool {
		switch wf := n.(type) {
		case *ast.Call:
			if wf.FuncType == ast.FuncTypeWindow {
				newWf := &ast.Call{
					Name:     wf.Name,
					FuncType: wf.FuncType,
					Args:     wf.Args,
				}
				windowFunctionCount++
				newName := fmt.Sprintf("wf_%s_%d", wf.Name, windowFunctionCount)
				newField := ast.Field{
					Name: newName,
					Expr: newWf,
				}
				newFieldRef := &ast.FieldRef{
					Name: newName,
				}
				windowFuncFields = append(windowFuncFields, &newField)
				rewriteIntoBypass(newFieldRef, wf)
			}
		}
		return true
	})
	return windowFuncFields
}

type rewriteResult struct {
	windowFuncFields  []*ast.Field
	incAggFields      []*ast.Field
	dsColAliasMapping map[ast.StreamName]map[string]string
}

// rewrite stmt will do following things:
// 1. extract and rewrite the window function
// 2. extract and rewrite the aggregation function
func rewriteStmt(stmt *ast.SelectStatement, opt *def.RuleOption) rewriteResult {
	result := rewriteResult{}
	result.windowFuncFields = extractWindowFuncFields(stmt)
	result.incAggFields = rewriteIfIncAggStmt(stmt, opt)
	result.dsColAliasMapping = rewriteIfPushdownAlias(stmt, opt)
	return result
}

func rewriteIfIncAggStmt(stmt *ast.SelectStatement, opt *def.RuleOption) []*ast.Field {
	if opt.PlanOptimizeStrategy == nil {
		return nil
	}
	if !opt.PlanOptimizeStrategy.EnableIncrementalWindow {
		return nil
	}
	if stmt.Dimensions == nil {
		return nil
	}
	if stmt.Dimensions.GetWindow() == nil {
		return nil
	}
	if !supportedWindowType(stmt.Dimensions.GetWindow()) {
		return nil
	}
	// TODO: support join later
	if stmt.Joins != nil {
		return nil
	}
	index := 0
	incAggFields, canIncAgg := extractNodeIncAgg(stmt.Fields, &index)
	if !canIncAgg {
		return nil
	}
	incAggHavingFields, _ := extractNodeIncAgg(stmt.Having, &index)
	return append(incAggFields, incAggHavingFields...)
}

func extractNodeIncAgg(node ast.Node, index *int) ([]*ast.Field, bool) {
	canIncAgg := true
	hasAgg := false
	ast.WalkFunc(node, func(n ast.Node) bool {
		switch f := n.(type) {
		case *ast.Call:
			if f.FuncType == ast.FuncTypeAgg {
				hasAgg = true
				if !function.IsSupportedIncAgg(f.Name) {
					canIncAgg = false
					return false
				}
			}
		}
		return true
	})
	if !hasAgg {
		return nil, false
	}
	if !canIncAgg {
		return nil, false
	}
	incAggFuncFields := make([]*ast.Field, 0)
	ast.WalkFunc(node, func(n ast.Node) bool {
		switch aggFunc := n.(type) {
		case *ast.Call:
			if aggFunc.FuncType == ast.FuncTypeAgg {
				if function.IsSupportedIncAgg(aggFunc.Name) {
					*index++
					newAggFunc := &ast.Call{
						Name:     fmt.Sprintf("inc_%s", aggFunc.Name),
						FuncType: ast.FuncTypeScalar,
						Args:     aggFunc.Args,
						FuncId:   *index,
					}
					name := fmt.Sprintf("inc_agg_col_%v", *index)
					newField := &ast.Field{
						Name: name,
						Expr: newAggFunc,
					}
					incAggFuncFields = append(incAggFuncFields, newField)
					newFieldRef := &ast.FieldRef{
						StreamName: ast.DefaultStream,
						Name:       name,
					}
					rewriteIntoBypass(newFieldRef, aggFunc)
				}
			}
		}
		return true
	})
	return incAggFuncFields, true
}

func rewriteIntoBypass(newFieldRef *ast.FieldRef, f *ast.Call) {
	f.FuncType = ast.FuncTypeScalar
	f.Args = []ast.Expr{newFieldRef}
	f.Name = "bypass"
}

func supportedWindowType(window *ast.Window) bool {
	_, ok := supportedWType[window.WindowType]
	if !ok {
		return false
	}
	if window.WindowType == ast.COUNT_WINDOW {
		if window.Interval != nil {
			return false
		}
	}
	return true
}

var supportedWType = map[ast.WindowType]struct{}{
	ast.COUNT_WINDOW:    {},
	ast.SLIDING_WINDOW:  {},
	ast.HOPPING_WINDOW:  {},
	ast.TUMBLING_WINDOW: {},
}

func rewriteIfPushdownAlias(stmt *ast.SelectStatement, opt *def.RuleOption) map[ast.StreamName]map[string]string {
	if opt.PlanOptimizeStrategy == nil {
		return nil
	}
	if !opt.PlanOptimizeStrategy.EnableAliasPushdown {
		return nil
	}
	if hasWildcard(stmt) {
		return nil
	}
	dsColAliasMapping := make(map[ast.StreamName]map[string]string)
	for index, field := range stmt.Fields {
		afr, ok := field.Expr.(*ast.FieldRef)
		if ok && afr.IsAlias() && afr.Expression != nil {
			cfr, ok := afr.Expression.(*ast.FieldRef)
			if ok && cfr.IsColumn() && cfr.Name == field.Name {
				columnUsed := searchColumnUsedCount(stmt, field.Name)
				if columnUsed == 1 {
					newField := buildField(afr.Name, cfr.StreamName)
					stmt.Fields[index] = newField
					v, ok := dsColAliasMapping[cfr.StreamName]
					if !ok {
						v = make(map[string]string)
					}
					v[cfr.Name] = afr.Name
					dsColAliasMapping[cfr.StreamName] = v
				}
			}
		}
	}
	return dsColAliasMapping
}

func hasWildcard(stmt *ast.SelectStatement) bool {
	wildcard := false
	ast.WalkFunc(stmt, func(n ast.Node) bool {
		switch n.(type) {
		case *ast.Wildcard:
			wildcard = true
			return false
		}
		return true
	})
	return wildcard
}

func searchColumnUsedCount(stmt *ast.SelectStatement, colName string) int {
	count := 0
	ast.WalkFunc(stmt, func(n ast.Node) bool {
		fr, ok := n.(*ast.FieldRef)
		if ok && fr.IsColumn() {
			if fr.Name == colName {
				count++
			}
		}
		return true
	})
	return count
}

func buildField(colName string, streamName ast.StreamName) ast.Field {
	return ast.Field{
		Name: colName,
		Expr: &ast.FieldRef{
			Name:       colName,
			StreamName: streamName,
		},
	}
}

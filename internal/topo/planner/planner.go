package planner

import (
	"errors"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/topo"
	"github.com/lf-edge/ekuiper/internal/topo/node"
	"github.com/lf-edge/ekuiper/internal/topo/operator"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/kv"
	"path"
)

func Plan(rule *api.Rule, storePath string) (*topo.Topo, error) {
	return PlanWithSourcesAndSinks(rule, storePath, nil, nil)
}

// For test only
func PlanWithSourcesAndSinks(rule *api.Rule, storePath string, sources []*node.SourceNode, sinks []*node.SinkNode) (*topo.Topo, error) {
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
	store := kv.GetDefaultKVStore(path.Join(storePath, "stream"))
	err = store.Open()
	if err != nil {
		return nil, err
	}
	defer store.Close()
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
		op  node.OperatorNode
		err error
	)
	switch t := lp.(type) {
	case *DataSourcePlan:
		switch t.streamStmt.StreamType {
		case ast.TypeStream:
			pp, err := operator.NewPreprocessor(t.streamFields, t.allMeta, t.metaFields, t.iet, t.timestampField, t.timestampFormat, t.isBinary)
			if err != nil {
				return nil, 0, err
			}
			var srcNode *node.SourceNode
			if len(sources) == 0 {
				node := node.NewSourceNode(string(t.name), t.streamStmt.StreamType, t.streamStmt.Options)
				srcNode = node
			} else {
				srcNode = getMockSource(sources, string(t.name))
				if srcNode == nil {
					return nil, 0, fmt.Errorf("can't find predefined source %s", t.name)
				}
			}
			tp.AddSrc(srcNode)
			op = Transform(pp, fmt.Sprintf("%d_preprocessor_%s", newIndex, t.name), options)
			inputs = []api.Emitter{srcNode}
		case ast.TypeTable:
			pp, err := operator.NewTableProcessor(string(t.name), t.streamFields, t.streamStmt.Options)
			if err != nil {
				return nil, 0, err
			}
			var srcNode *node.SourceNode
			if len(sources) > 0 {
				srcNode = getMockSource(sources, string(t.name))
			}
			if srcNode == nil {
				srcNode = node.NewSourceNode(string(t.name), t.streamStmt.StreamType, t.streamStmt.Options)
			}
			tp.AddSrc(srcNode)
			op = Transform(pp, fmt.Sprintf("%d_tableprocessor_%s", newIndex, t.name), options)
			inputs = []api.Emitter{srcNode}
		}
	case *WindowPlan:
		if t.condition != nil {
			wfilterOp := Transform(&operator.FilterOp{Condition: t.condition}, fmt.Sprintf("%d_windowFilter", newIndex), options)
			wfilterOp.SetConcurrency(options.Concurrency)
			tp.AddOperator(inputs, wfilterOp)
			inputs = []api.Emitter{wfilterOp}
		}

		op, err = node.NewWindowOp(fmt.Sprintf("%d_window", newIndex), node.WindowConfig{
			Type:     t.wtype,
			Length:   t.length,
			Interval: t.interval,
		}, streamsFromStmt, options)
		if err != nil {
			return nil, 0, err
		}
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
		op = Transform(&operator.ProjectOp{Fields: t.fields, IsAggregate: t.isAggregate, SendMeta: t.sendMeta}, fmt.Sprintf("%d_project", newIndex), options)
	default:
		return nil, 0, fmt.Errorf("unknown logical plan %v", t)
	}
	if uop, ok := op.(*node.UnaryOperator); ok {
		uop.SetConcurrency(options.Concurrency)
	}
	tp.AddOperator(inputs, op)
	return op, newIndex, nil
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
		tableChildren []LogicalPlan
		tableEmitters []string
		w             *ast.Window
		ds            ast.Dimensions
	)

	streamStmts, err := decorateStmt(stmt, store)
	if err != nil {
		return nil, err
	}

	for _, streamStmt := range streamStmts {
		p = DataSourcePlan{
			name:       streamStmt.Name,
			streamStmt: streamStmt,
			iet:        opt.IsEventTime,
			allMeta:    opt.SendMetaToSink,
		}.Init()
		if streamStmt.StreamType == ast.TypeStream {
			children = append(children, p)
		} else {
			tableChildren = append(tableChildren, p)
			tableEmitters = append(tableEmitters, string(streamStmt.Name))
		}
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
				//if no interval value is set and it's count window, then set interval to length value.
				wp.interval = w.Length.Val
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
		if len(tableChildren) > 0 {
			p = JoinAlignPlan{
				Emitters: tableEmitters,
			}.Init()
			p.SetChildren(append(children, tableChildren...))
			children = []LogicalPlan{p}
		} else if w == nil {
			return nil, errors.New("need to run stream join in windows")
		}
		// TODO extract on filter
		p = JoinPlan{
			from:  stmt.Sources[0].(*ast.Table),
			joins: stmt.Joins,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}
	if stmt.Condition != nil {
		p = FilterPlan{
			condition: stmt.Condition,
		}.Init()
		p.SetChildren(children)
		children = []LogicalPlan{p}
	}
	// TODO handle aggregateAlias in optimization as it does not only happen in select fields
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
			isAggregate: ast.IsAggStatement(stmt),
			sendMeta:    opt.SendMetaToSink,
		}.Init()
		p.SetChildren(children)
	}

	return optimize(p)
}

func Transform(op node.UnOperation, name string, options *api.RuleOption) *node.UnaryOperator {
	operator := node.New(name, xsql.FuncRegisters, options)
	operator.SetOperation(op)
	return operator
}

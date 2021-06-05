package planner

import (
	"errors"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/common/kv"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"github.com/emqx/kuiper/xstream/operators"
	"path"
	"sort"
	"strings"
)

func Plan(rule *api.Rule, storePath string) (*xstream.TopologyNew, error) {
	return PlanWithSourcesAndSinks(rule, storePath, nil, nil)
}

// For test only
func PlanWithSourcesAndSinks(rule *api.Rule, storePath string, sources []*nodes.SourceNode, sinks []*nodes.SinkNode) (*xstream.TopologyNew, error) {
	sql := rule.Sql

	common.Log.Infof("Init rule with options %+v", rule.Options)
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

type aliasInfo struct {
	alias       xsql.Field
	refSources  []string
	isAggregate bool
}

// Analyze the select statement by decorating the info from stream statement.
// Typically, set the correct stream name for fieldRefs
func decorateStmt(s *xsql.SelectStatement, store kv.KeyValue) ([]*xsql.StreamStmt, map[string]*aliasInfo, error) {
	streamsFromStmt := xsql.GetStreams(s)
	streamStmts := make([]*xsql.StreamStmt, len(streamsFromStmt))
	aliasSourceMap := make(map[string]*aliasInfo)
	isSchemaless := false
	for i, s := range streamsFromStmt {
		streamStmt, err := xsql.GetDataSource(store, s)
		if err != nil {
			return nil, nil, fmt.Errorf("fail to get stream %s, please check if stream is created", s)
		}
		streamStmts[i] = streamStmt
		if streamStmt.StreamFields == nil {
			isSchemaless = true
		}
	}
	var walkErr error
	for _, f := range s.Fields {
		if f.AName != "" {
			if _, ok := aliasSourceMap[strings.ToLower(f.AName)]; ok {
				return nil, nil, fmt.Errorf("duplicate alias %s", f.AName)
			}
			refStreams := make(map[string]struct{})
			xsql.WalkFunc(f.Expr, func(n xsql.Node) {
				switch expr := n.(type) {
				case *xsql.FieldRef:
					err := updateFieldRefStream(expr, streamStmts, isSchemaless)
					if err != nil {
						walkErr = err
						return
					}
					if expr.StreamName != "" {
						refStreams[string(expr.StreamName)] = struct{}{}
					}
				}
			})
			if walkErr != nil {
				return nil, nil, walkErr
			}
			refStreamKeys := make([]string, len(refStreams))
			c := 0
			for k, _ := range refStreams {
				refStreamKeys[c] = k
				c++
			}
			aliasSourceMap[strings.ToLower(f.AName)] = &aliasInfo{
				alias:       f,
				refSources:  refStreamKeys,
				isAggregate: xsql.HasAggFuncs(f.Expr),
			}
		}
	}
	// Select fields are visited firstly to make sure all aliases have streamName set
	xsql.WalkFunc(s, func(n xsql.Node) {
		//skip alias field
		switch f := n.(type) {
		case *xsql.Field:
			if f.AName != "" {
				return
			}
		case *xsql.FieldRef:
			if f.StreamName == xsql.DEFAULT_STREAM {
				for aname, ainfo := range aliasSourceMap {
					if strings.EqualFold(f.Name, aname) {
						switch len(ainfo.refSources) {
						case 0: // if no ref source, we can put it to any stream, here just assign it to the first stream
							f.StreamName = streamStmts[0].Name
						case 1:
							f.StreamName = xsql.StreamName(ainfo.refSources[0])
						default:
							f.StreamName = xsql.MULTI_STREAM
						}
						return
					}

				}
			}
			err := updateFieldRefStream(f, streamStmts, isSchemaless)
			if err != nil {
				walkErr = err
			}
		}
	})
	return streamStmts, aliasSourceMap, walkErr
}

func updateFieldRefStream(f *xsql.FieldRef, streamStmts []*xsql.StreamStmt, isSchemaless bool) (err error) {
	count := 0
	for _, streamStmt := range streamStmts {
		for _, field := range streamStmt.StreamFields {
			if strings.EqualFold(f.Name, field.Name) {
				if f.StreamName == xsql.DEFAULT_STREAM {
					f.StreamName = streamStmt.Name
					count++
				} else if f.StreamName == streamStmt.Name {
					count++
				}
				break
			}
		}
	}
	if count > 1 {
		err = fmt.Errorf("ambiguous field %s", f.Name)
	} else if count == 0 && f.StreamName == xsql.DEFAULT_STREAM { // alias may refer to non stream field
		if !isSchemaless {
			err = fmt.Errorf("unknown field %s.%s", f.StreamName, f.Name)
		} else if len(streamStmts) == 1 { // If only one schemaless stream, all the fields must be a field of that stream
			f.StreamName = streamStmts[0].Name
		}
	}
	return
}

func createTopo(rule *api.Rule, lp LogicalPlan, sources []*nodes.SourceNode, sinks []*nodes.SinkNode, streamsFromStmt []string) (*xstream.TopologyNew, error) {
	// Create topology
	tp, err := xstream.NewWithNameAndQos(rule.Id, rule.Options.Qos, rule.Options.CheckpointInterval)
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
				tp.AddSink(inputs, nodes.NewSinkNode(fmt.Sprintf("%s_%d", name, i), name, props))
			}
		}
	}

	return tp, nil
}

func buildOps(lp LogicalPlan, tp *xstream.TopologyNew, options *api.RuleOption, sources []*nodes.SourceNode, streamsFromStmt []string, index int) (api.Emitter, int, error) {
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
		op  nodes.OperatorNode
		err error
	)
	switch t := lp.(type) {
	case *DataSourcePlan:
		switch t.streamStmt.StreamType {
		case xsql.TypeStream:
			pp, err := operators.NewPreprocessor(t.streamFields, t.alias, t.allMeta, t.metaFields, t.iet, t.timestampField, t.timestampFormat, t.isBinary)
			if err != nil {
				return nil, 0, err
			}
			var srcNode *nodes.SourceNode
			if len(sources) == 0 {
				node := nodes.NewSourceNode(t.name, t.streamStmt.StreamType, t.streamStmt.Options)
				srcNode = node
			} else {
				srcNode = getMockSource(sources, t.name)
				if srcNode == nil {
					return nil, 0, fmt.Errorf("can't find predefined source %s", t.name)
				}
			}
			tp.AddSrc(srcNode)
			op = Transform(pp, fmt.Sprintf("%d_preprocessor_%s", newIndex, t.name), options)
			inputs = []api.Emitter{srcNode}
		case xsql.TypeTable:
			pp, err := operators.NewTableProcessor(t.name, t.streamFields, t.alias, t.streamStmt.Options)
			if err != nil {
				return nil, 0, err
			}
			var srcNode *nodes.SourceNode
			if len(sources) > 0 {
				srcNode = getMockSource(sources, t.name)
			}
			if srcNode == nil {
				srcNode = nodes.NewSourceNode(t.name, t.streamStmt.StreamType, t.streamStmt.Options)
			}
			tp.AddSrc(srcNode)
			op = Transform(pp, fmt.Sprintf("%d_tableprocessor_%s", newIndex, t.name), options)
			inputs = []api.Emitter{srcNode}
		}
	case *WindowPlan:
		if t.condition != nil {
			wfilterOp := Transform(&operators.FilterOp{Condition: t.condition}, fmt.Sprintf("%d_windowFilter", newIndex), options)
			wfilterOp.SetConcurrency(options.Concurrency)
			tp.AddOperator(inputs, wfilterOp)
			inputs = []api.Emitter{wfilterOp}
		}

		op, err = nodes.NewWindowOp(fmt.Sprintf("%d_window", newIndex), nodes.WindowConfig{
			Type:     t.wtype,
			Length:   t.length,
			Interval: t.interval,
		}, streamsFromStmt, options)
		if err != nil {
			return nil, 0, err
		}
	case *JoinAlignPlan:
		op, err = nodes.NewJoinAlignNode(fmt.Sprintf("%d_join_aligner", newIndex), t.Emitters, options)
	case *JoinPlan:
		op = Transform(&operators.JoinOp{Joins: t.joins, From: t.from}, fmt.Sprintf("%d_join", newIndex), options)
	case *FilterPlan:
		op = Transform(&operators.FilterOp{Condition: t.condition}, fmt.Sprintf("%d_filter", newIndex), options)
	case *AggregatePlan:
		op = Transform(&operators.AggregateOp{Dimensions: t.dimensions, Alias: t.alias}, fmt.Sprintf("%d_aggregate", newIndex), options)
	case *HavingPlan:
		op = Transform(&operators.HavingOp{Condition: t.condition}, fmt.Sprintf("%d_having", newIndex), options)
	case *OrderPlan:
		op = Transform(&operators.OrderOp{SortFields: t.SortFields}, fmt.Sprintf("%d_order", newIndex), options)
	case *ProjectPlan:
		op = Transform(&operators.ProjectOp{Fields: t.fields, IsAggregate: t.isAggregate, SendMeta: t.sendMeta}, fmt.Sprintf("%d_project", newIndex), options)
	default:
		return nil, 0, fmt.Errorf("unknown logical plan %v", t)
	}
	if uop, ok := op.(*nodes.UnaryOperator); ok {
		uop.SetConcurrency(options.Concurrency)
	}
	tp.AddOperator(inputs, op)
	return op, newIndex, nil
}

func getMockSource(sources []*nodes.SourceNode, name string) *nodes.SourceNode {
	for _, source := range sources {
		if name == source.GetName() {
			return source
		}
	}
	return nil
}

func createLogicalPlan(stmt *xsql.SelectStatement, opt *api.RuleOption, store kv.KeyValue) (LogicalPlan, error) {

	dimensions := stmt.Dimensions
	var (
		p        LogicalPlan
		children []LogicalPlan
		// If there are tables, the plan graph will be different for join/window
		tableChildren []LogicalPlan
		tableEmitters []string
		w             *xsql.Window
		ds            xsql.Dimensions
	)

	streamStmts, aliasMap, err := decorateStmt(stmt, store)
	if err != nil {
		return nil, err
	}

	for i, streamStmt := range streamStmts {
		p = DataSourcePlan{
			name:       string(streamStmt.Name),
			streamStmt: streamStmt,
			iet:        opt.IsEventTime,
			alias:      aliasFieldsForSource(aliasMap, streamStmt.Name, i == 0),
			allMeta:    opt.SendMetaToSink,
		}.Init()
		if streamStmt.StreamType == xsql.TypeStream {
			children = append(children, p)
		} else {
			tableChildren = append(tableChildren, p)
			tableEmitters = append(tableEmitters, string(streamStmt.Name))
		}
	}
	aggregateAlias, _ := complexAlias(aliasMap)
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
			} else if w.WindowType == xsql.COUNT_WINDOW {
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
			from:  stmt.Sources[0].(*xsql.Table),
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
	if dimensions != nil || len(aggregateAlias) > 0 {
		ds = dimensions.GetGroups()
		if (ds != nil && len(ds) > 0) || len(aggregateAlias) > 0 {
			p = AggregatePlan{
				dimensions: ds,
				alias:      aggregateAlias,
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
	}

	return optimize(p)
}

func aliasFieldsForSource(aliasMap map[string]*aliasInfo, name xsql.StreamName, isFirst bool) (result xsql.Fields) {
	for _, ainfo := range aliasMap {
		if ainfo.isAggregate {
			continue
		}
		switch len(ainfo.refSources) {
		case 0:
			if isFirst {
				result = append(result, ainfo.alias)
			}
		case 1:
			if strings.EqualFold(ainfo.refSources[0], string(name)) {
				result = append(result, ainfo.alias)
			}
		}
	}
	// sort to get a constant result for testing
	sort.Sort(result)
	return
}

func complexAlias(aliasMap map[string]*aliasInfo) (aggregateAlias xsql.Fields, joinAlias xsql.Fields) {
	for _, ainfo := range aliasMap {
		if ainfo.isAggregate {
			aggregateAlias = append(aggregateAlias, ainfo.alias)
			continue
		}
		if len(ainfo.refSources) > 1 {
			joinAlias = append(joinAlias, ainfo.alias)
		}
	}
	return
}

func Transform(op nodes.UnOperation, name string, options *api.RuleOption) *nodes.UnaryOperator {
	operator := nodes.New(name, xsql.FuncRegisters, options)
	operator.SetOperation(op)
	return operator
}

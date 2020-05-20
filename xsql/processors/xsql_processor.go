package processors

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xsql/plans"
	"github.com/emqx/kuiper/xstream"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"github.com/emqx/kuiper/xstream/operators"
	"path"
	"strings"
)

var log = common.Log

type StreamProcessor struct {
	db common.KeyValue
}

//@params d : the directory of the DB to save the stream info
func NewStreamProcessor(d string) *StreamProcessor {
	processor := &StreamProcessor{
		db: common.GetSimpleKVStore(d),
	}
	return processor
}

func (p *StreamProcessor) ExecStmt(statement string) (result []string, err error) {
	parser := xsql.NewParser(strings.NewReader(statement))
	stmt, err := xsql.Language.Parse(parser)
	if err != nil {
		return nil, err
	}
	switch s := stmt.(type) {
	case *xsql.StreamStmt:
		var r string
		r, err = p.execCreateStream(s, statement)
		result = append(result, r)
	case *xsql.ShowStreamsStatement:
		result, err = p.execShowStream(s)
	case *xsql.DescribeStreamStatement:
		var r string
		r, err = p.execDescribeStream(s)
		result = append(result, r)
	case *xsql.ExplainStreamStatement:
		var r string
		r, err = p.execExplainStream(s)
		result = append(result, r)
	case *xsql.DropStreamStatement:
		var r string
		r, err = p.execDropStream(s)
		result = append(result, r)
	default:
		return nil, fmt.Errorf("Invalid stream statement: %s", statement)
	}

	return
}

func (p *StreamProcessor) execCreateStream(stmt *xsql.StreamStmt, statement string) (string, error) {
	err := p.db.Open()
	if err != nil {
		return "", fmt.Errorf("Create stream fails, error when opening db: %v.", err)
	}
	defer p.db.Close()
	err = p.db.Set(string(stmt.Name), statement)
	if err != nil {
		return "", fmt.Errorf("Create stream fails: %v.", err)
	} else {
		info := fmt.Sprintf("Stream %s is created.", stmt.Name)
		log.Printf("%s", info)
		return info, nil
	}
}

func (p *StreamProcessor) ExecStreamSql(statement string) (string, error) {
	r, err := p.ExecStmt(statement)
	if err != nil {
		return "", err
	} else {
		return strings.Join(r, "\n"), err
	}
}

func (p *StreamProcessor) execShowStream(stmt *xsql.ShowStreamsStatement) ([]string, error) {
	keys, err := p.ShowStream()
	if len(keys) == 0 {
		keys = append(keys, "No stream definitions are found.")
	}
	return keys, err
}

func (p *StreamProcessor) ShowStream() ([]string, error) {
	err := p.db.Open()
	if err != nil {
		return nil, fmt.Errorf("Show stream fails, error when opening db: %v.", err)
	}
	defer p.db.Close()
	return p.db.Keys()
}

func (p *StreamProcessor) execDescribeStream(stmt *xsql.DescribeStreamStatement) (string, error) {
	streamStmt, err := p.DescStream(stmt.Name)
	if err != nil {
		return "", err
	}
	var buff bytes.Buffer
	buff.WriteString("Fields\n--------------------------------------------------------------------------------\n")
	for _, f := range streamStmt.StreamFields {
		buff.WriteString(f.Name + "\t")
		buff.WriteString(xsql.PrintFieldType(f.FieldType))
		buff.WriteString("\n")
	}
	buff.WriteString("\n")
	common.PrintMap(streamStmt.Options, &buff)
	return buff.String(), err
}

func (p *StreamProcessor) DescStream(name string) (*xsql.StreamStmt, error) {
	err := p.db.Open()
	if err != nil {
		return nil, fmt.Errorf("Describe stream fails, error when opening db: %v.", err)
	}
	defer p.db.Close()
	s, f := p.db.Get(name)
	if !f {
		return nil, common.NewErrorWithCode(common.NOT_FOUND, fmt.Sprintf("Stream %s is not found.", name))
	}
	s1 := s.(string)

	parser := xsql.NewParser(strings.NewReader(s1))
	stream, err := xsql.Language.Parse(parser)
	if err != nil {
		return nil, err
	}
	streamStmt, ok := stream.(*xsql.StreamStmt)
	if !ok {
		return nil, fmt.Errorf("Error resolving the stream %s, the data in db may be corrupted.", name)
	}
	return streamStmt, nil
}

func (p *StreamProcessor) execExplainStream(stmt *xsql.ExplainStreamStatement) (string, error) {
	err := p.db.Open()
	if err != nil {
		return "", fmt.Errorf("Explain stream fails, error when opening db: %v.", err)
	}
	defer p.db.Close()
	_, f := p.db.Get(stmt.Name)
	if !f {
		return "", fmt.Errorf("Stream %s is not found.", stmt.Name)
	}
	return "TO BE SUPPORTED", nil
}

func (p *StreamProcessor) execDropStream(stmt *xsql.DropStreamStatement) (string, error) {
	s, err := p.DropStream(stmt.Name)
	if err != nil {
		return s, fmt.Errorf("Drop stream fails: %s.", err)
	}
	return s, nil
}

func (p *StreamProcessor) DropStream(name string) (string, error) {
	err := p.db.Open()
	if err != nil {
		return "", fmt.Errorf("error when opening db: %v", err)
	}
	defer p.db.Close()
	err = p.db.Delete(name)
	if err != nil {
		return "", err
	} else {
		return fmt.Sprintf("Stream %s is dropped.", name), nil
	}
}

func GetStream(m *common.SimpleKVStore, name string) (stmt *xsql.StreamStmt, err error) {
	s, f := m.Get(name)
	if !f {
		return nil, fmt.Errorf("Cannot find key %s. ", name)
	}
	s1, _ := s.(string)
	parser := xsql.NewParser(strings.NewReader(s1))
	stream, err := xsql.Language.Parse(parser)
	stmt, ok := stream.(*xsql.StreamStmt)
	if !ok {
		err = fmt.Errorf("Error resolving the stream %s, the data in db may be corrupted.", name)
	}
	return
}

type RuleProcessor struct {
	db        common.KeyValue
	rootDbDir string
}

func NewRuleProcessor(d string) *RuleProcessor {
	processor := &RuleProcessor{
		db:        common.GetSimpleKVStore(path.Join(d, "rule")),
		rootDbDir: d,
	}
	return processor
}

func (p *RuleProcessor) ExecCreate(name, ruleJson string) (*api.Rule, error) {
	rule, err := p.getRuleByJson(name, ruleJson)
	if err != nil {
		return nil, err
	}

	err = p.db.Open()
	if err != nil {
		return nil, err
	}
	defer p.db.Close()

	err = p.db.Set(rule.Id, ruleJson)
	if err != nil {
		return nil, err
	} else {
		log.Infof("Rule %s is created.", rule.Id)
	}

	return rule, nil
}

func (p *RuleProcessor) GetRuleByName(name string) (*api.Rule, error) {
	err := p.db.Open()
	if err != nil {
		return nil, err
	}
	defer p.db.Close()
	s, f := p.db.Get(name)
	if !f {
		return nil, common.NewErrorWithCode(common.NOT_FOUND, fmt.Sprintf("Rule %s is not found.", name))
	}
	s1, _ := s.(string)
	return p.getRuleByJson(name, s1)
}

func (p *RuleProcessor) getRuleByJson(name, ruleJson string) (*api.Rule, error) {
	var rule api.Rule
	if err := json.Unmarshal([]byte(ruleJson), &rule); err != nil {
		return nil, fmt.Errorf("Parse rule %s error : %s.", ruleJson, err)
	}

	//validation
	if rule.Id == "" && name == "" {
		return nil, fmt.Errorf("Missing rule id.")
	}
	if name != "" && rule.Id != "" && name != rule.Id {
		return nil, fmt.Errorf("Name is not consistent with rule id.")
	}
	if rule.Id == "" {
		rule.Id = name
	}
	if rule.Sql == "" {
		return nil, fmt.Errorf("Missing rule SQL.")
	}
	if rule.Actions == nil || len(rule.Actions) == 0 {
		return nil, fmt.Errorf("Missing rule actions.")
	}
	return &rule, nil
}

func (p *RuleProcessor) ExecInitRule(rule *api.Rule) (*xstream.TopologyNew, error) {
	if tp, inputs, err := p.createTopo(rule); err != nil {
		return nil, err
	} else {
		for i, m := range rule.Actions {
			for name, action := range m {
				props, ok := action.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("expect map[string]interface{} type for the action properties, but found %v", action)
				}
				tp.AddSink(inputs, nodes.NewSinkNode(fmt.Sprintf("sink_%s_%d", name, i), name, props))
			}
		}
		return tp, nil
	}
}

func (p *RuleProcessor) ExecQuery(ruleid, sql string) (*xstream.TopologyNew, error) {
	if tp, inputs, err := p.createTopo(&api.Rule{Id: ruleid, Sql: sql}); err != nil {
		return nil, err
	} else {
		tp.AddSink(inputs, nodes.NewSinkNode("sink_memory_log", "logToMemory", nil))
		go func() {
			select {
			case err := <-tp.Open():
				log.Infof("closing query for error: %v", err)
				tp.GetContext().SetError(err)
				tp.Cancel()
			}
		}()
		return tp, nil
	}
}

func (p *RuleProcessor) ExecDesc(name string) (string, error) {
	err := p.db.Open()
	if err != nil {
		return "", err
	}
	defer p.db.Close()
	s, f := p.db.Get(name)
	if !f {
		return "", fmt.Errorf("Rule %s is not found.", name)
	}
	s1, _ := s.(string)
	dst := &bytes.Buffer{}
	if err := json.Indent(dst, []byte(s1), "", "  "); err != nil {
		return "", err
	}

	return fmt.Sprintln(dst.String()), nil
}

func (p *RuleProcessor) GetAllRules() ([]string, error) {
	err := p.db.Open()
	if err != nil {
		return nil, err
	}
	defer p.db.Close()
	return p.db.Keys()
}

func (p *RuleProcessor) ExecDrop(name string) (string, error) {
	err := p.db.Open()
	if err != nil {
		return "", err
	}
	defer p.db.Close()
	err = p.db.Delete(name)
	if err != nil {
		return "", err
	} else {
		return fmt.Sprintf("Rule %s is dropped.", name), nil
	}
}

func (p *RuleProcessor) createTopo(rule *api.Rule) (*xstream.TopologyNew, []api.Emitter, error) {
	return p.createTopoWithSources(rule, nil)
}

//For test to mock source
func (p *RuleProcessor) createTopoWithSources(rule *api.Rule, sources []*nodes.SourceNode) (*xstream.TopologyNew, []api.Emitter, error) {
	name := rule.Id
	sql := rule.Sql
	var (
		isEventTime  bool
		lateTol      int64
		concurrency  = 1
		bufferLength = 1024
	)

	if iet, ok := rule.Options["isEventTime"]; ok {
		isEventTime, ok = iet.(bool)
		if !ok {
			return nil, nil, fmt.Errorf("Invalid rule option isEventTime %v, bool type is required.", iet)
		}
	}
	if isEventTime {
		if l, ok := rule.Options["lateTolerance"]; ok {
			if fl, ok := l.(float64); ok {
				lateTol = int64(fl)
			} else {
				return nil, nil, fmt.Errorf("Invalid rule option lateTolerance %v, int type is required.", l)
			}
		}
	}
	if l, ok := rule.Options["concurrency"]; ok {
		if fl, ok := l.(float64); ok {
			concurrency = int(fl)
		} else {
			return nil, nil, fmt.Errorf("Invalid rule option concurrency %v, int type is required.", l)
		}
	}
	if l, ok := rule.Options["bufferLength"]; ok {
		if fl, ok := l.(float64); ok {
			bufferLength = int(fl)
		} else {
			return nil, nil, fmt.Errorf("Invalid rule option bufferLength %v, int type is required.", l)
		}
	}
	log.Infof("Init rule with options {isEventTime: %v, lateTolerance: %d, concurrency: %d, bufferLength: %d", isEventTime, lateTol, concurrency, bufferLength)
	shouldCreateSource := sources == nil
	parser := xsql.NewParser(strings.NewReader(sql))
	if stmt, err := xsql.Language.Parse(parser); err != nil {
		return nil, nil, fmt.Errorf("Parse SQL %s error: %s.", sql, err)
	} else {
		if selectStmt, ok := stmt.(*xsql.SelectStatement); !ok {
			return nil, nil, fmt.Errorf("SQL %s is not a select statement.", sql)
		} else {
			tp := xstream.NewWithName(name)
			var inputs []api.Emitter
			streamsFromStmt := xsql.GetStreams(selectStmt)
			if !shouldCreateSource && len(streamsFromStmt) != len(sources) {
				return nil, nil, fmt.Errorf("Invalid parameter sources or streams, the length cannot match the statement, expect %d sources.", len(streamsFromStmt))
			}
			store := common.GetSimpleKVStore(path.Join(p.rootDbDir, "stream"))
			err := store.Open()
			if err != nil {
				return nil, nil, err
			}
			defer store.Close()

			var alias, aggregateAlias xsql.Fields
			for _, f := range selectStmt.Fields {
				if f.AName != "" {
					if !xsql.HasAggFuncs(f.Expr) {
						alias = append(alias, f)
					} else {
						aggregateAlias = append(aggregateAlias, f)
					}
				}
			}
			for i, s := range streamsFromStmt {
				streamStmt, err := GetStream(store, s)
				if err != nil {
					return nil, nil, fmt.Errorf("fail to get stream %s, please check if stream is created", s)
				}
				pp, err := plans.NewPreprocessor(streamStmt, alias, isEventTime)
				if err != nil {
					return nil, nil, err
				}
				if shouldCreateSource {
					node := nodes.NewSourceNode(s, streamStmt.Options)
					tp.AddSrc(node)
					preprocessorOp := xstream.Transform(pp, "preprocessor_"+s, bufferLength)
					preprocessorOp.SetConcurrency(concurrency)
					tp.AddOperator([]api.Emitter{node}, preprocessorOp)
					inputs = append(inputs, preprocessorOp)
				} else {
					tp.AddSrc(sources[i])
					preprocessorOp := xstream.Transform(pp, "preprocessor_"+s, bufferLength)
					preprocessorOp.SetConcurrency(concurrency)
					tp.AddOperator([]api.Emitter{sources[i]}, preprocessorOp)
					inputs = append(inputs, preprocessorOp)
				}
			}
			dimensions := selectStmt.Dimensions
			var w *xsql.Window
			if dimensions != nil {
				w = dimensions.GetWindow()
				if w != nil {
					wop, err := operators.NewWindowOp("window", w, isEventTime, lateTol, streamsFromStmt, bufferLength)
					if err != nil {
						return nil, nil, err
					}
					tp.AddOperator(inputs, wop)
					inputs = []api.Emitter{wop}
				}
			}

			if w != nil && selectStmt.Joins != nil {
				joinOp := xstream.Transform(&plans.JoinPlan{Joins: selectStmt.Joins, From: selectStmt.Sources[0].(*xsql.Table)}, "join", bufferLength)
				joinOp.SetConcurrency(concurrency)
				tp.AddOperator(inputs, joinOp)
				inputs = []api.Emitter{joinOp}
			}

			if selectStmt.Condition != nil {
				filterOp := xstream.Transform(&plans.FilterPlan{Condition: selectStmt.Condition}, "filter", bufferLength)
				filterOp.SetConcurrency(concurrency)
				tp.AddOperator(inputs, filterOp)
				inputs = []api.Emitter{filterOp}
			}

			var ds xsql.Dimensions
			if dimensions != nil || len(aggregateAlias) > 0 {
				ds = dimensions.GetGroups()
				if (ds != nil && len(ds) > 0) || len(aggregateAlias) > 0 {
					aggregateOp := xstream.Transform(&plans.AggregatePlan{Dimensions: ds, Alias: aggregateAlias}, "aggregate", bufferLength)
					aggregateOp.SetConcurrency(concurrency)
					tp.AddOperator(inputs, aggregateOp)
					inputs = []api.Emitter{aggregateOp}
				}
			}

			if selectStmt.Having != nil {
				havingOp := xstream.Transform(&plans.HavingPlan{selectStmt.Having}, "having", bufferLength)
				havingOp.SetConcurrency(concurrency)
				tp.AddOperator(inputs, havingOp)
				inputs = []api.Emitter{havingOp}
			}

			if selectStmt.SortFields != nil {
				orderOp := xstream.Transform(&plans.OrderPlan{SortFields: selectStmt.SortFields}, "order", bufferLength)
				orderOp.SetConcurrency(concurrency)
				tp.AddOperator(inputs, orderOp)
				inputs = []api.Emitter{orderOp}
			}

			if selectStmt.Fields != nil {
				projectOp := xstream.Transform(&plans.ProjectPlan{Fields: selectStmt.Fields, IsAggregate: xsql.IsAggStatement(selectStmt)}, "project", bufferLength)
				projectOp.SetConcurrency(concurrency)
				tp.AddOperator(inputs, projectOp)
				inputs = []api.Emitter{projectOp}
			}
			return tp, inputs, nil
		}
	}
}

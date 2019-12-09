package processors

import (
	"bytes"
	"encoding/json"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/common/plugin_manager"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xsql/plans"
	"github.com/emqx/kuiper/xstream"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/extensions"
	"github.com/emqx/kuiper/xstream/nodes"
	"github.com/emqx/kuiper/xstream/operators"
	"github.com/emqx/kuiper/xstream/sinks"
	"fmt"
	"path"
	"strings"
)

var log = common.Log

type StreamProcessor struct {
	statement string
	dbDir     string
}

//@params s : the sql string of create stream statement
//@params d : the directory of the DB to save the stream info
func NewStreamProcessor(s, d string) *StreamProcessor {
	processor := &StreamProcessor{
		statement: s,
		dbDir:     d,
	}
	return processor
}


func (p *StreamProcessor) Exec() (result []string, err error) {
	parser := xsql.NewParser(strings.NewReader(p.statement))
	stmt, err := xsql.Language.Parse(parser)
	if err != nil {
		return
	}

	store := common.GetSimpleKVStore(p.dbDir)
	err = store.Open()
	if err != nil {
		return
	}
	defer store.Close()

	switch s := stmt.(type) {
	case *xsql.StreamStmt:
		var r string
		r, err = p.execCreateStream(s, store)
		result = append(result, r)
	case *xsql.ShowStreamsStatement:
		result, err = p.execShowStream(s, store)
	case *xsql.DescribeStreamStatement:
		var r string
		r, err = p.execDescribeStream(s, store)
		result = append(result, r)
	case *xsql.ExplainStreamStatement:
		var r string
		r, err = p.execExplainStream(s, store)
		result = append(result, r)
	case *xsql.DropStreamStatement:
		var r string
		r, err = p.execDropStream(s, store)
		result = append(result, r)
	}

	return
}

func (p *StreamProcessor) execCreateStream(stmt *xsql.StreamStmt, db common.KeyValue) (string, error) {
	err := db.Set(string(stmt.Name), p.statement)
	if err != nil {
		return "", fmt.Errorf("Create stream fails: %v.", err)
	}else{
		return fmt.Sprintf("Stream %s is created.", stmt.Name), nil
	}
}

func (p *StreamProcessor) execShowStream(stmt *xsql.ShowStreamsStatement, db common.KeyValue) ([]string,error) {
	keys, err := db.Keys()
	if len(keys) == 0 {
		keys = append(keys, "No stream definitions are found.")
	}
	return keys, err
}

func (p *StreamProcessor) execDescribeStream(stmt *xsql.DescribeStreamStatement, db common.KeyValue) (string,error) {
	s, f := db.Get(stmt.Name)
	s1, _ := s.(string)
	if !f {
		return "", fmt.Errorf("Stream %s is not found.", stmt.Name)
	}

	parser := xsql.NewParser(strings.NewReader(s1))
	stream, err := xsql.Language.Parse(parser)
	streamStmt, ok := stream.(*xsql.StreamStmt)
	if !ok{
		return "", fmt.Errorf("Error resolving the stream %s, the data in db may be corrupted.", stmt.Name)
	}
	var buff bytes.Buffer
	buff.WriteString("Fields\n--------------------------------------------------------------------------------\n")
	for _, f := range streamStmt.StreamFields {
		buff.WriteString(f.Name + "\t")
		xsql.PrintFieldType(f.FieldType, &buff)
		buff.WriteString("\n")
	}
	buff.WriteString("\n")
	common.PrintMap(streamStmt.Options, &buff)
	return buff.String(), err
}

func (p *StreamProcessor) execExplainStream(stmt *xsql.ExplainStreamStatement, db common.KeyValue) (string,error) {
	_, f := db.Get(stmt.Name)
	if !f {
		return "", fmt.Errorf("Stream %s is not found.", stmt.Name)
	}
	return "TO BE SUPPORTED", nil
}

func (p *StreamProcessor) execDropStream(stmt *xsql.DropStreamStatement, db common.KeyValue) (string, error) {
	err := db.Delete(stmt.Name)
	if err != nil {
		return "", fmt.Errorf("Drop stream fails: %v.", err)
	}else{
		return fmt.Sprintf("Stream %s is dropped.", stmt.Name), nil
	}
}

func GetStream(m *common.SimpleKVStore, name string) (stmt *xsql.StreamStmt, err error){
	s, f := m.Get(name)
	if !f {
		return nil, fmt.Errorf("Cannot find key %s. ", name)
	}
	s1, _ := s.(string)
	parser := xsql.NewParser(strings.NewReader(s1))
	stream, err := xsql.Language.Parse(parser)
	stmt, ok := stream.(*xsql.StreamStmt)
	if !ok{
		err = fmt.Errorf("Error resolving the stream %s, the data in db may be corrupted.", name)
	}
	return
}


type RuleProcessor struct {
	dbDir string
}

func NewRuleProcessor(d string) *RuleProcessor {
	processor := &RuleProcessor{
		dbDir: d,
	}
	return processor
}

func (p *RuleProcessor) ExecCreate(name, ruleJson string) (*api.Rule, error) {
	rule, err := p.getRuleByJson(name, ruleJson)
	if err != nil {
		return nil, err
	}
	store := common.GetSimpleKVStore(path.Join(p.dbDir, "rule"))
	err = store.Open()
	if err != nil {
		return nil, err
	}
	err = store.Set(string(name), ruleJson)
	defer store.Close()
	if err != nil {
		return nil, err
	}else{
		log.Infof("Rule %s is created.", name)
	}

	return rule, nil
}

func (p *RuleProcessor) GetRuleByName(name string) (*api.Rule, error) {
	store := common.GetSimpleKVStore(path.Join(p.dbDir, "rule"))
	err := store.Open()
	if err != nil {
		return nil, err
	}
	defer store.Close()
	s, f := store.Get(string(name))
	if !f {
		return nil, fmt.Errorf("Rule %s is not found.", name)
	}
	s1, _ := s.(string)
	return p.getRuleByJson(name, s1)
}

func (p *RuleProcessor) getRuleByJson(name, ruleJson string) (*api.Rule, error) {
	var rule api.Rule
	if err := json.Unmarshal([]byte(ruleJson), &rule); err != nil {
		return nil, fmt.Errorf("Parse rule %s error : %s.", ruleJson, err)
	}
	rule.Id = name
	//validation
	if name == ""{
		return nil, fmt.Errorf("Missing rule id.")
	}
	if rule.Sql == ""{
		return nil, fmt.Errorf("Missing rule SQL.")
	}
	if rule.Actions == nil || len(rule.Actions) == 0{
		return nil, fmt.Errorf("Missing rule actions.")
	}
	return &rule, nil
}

func (p *RuleProcessor) ExecInitRule(rule *api.Rule) (*xstream.TopologyNew, error) {
	if tp, inputs, err := p.createTopo(rule); err != nil {
		return nil, err
	}else{
		for _, m := range rule.Actions {
			for name, action := range m {
				props, ok := action.(map[string]interface{})
				if !ok {
					return nil, fmt.Errorf("expect map[string]interface{} type for the action properties, but found %v", action)
				}
				if s, err := getSink(name, props); err != nil{
					return nil, err
				}else{
					tp.AddSink(inputs, nodes.NewSinkNode("sink_" + name, s))
				}
			}
		}
		return tp, nil
	}
}

func (p *RuleProcessor) ExecQuery(ruleid, sql string) (*xstream.TopologyNew, error) {
	if tp, inputs, err := p.createTopo(&api.Rule{Id: ruleid, Sql: sql}); err != nil {
		return nil, err
	} else {
		tp.AddSink(inputs, nodes.NewSinkNode("sink_memory_log", sinks.NewLogSinkToMemory()))
		go func() {
			select {
			case err := <-tp.Open():
				log.Infof("closing query for error: %v", err)
				tp.Cancel()
			}
		}()
		return tp, nil
	}
}

func (p *RuleProcessor) ExecDesc(name string) (string, error) {
	store := common.GetSimpleKVStore(path.Join(p.dbDir, "rule"))
	err := store.Open()
	if err != nil {
		return "", err
	}
	defer store.Close()
	s, f := store.Get(string(name))
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

func (p *RuleProcessor) ExecShow() (string, error) {
	keys, err := p.GetAllRules()
	if err != nil{
		return "", err
	}
	if len(keys) == 0 {
		keys = append(keys, "No rule definitions are found.")
	}
	var result string
	for _, c := range keys{
		result = result + fmt.Sprintln(c)
	}
	return result, nil
}

func (p *RuleProcessor) GetAllRules() ([]string, error) {
	store := common.GetSimpleKVStore(path.Join(p.dbDir, "rule"))
	err := store.Open()
	if err != nil {
		return nil, err
	}
	defer store.Close()
	return store.Keys()
}

func (p *RuleProcessor) ExecDrop(name string) (string, error) {
	store := common.GetSimpleKVStore(path.Join(p.dbDir, "rule"))
	err := store.Open()
	if err != nil {
		return "", err
	}
	defer store.Close()
	err = store.Delete(string(name))
	if err != nil {
		return "", err
	}else{
		return fmt.Sprintf("Rule %s is dropped.", name), nil
	}
}

func (p *RuleProcessor) createTopo(rule *api.Rule) (*xstream.TopologyNew, []api.Emitter, error) {
	return p.createTopoWithSources(rule, nil)
}

//For test to mock source
func (p *RuleProcessor) createTopoWithSources(rule *api.Rule, sources []*nodes.SourceNode) (*xstream.TopologyNew, []api.Emitter, error){
	name := rule.Id
	sql := rule.Sql
	var isEventTime bool
	var lateTol int64
	if iet, ok := rule.Options["isEventTime"]; ok{
		isEventTime, ok = iet.(bool)
		if !ok{
			return nil, nil, fmt.Errorf("Invalid rule option isEventTime %v, bool type is required.", iet)
		}
	}
	if isEventTime {
		if l, ok := rule.Options["lateTolerance"]; ok{
			if fl, ok := l.(float64); ok{
				lateTol = int64(fl)
			}else{
				return nil, nil, fmt.Errorf("Invalid rule option lateTolerance %v, int type is required.", l)
			}
		}
	}
	shouldCreateSource := sources == nil
	parser := xsql.NewParser(strings.NewReader(sql))
	if stmt, err := xsql.Language.Parse(parser); err != nil{
		return nil, nil, fmt.Errorf("Parse SQL %s error: %s.", sql , err)
	}else {
		if selectStmt, ok := stmt.(*xsql.SelectStatement); !ok {
			return nil, nil, fmt.Errorf("SQL %s is not a select statement.", sql)
		} else {
			tp := xstream.NewWithName(name)
			var inputs []api.Emitter
			streamsFromStmt := xsql.GetStreams(selectStmt)
			if !shouldCreateSource && len(streamsFromStmt) != len(sources){
				return nil, nil, fmt.Errorf("Invalid parameter sources or streams, the length cannot match the statement, expect %d sources.", len(streamsFromStmt))
			}
			store := common.GetSimpleKVStore(path.Join(p.dbDir, "stream"))
			err := store.Open()
			if err != nil {
				return nil, nil, err
			}
			defer store.Close()

			for i, s := range streamsFromStmt {
				streamStmt, err := GetStream(store, s)
				if err != nil {
					return nil, nil, fmt.Errorf("fail to get stream %s, please check if stream is created", s)
				}
				pp, err := plans.NewPreprocessor(streamStmt, selectStmt.Fields, isEventTime)
				if err != nil{
					return nil, nil, err
				}
				if shouldCreateSource{
					src, err := getSource(streamStmt)
					if err != nil {
						return nil, nil, fmt.Errorf("fail to get source: %v", err)
					}
					node := nodes.NewSourceNode(s, src, streamStmt.Options)
					tp.AddSrc(node)
					preprocessorOp := xstream.Transform(pp, "preprocessor_"+s)
					tp.AddOperator([]api.Emitter{node}, preprocessorOp)
					inputs = append(inputs, preprocessorOp)
				} else {
					tp.AddSrc(sources[i])
					preprocessorOp := xstream.Transform(pp, "preprocessor_"+s)
					tp.AddOperator([]api.Emitter{sources[i]}, preprocessorOp)
					inputs = append(inputs, preprocessorOp)
				}
			}
			dimensions := selectStmt.Dimensions
			var w *xsql.Window
			if dimensions != nil {
				w = dimensions.GetWindow()
				if w != nil {
					wop, err := operators.NewWindowOp("window", w, isEventTime, lateTol, streamsFromStmt)
					if err != nil {
						return nil, nil, err
					}
					tp.AddOperator(inputs, wop)
					inputs = []api.Emitter{wop}
				}
			}

			if w != nil && selectStmt.Joins != nil {
				joinOp := xstream.Transform(&plans.JoinPlan{Joins: selectStmt.Joins, From: selectStmt.Sources[0].(*xsql.Table)}, "join")
				//TODO concurrency setting by command
				//joinOp.SetConcurrency(3)
				tp.AddOperator(inputs, joinOp)
				inputs = []api.Emitter{joinOp}
			}

			if selectStmt.Condition != nil {
				filterOp := xstream.Transform(&plans.FilterPlan{Condition: selectStmt.Condition}, "filter")
				//TODO concurrency setting by command
				// filterOp.SetConcurrency(3)
				tp.AddOperator(inputs, filterOp)
				inputs = []api.Emitter{filterOp}
			}

			var ds xsql.Dimensions
			if dimensions != nil {
				ds = dimensions.GetGroups()
				if ds != nil && len(ds) > 0 {
					aggregateOp := xstream.Transform(&plans.AggregatePlan{Dimensions: ds}, "aggregate")
					tp.AddOperator(inputs, aggregateOp)
					inputs = []api.Emitter{aggregateOp}
				}
			}

			if selectStmt.Having != nil {
				havingOp := xstream.Transform(&plans.HavingPlan{selectStmt.Having}, "having")
				tp.AddOperator(inputs, havingOp)
				inputs = []api.Emitter{havingOp}
			}

			if selectStmt.SortFields != nil {
				orderOp := xstream.Transform(&plans.OrderPlan{SortFields: selectStmt.SortFields}, "order")
				tp.AddOperator(inputs, orderOp)
				inputs = []api.Emitter{orderOp}
			}

			if selectStmt.Fields != nil {
				projectOp := xstream.Transform(&plans.ProjectPlan{Fields: selectStmt.Fields, IsAggregate: xsql.IsAggStatement(selectStmt)}, "project")
				tp.AddOperator(inputs, projectOp)
				inputs = []api.Emitter{projectOp}
			}
			return tp, inputs, nil
		}
	}
}

func getSource(streamStmt *xsql.StreamStmt) (api.Source, error) {
	t, ok := streamStmt.Options["TYPE"]
	if !ok{
		t = "mqtt"
	}
	var s api.Source
	switch t {
	case "mqtt":
		s = &extensions.MQTTSource{}
	default:
		nf, err := plugin_manager.GetPlugin(t, "sources")
		if err != nil {
			return nil, err
		}
		s, ok = nf.(api.Source)
		if !ok {
			return nil, fmt.Errorf("exported symbol %s is not type of api.Source", t)
		}
	}
	log.Debugf("Source %s created", t)
	return s, nil
}

func getSink(name string, action map[string]interface{}) (api.Sink, error) {
	log.Tracef("trying to get sink %s with action %v", name, action)
	var s api.Sink
	switch name {
	case "log":
		s = sinks.NewLogSink()
	case "mqtt":
		s = &sinks.MQTTSink{}
	case "rest":
		s = &sinks.RestSink{}
	default:
		nf, err := plugin_manager.GetPlugin(name, "sinks")
		if err != nil {
			return nil, err
		}
		var ok bool
		s, ok = nf.(api.Sink)
		if !ok {
			return nil, fmt.Errorf("exported symbol %s is not type of api.Sink", name)
		}
	}

	err := s.Configure(action)
	if err != nil{
		return nil, err
	}
	log.Debugf("Sink %s created", name)
	return s, nil
}


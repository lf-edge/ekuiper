package processors

import (
	"bytes"
	"encoding/json"
	"engine/common"
	"engine/xsql"
	"engine/xsql/plans"
	"engine/xstream"
	"engine/xstream/extensions"
	"engine/xstream/operators"
	"engine/xstream/sinks"
	"fmt"
	"github.com/dgraph-io/badger"
	"path"
	"strings"
)

var log = common.Log

type StreamProcessor struct {
	statement string
	badgerDir string
}

//@params s : the sql string of create stream statement
//@params d : the directory of the badger DB to save the stream info
func NewStreamProcessor(s, d string) *StreamProcessor {
	processor := &StreamProcessor{
		statement: s,
		badgerDir: d,
	}
	return processor
}


func (p *StreamProcessor) Exec() (result []string, err error) {
	parser := xsql.NewParser(strings.NewReader(p.statement))
	stmt, err := xsql.Language.Parse(parser)
	if err != nil {
		return
	}

	db, err := common.DbOpen(p.badgerDir)
	if err != nil {
		return
	}
	defer common.DbClose(db)

	switch s := stmt.(type) {
	case *xsql.StreamStmt:
		var r string
		r, err = p.execCreateStream(s, db)
		result = append(result, r)
	case *xsql.ShowStreamsStatement:
		result, err = p.execShowStream(s, db)
	case *xsql.DescribeStreamStatement:
		var r string
		r, err = p.execDescribeStream(s, db)
		result = append(result, r)
	case *xsql.ExplainStreamStatement:
		var r string
		r, err = p.execExplainStream(s, db)
		result = append(result, r)
	case *xsql.DropStreamStatement:
		var r string
		r, err = p.execDropStream(s, db)
		result = append(result, r)
	}

	return
}

func (p *StreamProcessor) execCreateStream(stmt *xsql.StreamStmt, db *badger.DB) (string, error) {
	err := common.DbSet(db, string(stmt.Name), p.statement)
	if err != nil {
		return "", err
	}else{
		return fmt.Sprintf("stream %s created", stmt.Name), nil
	}
}

func (p *StreamProcessor) execShowStream(stmt *xsql.ShowStreamsStatement, db *badger.DB) ([]string,error) {
	keys, err := common.DbKeys(db)
	if len(keys) == 0 {
		keys = append(keys, "no stream definition found")
	}
	return keys, err
}

func (p *StreamProcessor) execDescribeStream(stmt *xsql.DescribeStreamStatement, db *badger.DB) (string,error) {
	s, err := common.DbGet(db, string(stmt.Name))
	if err != nil {
		return "", fmt.Errorf("stream %s not found", stmt.Name)
	}

	parser := xsql.NewParser(strings.NewReader(s))
	stream, err := xsql.Language.Parse(parser)
	streamStmt, ok := stream.(*xsql.StreamStmt)
	if !ok{
		return "", fmt.Errorf("error resolving the stream %s, the data in db may be corrupted", stmt.Name)
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

func (p *StreamProcessor) execExplainStream(stmt *xsql.ExplainStreamStatement, db *badger.DB) (string,error) {
	_, err := common.DbGet(db, string(stmt.Name))
	if err != nil{
		return "", fmt.Errorf("stream %s not found", stmt.Name)
	}
	return "TO BE SUPPORTED", nil
}

func (p *StreamProcessor) execDropStream(stmt *xsql.DropStreamStatement, db *badger.DB) (string, error) {
	err := common.DbDelete(db, string(stmt.Name))
	if err != nil {
		return "", err
	}else{
		return fmt.Sprintf("stream %s dropped", stmt.Name), nil
	}
}

func GetStream(db *badger.DB, name string) (stmt *xsql.StreamStmt, err error){
	s, err := common.DbGet(db, name)
	if err != nil {
		return
	}

	parser := xsql.NewParser(strings.NewReader(s))
	stream, err := xsql.Language.Parse(parser)
	stmt, ok := stream.(*xsql.StreamStmt)
	if !ok{
		err = fmt.Errorf("error resolving the stream %s, the data in db may be corrupted", name)
	}
	return
}


type RuleProcessor struct {
	badgerDir string
}

func NewRuleProcessor(d string) *RuleProcessor {
	processor := &RuleProcessor{
		badgerDir: d,
	}
	return processor
}

func (p *RuleProcessor) ExecCreate(name, ruleJson string) (*xstream.Rule, error) {
	rule, err := p.getRuleByJson(name, ruleJson)
	if err != nil {
		return nil, err
	}
	db, err := common.DbOpen(path.Join(p.badgerDir, "rule"))
	if err != nil {
		return nil, err
	}
	err = common.DbSet(db, string(name), ruleJson)
	if err != nil {
		common.DbClose(db)
		return nil, err
	}else{
		log.Infof("rule %s created", name)
		common.DbClose(db)
	}
	return rule, nil
}

func (p *RuleProcessor) GetRuleByName(name string) (*xstream.Rule, error) {
	db, err := common.DbOpen(path.Join(p.badgerDir, "rule"))
	if err != nil {
		return nil, err
	}
	defer common.DbClose(db)
	s, err := common.DbGet(db, string(name))
	if err != nil {
		return nil, fmt.Errorf("rule %s not found", name)
	}
	return p.getRuleByJson(name, s)
}

func (p *RuleProcessor) getRuleByJson(name, ruleJson string) (*xstream.Rule, error) {
	var rule xstream.Rule
	if err := json.Unmarshal([]byte(ruleJson), &rule); err != nil {
		return nil, fmt.Errorf("parse rule %s error : %s", ruleJson, err)
	}
	rule.Id = name
	//validation
	if name == ""{
		return nil, fmt.Errorf("missing rule id")
	}
	if rule.Sql == ""{
		return nil, fmt.Errorf("missing rule sql")
	}
	if rule.Actions == nil || len(rule.Actions) == 0{
		return nil, fmt.Errorf("missing rule actions")
	}
	return &rule, nil
}

func (p *RuleProcessor) ExecInitRule(rule *xstream.Rule) (*xstream.TopologyNew, error) {
	if tp, inputs, err := p.createTopo(rule); err != nil {
		return nil, err
	}else{
		for _, m := range rule.Actions {
			for name, action := range m {
				switch name {
				case "log":
					log.Printf("Create log sink with %s", action)
					tp.AddSink(inputs, sinks.NewLogSink("sink_log", rule.Id))
				case "mqtt":
					log.Printf("Create mqtt sink with %s", action)
					if ms, err := sinks.NewMqttSink("mqtt_log", rule.Id, action); err != nil{
						return nil, err
					}else{
						tp.AddSink(inputs, ms)
					}
				default:
					return nil, fmt.Errorf("unsupported action: %s", name)
				}
			}
		}
		return tp, nil
	}
}

func (p *RuleProcessor) ExecQuery(ruleid, sql string) (*xstream.TopologyNew, error) {
	if tp, inputs, err := p.createTopo(&xstream.Rule{Id: ruleid, Sql: sql}); err != nil {
		return nil, err
	} else {
		tp.AddSink(inputs, sinks.NewLogSinkToMemory("sink_log", ruleid))
		go func() {
			select {
			case err := <-tp.Open():
				log.Println(err)
				tp.Cancel()
			}
		}()
		return tp, nil
	}
}

func (p *RuleProcessor) ExecDesc(name string) (string, error) {
	db, err := common.DbOpen(path.Join(p.badgerDir, "rule"))
	if err != nil {
		return "", err
	}
	defer common.DbClose(db)
	s, err := common.DbGet(db, string(name))
	if err != nil {
		return "", fmt.Errorf("rule %s not found", name)
	}
	dst := &bytes.Buffer{}
	if err := json.Indent(dst, []byte(s), "", "  "); err != nil {
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
		keys = append(keys, "no rule definition found")
	}
	var result string
	for _, c := range keys{
		result = result + fmt.Sprintln(c)
	}
	return result, nil
}

func (p *RuleProcessor) GetAllRules() ([]string, error) {
	db, err := common.DbOpen(path.Join(p.badgerDir, "rule"))
	if err != nil {
		return nil, err
	}
	defer common.DbClose(db)
	return common.DbKeys(db)
}

func (p *RuleProcessor) ExecDrop(name string) (string, error) {
	db, err := common.DbOpen(path.Join(p.badgerDir, "rule"))
	if err != nil {
		return "", err
	}
	defer common.DbClose(db)
	err = common.DbDelete(db, string(name))
	if err != nil {
		return "", err
	}else{
		return fmt.Sprintf("rule %s dropped", name), nil
	}
}

func (p *RuleProcessor) createTopo(rule *xstream.Rule) (*xstream.TopologyNew, []xstream.Emitter, error) {
	return p.createTopoWithSources(rule, nil)
}

//For test to mock source
func (p *RuleProcessor) createTopoWithSources(rule *xstream.Rule, sources []xstream.Source) (*xstream.TopologyNew, []xstream.Emitter, error){
	name := rule.Id
	sql := rule.Sql
	var isEventTime bool
	var lateTol int64
	if iet, ok := rule.Options["isEventTime"]; ok{
		isEventTime, ok = iet.(bool)
		if !ok{
			return nil, nil, fmt.Errorf("invalid rule option isEventTime %v, bool type required", iet)
		}
	}
	if isEventTime {
		if l, ok := rule.Options["lateTolerance"]; ok{
			if fl, ok := l.(float64); ok{
				lateTol = int64(fl)
			}else{
				return nil, nil, fmt.Errorf("invalid rule option lateTolerance %v, int type required", l)
			}
		}
	}
	shouldCreateSource := sources == nil
	parser := xsql.NewParser(strings.NewReader(sql))
	if stmt, err := xsql.Language.Parse(parser); err != nil{
		return nil, nil, fmt.Errorf("parse sql %s error: %s", sql , err)
	}else {
		if selectStmt, ok := stmt.(*xsql.SelectStatement); !ok {
			return nil, nil, fmt.Errorf("sql %s is not a select statement", sql)
		} else {
			tp := xstream.NewWithName(name)
			var inputs []xstream.Emitter
			streamsFromStmt := xsql.GetStreams(selectStmt)
			if !shouldCreateSource && len(streamsFromStmt) != len(sources){
				return nil, nil, fmt.Errorf("invalid parameter sources or streams, the length cannot match the statement, expect %d sources", len(streamsFromStmt))
			}
			db, err := common.DbOpen(path.Join(p.badgerDir, "stream"))
			if err != nil {
				return nil, nil, err
			}
			defer common.DbClose(db)

			for i, s := range streamsFromStmt {
				streamStmt, err := GetStream(db, s)
				if err != nil {
					return nil, nil, err
				}
				pp, err := plans.NewPreprocessor(streamStmt, selectStmt.Fields, isEventTime)
				if err != nil{
					return nil, nil, err
				}
				if shouldCreateSource{
					mqs, err := extensions.NewWithName(string(streamStmt.Name), streamStmt.Options["DATASOURCE"], streamStmt.Options["CONF_KEY"])
					if err != nil {
						return nil, nil, err
					}
					tp.AddSrc(mqs)
					preprocessorOp := xstream.Transform(pp, "preprocessor_"+s)
					tp.AddOperator([]xstream.Emitter{mqs}, preprocessorOp)
					inputs = append(inputs, preprocessorOp)
				}else{
					tp.AddSrc(sources[i])
					preprocessorOp := xstream.Transform(pp, "preprocessor_"+s)
					tp.AddOperator([]xstream.Emitter{sources[i]}, preprocessorOp)
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
					inputs = []xstream.Emitter{wop}
				}
			}

			if w != nil && selectStmt.Joins != nil {
				joinOp := xstream.Transform(&plans.JoinPlan{Joins: selectStmt.Joins, From: selectStmt.Sources[0].(*xsql.Table)}, "join")
				//TODO concurrency setting by command
				//joinOp.SetConcurrency(3)
				tp.AddOperator(inputs, joinOp)
				inputs = []xstream.Emitter{joinOp}
			}

			if selectStmt.Condition != nil {
				filterOp := xstream.Transform(&plans.FilterPlan{Condition: selectStmt.Condition}, "filter")
				//TODO concurrency setting by command
				// filterOp.SetConcurrency(3)
				tp.AddOperator(inputs, filterOp)
				inputs = []xstream.Emitter{filterOp}
			}

			var ds xsql.Dimensions
			if dimensions != nil {
				ds = dimensions.GetGroups()
				if ds != nil && len(ds) > 0 {
					aggregateOp := xstream.Transform(&plans.AggregatePlan{Dimensions: ds}, "aggregate")
					tp.AddOperator(inputs, aggregateOp)
					inputs = []xstream.Emitter{aggregateOp}
				}
			}

			if selectStmt.Having != nil {
				havingOp := xstream.Transform(&plans.HavingPlan{selectStmt.Having}, "having")
				tp.AddOperator(inputs, havingOp)
				inputs = []xstream.Emitter{havingOp}
			}

			if selectStmt.SortFields != nil {
				orderOp := xstream.Transform(&plans.OrderPlan{SortFields:selectStmt.SortFields}, "order")
				tp.AddOperator(inputs, orderOp)
				inputs = []xstream.Emitter{orderOp}
			}

			if selectStmt.Fields != nil {
				projectOp := xstream.Transform(&plans.ProjectPlan{Fields: selectStmt.Fields, IsAggregate: xsql.IsAggStatement(selectStmt)}, "project")
				tp.AddOperator(inputs, projectOp)
				inputs = []xstream.Emitter{projectOp}
			}
			return tp, inputs, nil
		}
	}
}


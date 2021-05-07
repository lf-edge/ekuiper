package processors

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/common/kv"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"github.com/emqx/kuiper/xstream/planner"
	"os"
	"path"
	"strings"
)

var (
	log = common.Log
)

type StreamProcessor struct {
	db kv.KeyValue
}

//@params d : the directory of the DB to save the stream info
func NewStreamProcessor(d string) *StreamProcessor {
	processor := &StreamProcessor{
		db: kv.GetDefaultKVStore(d),
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
	case *xsql.StreamStmt: //Table is also StreamStmt
		var r string
		err = p.execSave(s, statement, false)
		stt := xsql.StreamTypeMap[s.StreamType]
		if err != nil {
			err = fmt.Errorf("Create %s fails: %v.", stt, err)
		} else {
			r = fmt.Sprintf("%s %s is created.", strings.Title(stt), s.Name)
			log.Printf("%s", r)
		}
		result = append(result, r)
	case *xsql.ShowStreamsStatement:
		result, err = p.execShow(xsql.TypeStream)
	case *xsql.ShowTablesStatement:
		result, err = p.execShow(xsql.TypeTable)
	case *xsql.DescribeStreamStatement:
		var r string
		r, err = p.execDescribe(s, xsql.TypeStream)
		result = append(result, r)
	case *xsql.DescribeTableStatement:
		var r string
		r, err = p.execDescribe(s, xsql.TypeTable)
		result = append(result, r)
	case *xsql.ExplainStreamStatement:
		var r string
		r, err = p.execExplain(s, xsql.TypeStream)
		result = append(result, r)
	case *xsql.ExplainTableStatement:
		var r string
		r, err = p.execExplain(s, xsql.TypeTable)
		result = append(result, r)
	case *xsql.DropStreamStatement:
		var r string
		r, err = p.execDrop(s, xsql.TypeStream)
		result = append(result, r)
	case *xsql.DropTableStatement:
		var r string
		r, err = p.execDrop(s, xsql.TypeTable)
		result = append(result, r)
	default:
		return nil, fmt.Errorf("Invalid stream statement: %s", statement)
	}

	return
}

func (p *StreamProcessor) execSave(stmt *xsql.StreamStmt, statement string, replace bool) error {
	err := p.db.Open()
	if err != nil {
		return fmt.Errorf("error when opening db: %v.", err)
	}
	defer p.db.Close()
	s, err := json.Marshal(xsql.StreamInfo{
		StreamType: stmt.StreamType,
		Statement:  statement,
	})
	if err != nil {
		return fmt.Errorf("error when saving to db: %v.", err)
	}
	if replace {
		err = p.db.Set(string(stmt.Name), string(s))
	} else {
		err = p.db.Setnx(string(stmt.Name), string(s))
	}
	return err
}

func (p *StreamProcessor) ExecReplaceStream(statement string, st xsql.StreamType) (string, error) {
	parser := xsql.NewParser(strings.NewReader(statement))
	stmt, err := xsql.Language.Parse(parser)
	if err != nil {
		return "", err
	}
	stt := xsql.StreamTypeMap[st]
	switch s := stmt.(type) {
	case *xsql.StreamStmt:
		if s.StreamType != st {
			return "", common.NewErrorWithCode(common.NOT_FOUND, fmt.Sprintf("%s %s is not found", xsql.StreamTypeMap[st], s.Name))
		}
		err = p.execSave(s, statement, true)
		if err != nil {
			return "", fmt.Errorf("Replace %s fails: %v.", stt, err)
		} else {
			info := fmt.Sprintf("%s %s is replaced.", strings.Title(stt), s.Name)
			log.Printf("%s", info)
			return info, nil
		}
	default:
		return "", fmt.Errorf("Invalid %s statement: %s", stt, statement)
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

func (p *StreamProcessor) execShow(st xsql.StreamType) ([]string, error) {
	keys, err := p.ShowStream(st)
	if len(keys) == 0 {
		keys = append(keys, fmt.Sprintf("No %s definitions are found.", xsql.StreamTypeMap[st]))
	}
	return keys, err
}

func (p *StreamProcessor) ShowStream(st xsql.StreamType) ([]string, error) {
	stt := xsql.StreamTypeMap[st]
	err := p.db.Open()
	if err != nil {
		return nil, fmt.Errorf("Show %ss fails, error when opening db: %v.", stt, err)
	}
	defer p.db.Close()
	keys, err := p.db.Keys()
	if err != nil {
		return nil, fmt.Errorf("Show %ss fails, error when loading data from db: %v.", stt, err)
	}
	var (
		v      string
		vs     = &xsql.StreamInfo{}
		result = make([]string, 0)
	)
	for _, k := range keys {
		if ok, _ := p.db.Get(k, &v); ok {
			if err := json.Unmarshal([]byte(v), vs); err == nil && vs.StreamType == st {
				result = append(result, k)
			}
		}
	}
	return result, nil
}

func (p *StreamProcessor) getStream(name string, st xsql.StreamType) (string, error) {
	vs, err := xsql.GetDataSourceStatement(p.db, name)
	if vs != nil && vs.StreamType == st {
		return vs.Statement, nil
	}
	if err != nil {
		return "", err
	}
	return "", common.NewErrorWithCode(common.NOT_FOUND, fmt.Sprintf("%s %s is not found", xsql.StreamTypeMap[st], name))
}

func (p *StreamProcessor) execDescribe(stmt xsql.NameNode, st xsql.StreamType) (string, error) {
	streamStmt, err := p.DescStream(stmt.GetName(), st)
	if err != nil {
		return "", err
	}
	switch s := streamStmt.(type) {
	case *xsql.StreamStmt:
		var buff bytes.Buffer
		buff.WriteString("Fields\n--------------------------------------------------------------------------------\n")
		for _, f := range s.StreamFields {
			buff.WriteString(f.Name + "\t")
			buff.WriteString(xsql.PrintFieldType(f.FieldType))
			buff.WriteString("\n")
		}
		buff.WriteString("\n")
		printOptions(s.Options, &buff)
		return buff.String(), err
	default:
		return "%s", fmt.Errorf("Error resolving the %s %s, the data in db may be corrupted.", xsql.StreamTypeMap[st], stmt.GetName())
	}

}

func printOptions(opts *xsql.Options, buff *bytes.Buffer) {
	if opts.CONF_KEY != "" {
		buff.WriteString(fmt.Sprintf("CONF_KEY: %s\n", opts.CONF_KEY))
	}
	if opts.DATASOURCE != "" {
		buff.WriteString(fmt.Sprintf("DATASOURCE: %s\n", opts.DATASOURCE))
	}
	if opts.FORMAT != "" {
		buff.WriteString(fmt.Sprintf("FORMAT: %s\n", opts.FORMAT))
	}
	if opts.KEY != "" {
		buff.WriteString(fmt.Sprintf("KEY: %s\n", opts.KEY))
	}
	if opts.RETAIN_SIZE != 0 {
		buff.WriteString(fmt.Sprintf("RETAIN_SIZE: %d\n", opts.RETAIN_SIZE))
	}
	if opts.STRICT_VALIDATION {
		buff.WriteString(fmt.Sprintf("STRICT_VALIDATION: %v\n", opts.STRICT_VALIDATION))
	}
	if opts.TIMESTAMP != "" {
		buff.WriteString(fmt.Sprintf("TIMESTAMP: %s\n", opts.TIMESTAMP))
	}
	if opts.TIMESTAMP_FORMAT != "" {
		buff.WriteString(fmt.Sprintf("TIMESTAMP_FORMAT: %s\n", opts.TIMESTAMP_FORMAT))
	}
	if opts.TYPE != "" {
		buff.WriteString(fmt.Sprintf("TYPE: %s\n", opts.TYPE))
	}
}

func (p *StreamProcessor) DescStream(name string, st xsql.StreamType) (xsql.Statement, error) {
	statement, err := p.getStream(name, st)
	if err != nil {
		return nil, fmt.Errorf("Describe %s fails, %s.", xsql.StreamTypeMap[st], err)
	}
	parser := xsql.NewParser(strings.NewReader(statement))
	stream, err := xsql.Language.Parse(parser)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (p *StreamProcessor) execExplain(stmt xsql.NameNode, st xsql.StreamType) (string, error) {
	_, err := p.getStream(stmt.GetName(), st)
	if err != nil {
		return "", fmt.Errorf("Explain %s fails, %s.", xsql.StreamTypeMap[st], err)
	}
	return "TO BE SUPPORTED", nil
}

func (p *StreamProcessor) execDrop(stmt xsql.NameNode, st xsql.StreamType) (string, error) {
	s, err := p.DropStream(stmt.GetName(), st)
	if err != nil {
		return s, fmt.Errorf("Drop %s fails: %s.", xsql.StreamTypeMap[st], err)
	}
	return s, nil
}

func (p *StreamProcessor) DropStream(name string, st xsql.StreamType) (string, error) {
	defer p.db.Close()
	_, err := p.getStream(name, st)
	if err != nil {
		return "", err
	}

	err = p.db.Open()
	if err != nil {
		return "", fmt.Errorf("error when opening db: %v", err)
	}
	defer p.db.Close()
	err = p.db.Delete(name)
	if err != nil {
		return "", err
	} else {
		return fmt.Sprintf("%s %s is dropped.", strings.Title(xsql.StreamTypeMap[st]), name), nil
	}
}

type RuleProcessor struct {
	db        kv.KeyValue
	rootDbDir string
}

func NewRuleProcessor(d string) *RuleProcessor {
	processor := &RuleProcessor{
		db:        kv.GetDefaultKVStore(path.Join(d, "rule")),
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

	err = p.db.Setnx(rule.Id, ruleJson)
	if err != nil {
		return nil, err
	} else {
		log.Infof("Rule %s is created.", rule.Id)
	}

	return rule, nil
}
func (p *RuleProcessor) ExecUpdate(name, ruleJson string) (*api.Rule, error) {
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
		log.Infof("Rule %s is update.", rule.Id)
	}

	return rule, nil
}

func (p *RuleProcessor) ExecReplaceRuleState(name string, triggered bool) (err error) {
	rule, err := p.GetRuleByName(name)
	if err != nil {
		return err
	}

	rule.Triggered = triggered
	ruleJson, err := json.Marshal(rule)
	if err != nil {
		return fmt.Errorf("Marshal rule %s error : %s.", name, err)
	}

	err = p.db.Open()
	if err != nil {
		return err
	}
	defer p.db.Close()

	err = p.db.Set(name, string(ruleJson))
	if err != nil {
		return err
	} else {
		log.Infof("Rule %s is replaced.", name)
	}
	return err
}

func (p *RuleProcessor) GetRuleByName(name string) (*api.Rule, error) {
	err := p.db.Open()
	if err != nil {
		return nil, err
	}
	defer p.db.Close()
	var s1 string
	f, _ := p.db.Get(name, &s1)
	if !f {
		return nil, common.NewErrorWithCode(common.NOT_FOUND, fmt.Sprintf("Rule %s is not found.", name))
	}
	return p.getRuleByJson(name, s1)
}

func (p *RuleProcessor) getDefaultRule(name, sql string) *api.Rule {
	return &api.Rule{
		Id:  name,
		Sql: sql,
		Options: &api.RuleOption{
			IsEventTime:        false,
			LateTol:            1000,
			Concurrency:        1,
			BufferLength:       1024,
			SendMetaToSink:     false,
			SendError:          true,
			Qos:                api.AtMostOnce,
			CheckpointInterval: 300000,
		},
	}
}

func (p *RuleProcessor) getRuleByJson(name, ruleJson string) (*api.Rule, error) {
	opt := common.Config.Rule
	//set default rule options
	rule := &api.Rule{
		Options: &opt,
	}
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
	if _, err := xsql.GetStatementFromSql(rule.Sql); err != nil {
		return nil, err
	}
	if rule.Actions == nil || len(rule.Actions) == 0 {
		return nil, fmt.Errorf("Missing rule actions.")
	}
	if rule.Options == nil {
		rule.Options = &api.RuleOption{}
	}
	//Setnx default options
	if rule.Options.CheckpointInterval < 0 {
		return nil, fmt.Errorf("rule option checkpointInterval %d is invalid, require a positive integer", rule.Options.CheckpointInterval)
	}
	if rule.Options.Concurrency < 0 {
		return nil, fmt.Errorf("rule option concurrency %d is invalid, require a positive integer", rule.Options.Concurrency)
	}
	if rule.Options.BufferLength < 0 {
		return nil, fmt.Errorf("rule option bufferLength %d is invalid, require a positive integer", rule.Options.BufferLength)
	}
	if rule.Options.LateTol < 0 {
		return nil, fmt.Errorf("rule option lateTolerance %d is invalid, require a positive integer", rule.Options.LateTol)
	}
	return rule, nil
}

func (p *RuleProcessor) ExecQuery(ruleid, sql string) (*xstream.TopologyNew, error) {
	if tp, err := planner.PlanWithSourcesAndSinks(p.getDefaultRule(ruleid, sql), p.rootDbDir, nil, []*nodes.SinkNode{nodes.NewSinkNode("sink_memory_log", "logToMemory", nil)}); err != nil {
		return nil, err
	} else {
		go func() {
			select {
			case err := <-tp.Open():
				if err != nil {
					log.Infof("closing query for error: %v", err)
					tp.GetContext().SetError(err)
					tp.Cancel()
				} else {
					log.Info("closing query")
				}
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
	var s1 string
	f, _ := p.db.Get(name, &s1)
	if !f {
		return "", fmt.Errorf("Rule %s is not found.", name)
	}
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
	result := fmt.Sprintf("Rule %s is dropped.", name)
	var ruleJson string
	if ok, _ := p.db.Get(name, &ruleJson); ok {
		rule, err := p.getRuleByJson(name, ruleJson)
		if err != nil {
			return "", err
		}
		if err := cleanSinkCache(rule); err != nil {
			result = fmt.Sprintf("%s. Clean sink cache faile: %s.", result, err)
		}
		if err := cleanCheckpoint(name); err != nil {
			result = fmt.Sprintf("%s. Clean checkpoint cache faile: %s.", result, err)
		}
	}
	err = p.db.Delete(name)
	if err != nil {
		return "", err
	} else {
		return result, nil
	}
}

func cleanCheckpoint(name string) error {
	dbDir, _ := common.GetDataLoc()
	c := path.Join(dbDir, "checkpoints", name)
	return os.RemoveAll(c)
}

func cleanSinkCache(rule *api.Rule) error {
	dbDir, err := common.GetDataLoc()
	if err != nil {
		return err
	}
	store := kv.GetDefaultKVStore(path.Join(dbDir, "sink"))
	err = store.Open()
	if err != nil {
		return err
	}
	defer store.Close()
	for d, m := range rule.Actions {
		con := 1
		for name, action := range m {
			props, _ := action.(map[string]interface{})
			if c, ok := props["concurrency"]; ok {
				if t, err := common.ToInt(c, common.STRICT); err == nil && t > 0 {
					con = t
				}
			}
			for i := 0; i < con; i++ {
				key := fmt.Sprintf("%s%s_%d%d", rule.Id, name, d, i)
				common.Log.Debugf("delete cache key %s", key)
				store.Delete(key)
			}
		}
	}
	return nil
}

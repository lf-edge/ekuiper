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

var log = common.Log

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
	err = p.db.Setnx(string(stmt.Name), statement)
	if err != nil {
		return "", fmt.Errorf("Create stream fails: %v.", err)
	} else {
		info := fmt.Sprintf("Stream %s is created.", stmt.Name)
		log.Printf("%s", info)
		return info, nil
	}
}

func (p *StreamProcessor) ExecReplaceStream(statement string) (string, error) {
	parser := xsql.NewParser(strings.NewReader(statement))
	stmt, err := xsql.Language.Parse(parser)
	if err != nil {
		return "", err
	}

	switch s := stmt.(type) {
	case *xsql.StreamStmt:
		if err = p.db.Open(); nil != err {
			return "", fmt.Errorf("Replace stream fails, error when opening db: %v.", err)
		}
		defer p.db.Close()

		if err = p.db.Set(string(s.Name), statement); nil != err {
			return "", fmt.Errorf("Replace stream fails: %v.", err)
		} else {
			info := fmt.Sprintf("Stream %s is replaced.", s.Name)
			log.Printf("%s", info)
			return info, nil
		}
	default:
		return "", fmt.Errorf("Invalid stream statement: %s", statement)
	}
	return "", nil
}

func (p *StreamProcessor) ExecStreamSql(statement string) (string, error) {
	r, err := p.ExecStmt(statement)
	if err != nil {
		return "", err
	} else {
		return strings.Join(r, "\n"), err
	}
}

func (p *StreamProcessor) execShowStream(_ *xsql.ShowStreamsStatement) ([]string, error) {
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
	var s1 string
	f, _ := p.db.Get(name, &s1)
	if !f {
		return nil, common.NewErrorWithCode(common.NOT_FOUND, fmt.Sprintf("Stream %s is not found.", name))
	}

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
	var s string
	f, _ := p.db.Get(stmt.Name, &s)
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
				if t, err := common.ToInt(c); err == nil && t > 0 {
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

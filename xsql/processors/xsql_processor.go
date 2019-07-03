package processors

import (
	"bytes"
	"engine/common"
	"engine/xsql"
	"engine/xsql/plans"
	"engine/xstream"
	"engine/xstream/collectors"
	"engine/xstream/extensions"
	"fmt"
	"github.com/dgraph-io/badger"
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
	s, err := common.DbGet(db, string(name))
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
	sql string
//	actions string
	badgerDir string
}

func NewRuleProcessor(s, d string) *RuleProcessor {
	processor := &RuleProcessor{
		sql: s,
		badgerDir: d,
	}
	return processor
}

func (p *RuleProcessor) Exec() error {
	parser := xsql.NewParser(strings.NewReader(p.sql))
	if stmt, err := xsql.Language.Parse(parser); err != nil{
		return fmt.Errorf("parse sql %s error: %s", p.sql , err)
	}else{
		if selectStmt, ok := stmt.(*xsql.SelectStatement); !ok{
			return fmt.Errorf("sql %s is not a select statement", p.sql)
		}else{
			//TODO Validation here or in the cli?
			tp := xstream.New()

			//create sources and preprocessor
			db, err := common.DbOpen(p.badgerDir)
			if err != nil {
				return err
			}
			defer common.DbClose(db)
			var inputs []xstream.Emitter
			for _, s := range selectStmt.Sources {
				switch t := s.(type){
				case *xsql.Table:
					if streamStmt, err := GetStream(db, t.Name); err != nil{
						return err
					} else {
						mqs, err := extensions.NewWithName(string(streamStmt.Name), streamStmt.Options["DATASOURCE"], streamStmt.Options["CONF_KEY"])
						if err != nil {
							return err
						}
						tp.AddSrc(mqs)

						preprocessorOp := xstream.Transform(&plans.Preprocessor{StreamStmt: streamStmt}, "preprocessor_" +t.Name)
						tp.AddOperator([]xstream.Emitter{mqs}, preprocessorOp)
						inputs = append(inputs, preprocessorOp)
					}
				default:
					return fmt.Errorf("unsupported source type %T", s)
				}
			}

			//if selectStmt.Joins != nil {
			//	for _, join := range selectStmt.Joins {
			//		if streamStmt, err := GetStream(db, join.Name); err != nil{
			//			return err
			//		} else {
			//			mqs, err := extensions.NewWithName(string(streamStmt.Name), streamStmt.Options["DATASOURCE"], streamStmt.Options["CONF_KEY"])
			//			if err != nil {
			//				return err
			//			}
			//			tp.AddSrc(mqs)
			//
			//			preprocessorOp := xstream.Transform(&plans.Preprocessor{StreamStmt: streamStmt}, "preprocessor_" + join.Name)
			//			tp.AddOperator([]xstream.Emitter{mqs}, preprocessorOp)
			//			inputs = append(inputs, preprocessorOp)
			//		}
			//	}
			//
			//	joinOp := xstream.Transform(&plans.JoinPlan{Joins:selectStmt.Joins, Dimensions: selectStmt.Dimensions}, "join")
			//	//TODO concurrency setting by command
			//	//joinOp.SetConcurrency(3)
			//	//TODO Read the ticker from dimension statement
			//	joinOp.SetTicker(time.Second * 5)
			//	tp.AddOperator(inputs, joinOp)
			//	inputs = []xstream.Emitter{joinOp}
			//}


			if selectStmt.Condition != nil {
				filterOp := xstream.Transform(&plans.FilterPlan{Condition: selectStmt.Condition}, "filter")
				//TODO concurrency setting by command
				// filterOp.SetConcurrency(3)
				tp.AddOperator(inputs, filterOp)
				inputs = []xstream.Emitter{filterOp}
			}

			if selectStmt.Fields != nil {
				projectOp := xstream.Transform(&plans.ProjectPlan{Fields: selectStmt.Fields}, "project")
				tp.AddOperator(inputs, projectOp)
				inputs = []xstream.Emitter{projectOp}
			}


			//TODO hard coded sink now. parameterize it
			tp.AddSink(inputs, collectors.Func(func(data interface{}) error {
				fmt.Printf("Sink: %s\n", data)
				return nil
			}))

			if err := <-tp.Open(); err != nil {
				return err
			}
		}
	}
	return nil
}


package processor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"github.com/lf-edge/ekuiper/pkg/kv"
	"strings"
)

var (
	log = conf.Log
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
	case *ast.StreamStmt: //Table is also StreamStmt
		var r string
		err = p.execSave(s, statement, false)
		stt := ast.StreamTypeMap[s.StreamType]
		if err != nil {
			err = fmt.Errorf("Create %s fails: %v.", stt, err)
		} else {
			r = fmt.Sprintf("%s %s is created.", strings.Title(stt), s.Name)
			log.Printf("%s", r)
		}
		result = append(result, r)
	case *ast.ShowStreamsStatement:
		result, err = p.execShow(ast.TypeStream)
	case *ast.ShowTablesStatement:
		result, err = p.execShow(ast.TypeTable)
	case *ast.DescribeStreamStatement:
		var r string
		r, err = p.execDescribe(s, ast.TypeStream)
		result = append(result, r)
	case *ast.DescribeTableStatement:
		var r string
		r, err = p.execDescribe(s, ast.TypeTable)
		result = append(result, r)
	case *ast.ExplainStreamStatement:
		var r string
		r, err = p.execExplain(s, ast.TypeStream)
		result = append(result, r)
	case *ast.ExplainTableStatement:
		var r string
		r, err = p.execExplain(s, ast.TypeTable)
		result = append(result, r)
	case *ast.DropStreamStatement:
		var r string
		r, err = p.execDrop(s, ast.TypeStream)
		result = append(result, r)
	case *ast.DropTableStatement:
		var r string
		r, err = p.execDrop(s, ast.TypeTable)
		result = append(result, r)
	default:
		return nil, fmt.Errorf("Invalid stream statement: %s", statement)
	}

	return
}

func (p *StreamProcessor) execSave(stmt *ast.StreamStmt, statement string, replace bool) error {
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

func (p *StreamProcessor) ExecReplaceStream(statement string, st ast.StreamType) (string, error) {
	parser := xsql.NewParser(strings.NewReader(statement))
	stmt, err := xsql.Language.Parse(parser)
	if err != nil {
		return "", err
	}
	stt := ast.StreamTypeMap[st]
	switch s := stmt.(type) {
	case *ast.StreamStmt:
		if s.StreamType != st {
			return "", errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("%s %s is not found", ast.StreamTypeMap[st], s.Name))
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

func (p *StreamProcessor) execShow(st ast.StreamType) ([]string, error) {
	keys, err := p.ShowStream(st)
	if len(keys) == 0 {
		keys = append(keys, fmt.Sprintf("No %s definitions are found.", ast.StreamTypeMap[st]))
	}
	return keys, err
}

func (p *StreamProcessor) ShowStream(st ast.StreamType) ([]string, error) {
	stt := ast.StreamTypeMap[st]
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

func (p *StreamProcessor) getStream(name string, st ast.StreamType) (string, error) {
	vs, err := xsql.GetDataSourceStatement(p.db, name)
	if vs != nil && vs.StreamType == st {
		return vs.Statement, nil
	}
	if err != nil {
		return "", err
	}
	return "", errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("%s %s is not found", ast.StreamTypeMap[st], name))
}

func (p *StreamProcessor) execDescribe(stmt ast.NameNode, st ast.StreamType) (string, error) {
	streamStmt, err := p.DescStream(stmt.GetName(), st)
	if err != nil {
		return "", err
	}
	switch s := streamStmt.(type) {
	case *ast.StreamStmt:
		var buff bytes.Buffer
		buff.WriteString("Fields\n--------------------------------------------------------------------------------\n")
		for _, f := range s.StreamFields {
			buff.WriteString(f.Name + "\t")
			buff.WriteString(printFieldType(f.FieldType))
			buff.WriteString("\n")
		}
		buff.WriteString("\n")
		printOptions(s.Options, &buff)
		return buff.String(), err
	default:
		return "%s", fmt.Errorf("Error resolving the %s %s, the data in db may be corrupted.", ast.StreamTypeMap[st], stmt.GetName())
	}

}

func printOptions(opts *ast.Options, buff *bytes.Buffer) {
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
	if opts.SHARED {
		buff.WriteString(fmt.Sprintf("SHARED: %v\n", opts.SHARED))
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

func (p *StreamProcessor) DescStream(name string, st ast.StreamType) (ast.Statement, error) {
	statement, err := p.getStream(name, st)
	if err != nil {
		return nil, fmt.Errorf("Describe %s fails, %s.", ast.StreamTypeMap[st], err)
	}
	parser := xsql.NewParser(strings.NewReader(statement))
	stream, err := xsql.Language.Parse(parser)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (p *StreamProcessor) execExplain(stmt ast.NameNode, st ast.StreamType) (string, error) {
	_, err := p.getStream(stmt.GetName(), st)
	if err != nil {
		return "", fmt.Errorf("Explain %s fails, %s.", ast.StreamTypeMap[st], err)
	}
	return "TO BE SUPPORTED", nil
}

func (p *StreamProcessor) execDrop(stmt ast.NameNode, st ast.StreamType) (string, error) {
	s, err := p.DropStream(stmt.GetName(), st)
	if err != nil {
		return s, fmt.Errorf("Drop %s fails: %s.", ast.StreamTypeMap[st], err)
	}
	return s, nil
}

func (p *StreamProcessor) DropStream(name string, st ast.StreamType) (string, error) {
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
		return fmt.Sprintf("%s %s is dropped.", strings.Title(ast.StreamTypeMap[st]), name), nil
	}
}

func printFieldType(ft ast.FieldType) (result string) {
	switch t := ft.(type) {
	case *ast.BasicType:
		result = t.Type.String()
	case *ast.ArrayType:
		result = "array("
		if t.FieldType != nil {
			result += printFieldType(t.FieldType)
		} else {
			result += t.Type.String()
		}
		result += ")"
	case *ast.RecType:
		result = "struct("
		isFirst := true
		for _, f := range t.StreamFields {
			if isFirst {
				isFirst = false
			} else {
				result += ", "
			}
			result = result + f.Name + " " + printFieldType(f.FieldType)
		}
		result += ")"
	}
	return
}

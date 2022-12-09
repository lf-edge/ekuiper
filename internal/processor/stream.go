// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package processor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/schema"
	"github.com/lf-edge/ekuiper/internal/topo/lookup"
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

func NewStreamProcessor() *StreamProcessor {
	err, db := store.GetKV("stream")
	if err != nil {
		panic(fmt.Sprintf("Can not initalize store for the stream processor at path 'stream': %v", err))
	}
	processor := &StreamProcessor{
		db: db,
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

func (p *StreamProcessor) RecoverLookupTable() error {
	keys, err := p.db.Keys()
	if err != nil {
		return fmt.Errorf("error loading data from db: %v.", err)
	}
	var (
		v  string
		vs = &xsql.StreamInfo{}
	)
	for _, k := range keys {
		if ok, _ := p.db.Get(k, &v); ok {
			if err := json.Unmarshal([]byte(v), vs); err == nil && vs.StreamType == ast.TypeTable {
				parser := xsql.NewParser(strings.NewReader(vs.Statement))
				stmt, e := xsql.Language.Parse(parser)
				if e != nil {
					log.Error(err)
				}
				switch s := stmt.(type) {
				case *ast.StreamStmt:
					log.Infof("Starting lookup table %s", s.Name)
					e = lookup.CreateInstance(string(s.Name), s.Options.TYPE, s.Options)
					if err != nil {
						log.Errorf("%s", err.Error())
						return err
					}
				default:
					log.Errorf("Invalid lookup table statement: %s", vs.Statement)
				}

			}
		}
	}
	return nil
}

func (p *StreamProcessor) execSave(stmt *ast.StreamStmt, statement string, replace bool) error {
	if stmt.StreamType == ast.TypeTable && stmt.Options.KIND == ast.StreamKindLookup {
		log.Infof("Creating lookup table %s", stmt.Name)
		err := lookup.CreateInstance(string(stmt.Name), stmt.Options.TYPE, stmt.Options)
		if err != nil {
			return err
		}
	}
	s, err := json.Marshal(xsql.StreamInfo{
		StreamType: stmt.StreamType,
		Statement:  statement,
		StreamKind: stmt.Options.KIND,
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

func (p *StreamProcessor) ExecReplaceStream(name string, statement string, st ast.StreamType) (string, error) {
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
		if string(s.Name) != name {
			return "", fmt.Errorf("Replace %s fails: the sql statement must update the %s source.", name, name)
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

func (p *StreamProcessor) ShowTable(kind string) ([]string, error) {
	if kind == "" {
		return p.ShowStream(ast.TypeTable)
	}
	keys, err := p.db.Keys()
	if err != nil {
		return nil, fmt.Errorf("Show tables fails, error when loading data from db: %v.", err)
	}
	var (
		v      string
		vs     = &xsql.StreamInfo{}
		result = make([]string, 0)
	)
	for _, k := range keys {
		if ok, _ := p.db.Get(k, &v); ok {
			if err := json.Unmarshal([]byte(v), vs); err == nil && vs.StreamType == ast.TypeTable {
				if kind == "scan" && (vs.StreamKind == ast.StreamKindScan || vs.StreamKind == "") {
					result = append(result, k)
				} else if kind == "lookup" && vs.StreamKind == ast.StreamKindLookup {
					result = append(result, k)
				}
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
	if opts.SCHEMAID != "" {
		buff.WriteString(fmt.Sprintf("SCHEMAID: %s\n", opts.SCHEMAID))
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

func (p *StreamProcessor) GetInferredSchema(name string, st ast.StreamType) (ast.StreamFields, error) {
	statement, err := p.getStream(name, st)
	if err != nil {
		return nil, fmt.Errorf("Describe %s fails, %s.", ast.StreamTypeMap[st], err)
	}
	parser := xsql.NewParser(strings.NewReader(statement))
	stream, err := xsql.Language.Parse(parser)
	if err != nil {
		return nil, err
	}
	stmt, ok := stream.(*ast.StreamStmt)
	if !ok {
		return nil, fmt.Errorf("Describe %s fails, cannot parse the data \"%s\" to a stream statement", ast.StreamTypeMap[st], statement)
	}
	if stmt.Options.SCHEMAID != "" {
		return schema.InferFromSchemaFile(stmt.Options.FORMAT, stmt.Options.SCHEMAID)
	}
	return nil, nil
}

// GetInferredJsonSchema return schema in json schema type
func (p *StreamProcessor) GetInferredJsonSchema(name string, st ast.StreamType) (map[string]*ast.JsonStreamField, error) {
	statement, err := p.getStream(name, st)
	if err != nil {
		return nil, fmt.Errorf("Describe %s fails, %s.", ast.StreamTypeMap[st], err)
	}
	parser := xsql.NewParser(strings.NewReader(statement))
	stream, err := xsql.Language.Parse(parser)
	if err != nil {
		return nil, err
	}
	stmt, ok := stream.(*ast.StreamStmt)
	if !ok {
		return nil, fmt.Errorf("Describe %s fails, cannot parse the data \"%s\" to a stream statement", ast.StreamTypeMap[st], statement)
	}
	sfs := stmt.StreamFields
	if stmt.Options.SCHEMAID != "" {
		sfs, err = schema.InferFromSchemaFile(stmt.Options.FORMAT, stmt.Options.SCHEMAID)
		if err != nil {
			return nil, err
		}
	}
	return sfs.ToJsonSchema(), nil
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
	if st == ast.TypeTable {
		err := lookup.DropInstance(name)
		if err != nil {
			return "", err
		}
	}
	_, err := p.getStream(name, st)
	if err != nil {
		return "", err
	}

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

// GetAll return all streams and tables defined to export.
func (p *StreamProcessor) GetAll() (result map[string]map[string]string, e error) {
	defs, err := p.db.All()
	if err != nil {
		e = err
		return
	}
	var (
		vs = &xsql.StreamInfo{}
	)
	result = map[string]map[string]string{
		"streams": make(map[string]string),
		"tables":  make(map[string]string),
	}
	for k, v := range defs {
		if err := json.Unmarshal([]byte(v), vs); err == nil {
			switch vs.StreamType {
			case ast.TypeStream:
				result["streams"][k] = vs.Statement
			case ast.TypeTable:
				result["tables"][k] = vs.Statement
			}
		}
	}
	return
}

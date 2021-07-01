package xsql

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/pkg/ast"
	"github.com/emqx/kuiper/pkg/errorx"
	"github.com/emqx/kuiper/pkg/kv"
	"strings"
)

func GetStreams(stmt *ast.SelectStatement) (result []string) {
	if stmt == nil {
		return nil
	}
	for _, source := range stmt.Sources {
		if s, ok := source.(*ast.Table); ok {
			result = append(result, s.Name)
		}
	}

	for _, join := range stmt.Joins {
		result = append(result, join.Name)
	}
	return
}

func GetStatementFromSql(sql string) (*ast.SelectStatement, error) {
	parser := NewParser(strings.NewReader(sql))
	if stmt, err := Language.Parse(parser); err != nil {
		return nil, fmt.Errorf("Parse SQL %s error: %s.", sql, err)
	} else {
		if r, ok := stmt.(*ast.SelectStatement); !ok {
			return nil, fmt.Errorf("SQL %s is not a select statement.", sql)
		} else {
			return r, nil
		}
	}
}

type StreamInfo struct {
	StreamType ast.StreamType `json:"streamType"`
	Statement  string         `json:"statement"`
}

func GetDataSourceStatement(m kv.KeyValue, name string) (*StreamInfo, error) {
	var (
		v  string
		vs = &StreamInfo{}
	)
	err := m.Open()
	if err != nil {
		return nil, fmt.Errorf("error when opening db: %v", err)
	}
	defer m.Close()
	if ok, _ := m.Get(name, &v); ok {
		if err := json.Unmarshal([]byte(v), vs); err != nil {
			return nil, fmt.Errorf("error unmarshall %s, the data in db may be corrupted", name)
		} else {
			return vs, nil
		}
	}
	return nil, errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("%s is not found", name))
}

func GetDataSource(m kv.KeyValue, name string) (stmt *ast.StreamStmt, err error) {
	info, err := GetDataSourceStatement(m, name)
	if err != nil {
		return nil, err
	}
	parser := NewParser(strings.NewReader(info.Statement))
	stream, err := Language.Parse(parser)
	stmt, ok := stream.(*ast.StreamStmt)
	if !ok {
		err = fmt.Errorf("Error resolving the stream %s, the data in db may be corrupted.", name)
	}
	return
}

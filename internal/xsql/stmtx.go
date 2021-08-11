// Copyright 2021 EMQ Technologies Co., Ltd.
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

package xsql

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"github.com/lf-edge/ekuiper/pkg/kv/stores"
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

func GetDataSourceStatement(m stores.KeyValue, name string) (*StreamInfo, error) {
	var (
		v  string
		vs = &StreamInfo{}
	)
	if ok, _ := m.Get(name, &v); ok {
		if err := json.Unmarshal([]byte(v), vs); err != nil {
			return nil, fmt.Errorf("error unmarshall %s, the data in db may be corrupted", name)
		} else {
			return vs, nil
		}
	}
	return nil, errorx.NewWithCode(errorx.NOT_FOUND, fmt.Sprintf("%s is not found", name))
}

func GetDataSource(m stores.KeyValue, name string) (stmt *ast.StreamStmt, err error) {
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

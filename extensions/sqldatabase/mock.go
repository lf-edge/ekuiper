// Copyright 2023 EMQ Technologies Co., Ltd.
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

package sqldatabase

import "database/sql"

type MockDB struct {
	sqls []string
}

func (m *MockDB) Exec(query string, _ ...interface{}) (sql.Result, error) {
	m.sqls = append(m.sqls, query)
	return &MockResult{rowsAffected: 1}, nil
}

func (m *MockDB) LastSql() string {
	if len(m.sqls) == 0 {
		return ""
	} else {
		return m.sqls[len(m.sqls)-1]
	}
}

type MockResult struct {
	rowsAffected int64
}

func (m *MockResult) LastInsertId() (int64, error) {
	return 1, nil
}

func (m *MockResult) RowsAffected() (int64, error) {
	return m.rowsAffected, nil
}

// Copyright 2022 EMQ Technologies Co., Ltd.
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

package sql

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/pkg/store/definition"
	"github.com/lf-edge/ekuiper/internal/pkg/store/sql/sqlite"
)

func BuildStores(c definition.Config, name string) (definition.StoreBuilder, definition.TsBuilder, error) {
	db, err := sqlite.NewSqliteDatabase(c, name)
	if err != nil {
		return nil, nil, err
	}
	err = db.Connect()
	if err != nil {
		return nil, nil, err
	}
	d, ok := db.(Database)
	if !ok {
		return nil, nil, fmt.Errorf("unrecognized database type")
	}
	kvBuilder := NewStoreBuilder(d)
	tsBuilder := NewTsBuilder(d)
	return kvBuilder, tsBuilder, nil
}

// Copyright 2021 INTECH Process Automation Ltd.
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
	"github.com/lf-edge/ekuiper/internal/pkg/db/sql"
	"github.com/lf-edge/ekuiper/pkg/kv/stores"
)

type StoreBuilder struct {
	database sql.Database
}

func NewStoreBuilder(d sql.Database) StoreBuilder {
	return StoreBuilder{
		database: d,
	}
}

func (b StoreBuilder) CreateStore(table string) (error, stores.KeyValue) {
	return createSqlKvStore(b.database, table)
}

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

//go:build fdb || full

package fdb

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/lf-edge/ekuiper/pkg/kv"
)

const TSNamespace = "TS"

type TsBuilder struct {
	database  *fdb.Database
	namespace string
}

func NewTsBuilder(d *fdb.Database) TsBuilder {
	return TsBuilder{
		database:  d,
		namespace: TSNamespace,
	}
}

func (b TsBuilder) CreateTs(table string) (kv.Tskv, error) {
	return CreateFdbTs(b.database, b.namespace, table)
}

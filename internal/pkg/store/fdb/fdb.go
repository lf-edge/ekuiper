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

	"github.com/lf-edge/ekuiper/internal/pkg/store/definition"
)

const defaultAPIVersion int = 710

func NewFdbFromConf(c definition.Config) (*fdb.Database, error) {
	conf := c.Fdb
	var err error
	if conf.APIVersion > 0 {
		err = fdb.APIVersion(conf.APIVersion)
	} else {
		err = fdb.APIVersion(defaultAPIVersion)
	}
	if err != nil {
		return nil, err
	}
	var db fdb.Database
	if conf.Path == "" {
		db, err = fdb.OpenDefault()
	} else {
		db, err = fdb.OpenDatabase(conf.Path)
	}
	if err != nil {
		return nil, err
	}
	if conf.Timeout > 0 && conf.APIVersion >= 610 {
		err = db.Options().SetTransactionTimeout(conf.Timeout)
		if err != nil {
			return nil, err
		}
	}
	return &db, nil
}

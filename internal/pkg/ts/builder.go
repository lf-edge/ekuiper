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

package ts

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/pkg/db"
	"github.com/lf-edge/ekuiper/internal/pkg/db/redis"
	"github.com/lf-edge/ekuiper/internal/pkg/db/sql"
	rb "github.com/lf-edge/ekuiper/internal/pkg/ts/redis"
	sb "github.com/lf-edge/ekuiper/internal/pkg/ts/sql"
	"github.com/lf-edge/ekuiper/pkg/kv"
)

type Builder interface {
	CreateTs(table string) (error, kv.Tskv)
}

func CreateTsBuilder(database db.Database) (error, Builder) {
	switch database.(type) {
	case *redis.Instance:
		d := *database.(*redis.Instance)
		return nil, rb.NewTsBuilder(d)
	case sql.Database:
		d := database.(sql.Database)
		return nil, sb.NewTsBuilder(d)
	default:
		return fmt.Errorf("unrecognized database type"), nil
	}
}

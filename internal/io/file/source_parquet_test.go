// Copyright 2024 EMQ Technologies Co., Ltd.
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

//go:build parquet || full

package file

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	_ "github.com/lf-edge/ekuiper/v2/internal/io/file/reader"
	"github.com/lf-edge/ekuiper/v2/pkg/mock"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestParquet(t *testing.T) {
	path, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(path, "test")
	meta := map[string]interface{}{
		"file": filepath.Join(path, "parquet", "simple.parq"),
	}
	mc := timex.Clock
	exp := []api.MessageTuple{
		model.NewDefaultSourceTuple(map[string]interface{}{"id": int64(1), "name": "user1"}, meta, mc.Now()),
		model.NewDefaultSourceTuple(map[string]interface{}{"id": int64(2), "name": "user2"}, meta, mc.Now()),
		model.NewDefaultSourceTuple(map[string]interface{}{"id": int64(7), "name": "user7"}, meta, mc.Now()),
		model.NewDefaultSourceTuple(map[string]interface{}{"id": int64(8), "name": "user8"}, meta, mc.Now()),
		model.NewDefaultSourceTuple(map[string]interface{}{"id": int64(10), "name": "user10"}, meta, mc.Now()),
		model.NewDefaultSourceTuple(map[string]interface{}{"id": int64(12), "name": "user12"}, meta, mc.Now()),
		model.NewDefaultSourceTuple(map[string]interface{}{"id": int64(15), "name": "user15"}, meta, mc.Now()),
		model.NewDefaultSourceTuple(map[string]interface{}{"id": int64(16), "name": "user16"}, meta, mc.Now()),
	}
	r := GetSource()
	mock.TestSourceConnector(t, r, map[string]any{
		"fileType":   "parquet",
		"path":       path,
		"datasource": "parquet/simple.parq",
	}, exp, func() {
		// do nothing
	})
}

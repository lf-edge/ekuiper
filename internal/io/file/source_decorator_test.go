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

package file

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/mock"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type MockDecoder struct {
	meta string
}

func (m *MockDecoder) Provision(_ api.StreamContext, props map[string]any) error {
	_, ok := props["wrong"]
	if ok {
		return fmt.Errorf("wrong")
	}
	return nil
}

func (m *MockDecoder) ReadMeta(_ api.StreamContext, line []byte) {
	if line != nil {
		m.meta = string(line)
		fmt.Println(m.meta)
	}
}

func (m *MockDecoder) Decorate(ctx api.StreamContext, data any) any {
	data.(map[string]any)["meta"] = m.meta
	return data
}

func TestCSVDec(t *testing.T) {
	path, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	path = filepath.Join(path, "test", "csv")

	modules.RegisterFileStreamReaderAlias("mockmock", "csv")
	modules.RegisterFileStreamDecorator("mockmock", func(ctx api.StreamContext) modules.FileStreamDecorator {
		return &MockDecoder{}
	})

	meta := map[string]any{
		"file": filepath.Join(path, "a.csv"),
	}
	mc := timex.Clock
	exp := []api.MessageTuple{
		model.NewDefaultSourceTuple(map[string]any{"@": "#", "id": "1", "ts": "1670170500", "value": "161.927872", "meta": "<special content>"}, meta, mc.Now()),
		model.NewDefaultSourceTuple(map[string]any{"@": "#", "id": "2", "ts": "1670170900", "value": "176", "meta": "<special content>"}, meta, mc.Now()),
	}
	r := GetSource()
	mock.TestSourceConnector(t, r, map[string]any{
		"path":             path,
		"fileType":         "mockmock",
		"datasource":       "a.csv",
		"hasHeader":        true,
		"delimiter":        "\t",
		"ignoreStartLines": 3,
		"ignoreEndLines":   1,
	}, exp, func() {
		// do nothing
	})
}

// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package meta

import (
	"fmt"
	"path"
	"testing"

	"github.com/lf-edge/ekuiper/internal/conf"
)

func TestGetMqttSourceMeta(t *testing.T) {
	confDir, err := conf.GetConfLoc()
	if nil != err {
		return
	}

	if err = ReadSourceMetaFile(path.Join(confDir, "mqtt_source.json"), true, false); nil != err {
		t.Error(err)
		return
	}

	showMeta, err := GetSourceMeta("mqtt", "zh_CN")
	if nil != err {
		t.Error(err)
	}

	if showMeta.DataSource == nil {
		t.Errorf("mqtt source meta data source is null")
	}

	fields := showMeta.ConfKeys["default"]

	if len(fields) == 0 {
		t.Errorf("default fields %v", fields)
	}

}

func TestGetSqlSourceMeta(t *testing.T) {
	confDir, err := conf.GetConfLoc()
	if nil != err {
		return
	}

	if err = ReadSourceMetaFile(path.Join(confDir, "sources", "httppull.json"), true, false); nil != err {
		t.Error(err)
		return
	}

	showMeta, err := GetSourceMeta("httppull", "zh_CN")
	if nil != err {
		t.Error(err)
	}

	fields := showMeta.ConfKeys["default"]

	for _, value := range fields {
		if value.Default == nil {
			t.Errorf("value  %v default field is null", value)
		}
	}
}

func TestGetSqlSinkMeta(t *testing.T) {
	confDir, err := conf.GetConfLoc()
	if nil != err {
		return
	}

	if err = ReadSinkMetaFile(path.Join(confDir, "sinks", "mqtt.json"), true); nil != err {
		t.Error(err)
		return
	}

	showMeta, err := GetSinkMeta("mqtt", "zh_CN")
	if nil != err {
		t.Error(err)
		return
	}

	fields := showMeta.Fields

	for _, value := range fields {
		fmt.Printf("value %v", value)
		if value.Type == "" {
			t.Errorf("value %v type field shoud not be empty", value)
		}
	}
}

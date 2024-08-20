// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
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

	assert.Equal(t, "internal", showMeta.Type)

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

func TestSourceMeta(t *testing.T) {
	_, err := GetSourceMeta("123", "123")
	require.Error(t, err)
	ewc, ok := err.(errorx.ErrorWithCode)
	require.True(t, ok)
	require.Equal(t, errorx.ConfKeyError, ewc.Code())
}

func TestGetSource(t *testing.T) {
	commonAbout := &about{Installed: true}
	gSourcemetadata = map[string]*uiSource{
		"mqtt.json": {
			About: commonAbout,
		},
		"random.json": {
			About: commonAbout,
		},
		"pyjson.json": {
			About: commonAbout,
		},
	}
	expected := []*pluginfo{
		{
			Name:  "mqtt",
			About: commonAbout,
			Type:  "internal",
		},
		{
			Name:  "pyjson",
			About: commonAbout,
			Type:  "none",
		},
		{
			Name:  "random",
			About: commonAbout,
			Type:  "none",
		},
	}
	sources := GetSourcesPlugins("stream")
	sort.SliceStable(sources, func(i, j int) bool {
		return sources[i].Name < sources[j].Name
	})
	require.Equal(t, expected, sources)
}

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

package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type MetaConfiguration struct {
	SourceConfig     map[string]map[string]any `json:"sourceConfig" yaml:"sourceConfig"`
	SinkConfig       map[string]map[string]any `json:"sinkConfig" yaml:"sinkConfig"`
	ConnectionConfig map[string]map[string]any `json:"connectionConfig" yaml:"connectionConfig"`

	Streams map[string]*DatasourceExport `json:"streams" yaml:"streams"`
	Tables  map[string]*DatasourceExport `json:"tables" yaml:"tables"`
	Rules   map[string]*def.Rule         `json:"rules" yaml:"rules"`
}

type DatasourceExport struct {
	SQL string `json:"sql" yaml:"sql"`
}

func GenMetaConfiguration() (*MetaConfiguration, error) {
	m := &MetaConfiguration{
		Streams: make(map[string]*DatasourceExport),
		Tables:  make(map[string]*DatasourceExport),
		Rules:   map[string]*def.Rule{},
	}
	var err error
	m.SourceConfig, err = conf.GetCfgFromKVStorage("sources", "", "")
	if err != nil {
		return nil, err
	}
	m.SinkConfig, err = conf.GetCfgFromKVStorage("sinks", "", "")
	if err != nil {
		return nil, err
	}
	m.ConnectionConfig, err = conf.GetCfgFromKVStorage("connections", "", "")
	if err != nil {
		return nil, err
	}

	rset := rulesetProcessor.ExportRuleSet()
	for key, sql := range rset.Streams {
		m.Streams[key] = &DatasourceExport{
			SQL: sql,
		}
	}
	for key, sql := range rset.Tables {
		m.Tables[key] = &DatasourceExport{
			SQL: sql,
		}
	}
	for key, v := range rset.Rules {
		jm := make(map[string]any)
		if err := json.Unmarshal([]byte(v), &jm); err != nil {
			return nil, err
		}
		d := &def.Rule{}
		if err := cast.MapToStruct(jm, d); err != nil {
			return nil, err
		}
		m.Rules[key] = d
	}
	return m, nil
}

func yamlConfigurationExportHandler(w http.ResponseWriter, r *http.Request) {
	var yamlBytes []byte
	var err error
	var m *MetaConfiguration
	const name = "ekuiper_export.json"
	switch r.Method {
	case http.MethodGet:
		m, err = GenMetaConfiguration()
		if err != nil {
			handleError(w, err, "", logger)
			return
		}
		yamlBytes, err = yaml.Marshal(m)
		if err != nil {
			handleError(w, err, "", logger)
			return
		}
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Disposition", "Attachment")
	http.ServeContent(w, r, name, time.Now(), bytes.NewReader(yamlBytes))
}

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

	"github.com/pingcap/failpoint"
	"gopkg.in/yaml.v3"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/plugin"
	"github.com/lf-edge/ekuiper/v2/internal/schema"
	"github.com/lf-edge/ekuiper/v2/internal/service"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type MetaConfiguration struct {
	SourceConfig     map[string]map[string]any `json:"sourceConfig,omitempty" yaml:"sourceConfig,omitempty"`
	SinkConfig       map[string]map[string]any `json:"sinkConfig,omitempty" yaml:"sinkConfig,omitempty"`
	ConnectionConfig map[string]map[string]any `json:"connectionConfig,omitempty" yaml:"connectionConfig,omitempty"`
	// plugins
	PortablePlugins map[string]*plugin.IOPlugin `json:"portablePlugins,omitempty" yaml:"portablePlugins,omitempty"`
	// others
	Service map[string]*service.ServiceCreationRequest `json:"service,omitempty" yaml:"service,omitempty"`
	Schema  map[string]*schema.Info                    `json:"schema,omitempty" yaml:"schema,omitempty"`
	Uploads map[string]*fileContent                    `json:"uploads,omitempty" yaml:"uploads,omitempty"`
	// rules related
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
	if err = addConfiguration(m); err != nil {
		return nil, err
	}
	if err = addPlugins(m); err != nil {
		return nil, err
	}
	if err = addService(m); err != nil {
		return nil, err
	}
	if err = addSchema(m); err != nil {
		return nil, err
	}
	if err = addUploads(m); err != nil {
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

func addConfiguration(m *MetaConfiguration) error {
	var err error
	m.SourceConfig, err = conf.GetCfgFromKVStorage("sources", "", "")
	if err != nil {
		return err
	}
	m.SinkConfig, err = conf.GetCfgFromKVStorage("sinks", "", "")
	if err != nil {
		return err
	}
	m.ConnectionConfig, err = conf.GetCfgFromKVStorage("connections", "", "")
	if err != nil {
		return err
	}
	return nil
}

func addPlugins(m *MetaConfiguration) error {
	if managers["portable"] != nil {
		want := make(map[string]*plugin.IOPlugin)
		pm := managers["portable"].Export()
		failpoint.Inject("mockYamlExport", func() {
			e := map[string]*plugin.IOPlugin{
				"p1": {
					Name: "p1",
					File: "path",
				},
			}
			b, _ := json.Marshal(e)
			pm = map[string]string{
				"p1": string(b),
			}
		})
		for k, v := range pm {
			p := &plugin.IOPlugin{}
			if err := json.Unmarshal([]byte(v), p); err != nil {
				return err
			}
			want[k] = p
		}
		m.PortablePlugins = want
	}
	return nil
}

func addSchema(m *MetaConfiguration) error {
	if managers["schema"] != nil {
		want := make(map[string]*schema.Info)
		pm := managers["schema"].Export()
		failpoint.Inject("mockYamlExport", func() {
			e := map[string]*schema.Info{
				"p1": {
					Name:     "p1",
					FilePath: "path",
				},
			}
			b, _ := json.Marshal(e)
			pm = map[string]string{
				"p1": string(b),
			}
		})
		for k, v := range pm {
			s := &schema.Info{}
			if err := json.Unmarshal([]byte(v), s); err != nil {
				return err
			}
			want[k] = s
		}
		m.Schema = want
	}
	return nil
}

func addUploads(m *MetaConfiguration) error {
	ue := uploadsExport()
	failpoint.Inject("mockYamlExport", func() {
		e := map[string]*fileContent{
			"p1": {
				Name:     "p1",
				FilePath: "path",
			},
		}
		b, _ := json.Marshal(e)
		ue = map[string]string{
			"p1": string(b),
		}
	})
	if len(ue) > 0 {
		want := make(map[string]*fileContent)
		for k, v := range ue {
			f := &fileContent{}
			if err := json.Unmarshal([]byte(v), f); err != nil {
				return err
			}
			want[k] = f
		}
		m.Uploads = want
	}
	return nil
}

func addService(m *MetaConfiguration) error {
	if managers["service"] != nil {
		want := make(map[string]*service.ServiceCreationRequest)
		pm := managers["service"].Export()
		failpoint.Inject("mockYamlExport", func() {
			e := map[string]*service.ServiceCreationRequest{
				"p1": {
					Name: "p1",
					File: "path",
				},
			}
			b, _ := json.Marshal(e)
			pm = map[string]string{
				"p1": string(b),
			}
		})
		for k, v := range pm {
			s := &service.ServiceCreationRequest{}
			if err := json.Unmarshal([]byte(v), s); err != nil {
				return err
			}
			want[k] = s
		}
		m.Service = want
	}
	return nil
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

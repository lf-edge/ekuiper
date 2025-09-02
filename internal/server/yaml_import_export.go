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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/failpoint"
	"gopkg.in/yaml.v3"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/v2/internal/plugin"
	"github.com/lf-edge/ekuiper/v2/internal/schema"
	"github.com/lf-edge/ekuiper/v2/internal/service"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/replace"
)

type MetaConfiguration struct {
	SourceConfig     map[string]map[string]any `json:"sourceConfig,omitempty" yaml:"sourceConfig,omitempty"`
	SinkConfig       map[string]map[string]any `json:"sinkConfig,omitempty" yaml:"sinkConfig,omitempty"`
	ConnectionConfig map[string]map[string]any `json:"connectionConfig,omitempty" yaml:"connectionConfig,omitempty"`
	// plugins
	NativePlugins   map[string]*plugin.IOPlugin `json:"nativePlugins,omitempty" yaml:"nativePlugins,omitempty"`
	PortablePlugins map[string]*plugin.IOPlugin `json:"portablePlugins,omitempty" yaml:"portablePlugins,omitempty"`
	// others
	Service map[string]*service.ServiceCreationRequest `json:"service,omitempty" yaml:"service,omitempty"`
	Schema  map[string]*schema.Info                    `json:"schema,omitempty" yaml:"schema,omitempty"`
	Uploads map[string]*fileContent                    `json:"uploads,omitempty" yaml:"uploads,omitempty"`
	// rules related
	Streams map[string]*DatasourceExport `json:"streams" yaml:"streams"`
	Tables  map[string]*DatasourceExport `json:"tables,omitempty" yaml:"tables,omitempty"`
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
	if managers["plugin"] != nil {
		want := make(map[string]*plugin.IOPlugin)
		pm := managers["plugin"].Export()
		failpoint.Inject("mockYamlExport", func() {
			e := map[string]*plugin.IOPlugin{
				"p1": {
					Name: "p2",
					File: "path",
				},
			}
			b, _ := json.Marshal(e)
			pm = map[string]string{
				"p2": string(b),
			}
		})
		for k, v := range pm {
			p := &plugin.IOPlugin{}
			if err := json.Unmarshal([]byte(v), p); err != nil {
				return err
			}
			want[k] = p
		}
		m.NativePlugins = want
	}
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
	const name = "ekuiper_export.yaml"
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

type importConfiguration struct {
	configurationInfo

	// TODO: support these later
	Partial bool `json:"partial" yaml:"partial"`
	Reboot  bool `json:"reboot" yaml:"reboot"`
}

func yamlConfImportHandler(w http.ResponseWriter, r *http.Request) {
	c := &importConfiguration{}
	err := json.NewDecoder(r.Body).Decode(c)
	if err != nil {
		handleError(w, err, "Invalid body: Error decoding json", logger)
		return
	}
	if c.Content != "" && c.FilePath != "" {
		handleError(w, errors.New("bad request"), "Invalid body: Cannot specify both content and file", logger)
		return
	} else if c.Content == "" && c.FilePath == "" {
		handleError(w, errors.New("bad request"), "Invalid body: must specify content or file", logger)
		return
	}
	content := []byte(c.Content)
	if c.FilePath != "" {
		reader, err := httpx.ReadFile(c.FilePath)
		if err != nil {
			handleError(w, err, "Fail to read file", logger)
			return
		}
		defer reader.Close()
		buf := new(bytes.Buffer)
		_, err = io.Copy(buf, reader)
		if err != nil {
			handleError(w, err, "fail to convert file", logger)
			return
		}
		content = buf.Bytes()
	}
	err = importFromByte(content)
	if err != nil {
		handleError(w, err, "import failed", logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("success"))
}

func importFromByte(content []byte) error {
	m := &MetaConfiguration{}
	err := yaml.Unmarshal(content, m) //nolint:staticcheck
	err = mockImportErr(err, mockImportFromByteErr)
	if err != nil {
		return err
	}
	return importYamlConf(m)
}

const (
	mockErrStart int = iota
	mockImportFromByteErr
	mockSourcesErr
	mockSinksErr
	mockConnectionsErr
	mockStreamsErr
	mockTablesErr
	mockRulesErr
	mockUploadErr
	mockServiceErr
	mockSchemaErr
	mockPortablePluginErr
	mockNativePluginErr
	mockErrEnd
)

func mockImportErr(err error, errSwitch int) error {
	failpoint.Inject("mockImportErr", func(val failpoint.Value) {
		if errSwitch == val.(int) {
			err = errors.New("mockImportErr")
		}
	})
	return err
}

func importYamlConf(m *MetaConfiguration) error {
	if err := importConfigurations(m); err != nil {
		return err
	}
	var err error
	err = importUploads(m) //nolint:staticcheck
	err = mockImportErr(err, mockUploadErr)
	if err != nil {
		return err
	}
	err = importService(m) //nolint:staticcheck
	err = mockImportErr(err, mockServiceErr)
	if err != nil {
		return err
	}
	err = importSchema(m) //nolint:staticcheck
	err = mockImportErr(err, mockSchemaErr)
	if err != nil {
		return err
	}
	err = importNativePlugins(m) //nolint:staticcheck
	err = mockImportErr(err, mockNativePluginErr)
	if err != nil {
		return err
	}
	err = importPortablePlugins(m) //nolint:staticcheck
	err = mockImportErr(err, mockPortablePluginErr)
	if err != nil {
		return err
	}
	if err := importDataSource(m); err != nil {
		return err
	}
	if err := importRules(m); err != nil {
		return err
	}
	return nil
}

func importSchema(m *MetaConfiguration) error {
	sm, ok := managers["schema"]
	if !ok {
		return fmt.Errorf("schema manager not exist")
	}
	want := make(map[string]string)
	for k, v := range m.Schema {
		b, _ := json.Marshal(v)
		want[k] = string(b)
	}
	return importByManager(want, sm, "schema")
}

func importService(m *MetaConfiguration) error {
	sm, ok := managers["service"]
	if !ok {
		return fmt.Errorf("service manager not exist")
	}
	want := make(map[string]string)
	for k, v := range m.Service {
		b, _ := json.Marshal(v)
		want[k] = string(b)
	}
	return importByManager(want, sm, "service")
}

func importUploads(m *MetaConfiguration) error {
	want := make(map[string]string)
	for k, v := range m.Uploads {
		b, _ := json.Marshal(v)
		want[k] = string(b)
	}
	result := uploadsImport(want)
	if len(result) < 1 {
		return nil
	}
	errs := make([]error, 0)
	for k, v := range result {
		errs = append(errs, fmt.Errorf("import upload %s failed, err:%v", k, v))
	}
	return errors.Join(errs...)
}

func importByManager(want map[string]string, m ConfManager, typ string) error {
	result := m.Import(context.Background(), want)
	if len(result) < 1 {
		return nil
	}
	errs := make([]error, 0)
	for k, v := range result {
		errs = append(errs, fmt.Errorf("import %s %v failed, err:%v", typ, k, v))
	}
	return errors.Join(errs...)
}

func importPortablePlugins(m *MetaConfiguration) error {
	manager, ok := managers["portable"]
	if !ok {
		return fmt.Errorf("portable manager not exist")
	}
	importPlugin := make(map[string]string)
	for key, value := range m.PortablePlugins {
		b, _ := json.Marshal(value)
		importPlugin[key] = string(b)
	}
	return importByManager(importPlugin, manager, "portable plugin")
}

func importNativePlugins(m *MetaConfiguration) error {
	manager, ok := managers["plugin"]
	if !ok {
		return fmt.Errorf("native manager not exist")
	}
	importPlugin := make(map[string]string)
	for key, value := range m.PortablePlugins {
		b, _ := json.Marshal(value)
		importPlugin[key] = string(b)
	}
	return importByManager(importPlugin, manager, "native plugin")
}

func importRules(m *MetaConfiguration) error {
	for key, value := range m.Rules {
		_ = registry.DeleteRule(key)
		b, _ := json.Marshal(value)
		_, err := registry.CreateRule(key, string(b)) //nolint:staticcheck
		err = mockImportErr(err, mockRulesErr)
		if err != nil {
			return fmt.Errorf("replace rule %v failed, err:%v", key, err)
		}
	}
	return nil
}

func importConfigurations(m *MetaConfiguration) error {
	for key, value := range m.SourceConfig {
		err := writeConf(key, value) //nolint:staticcheck
		err = mockImportErr(err, mockSourcesErr)
		if err != nil {
			return err
		}
	}
	for key, value := range m.SinkConfig {
		err := writeConf(key, value) //nolint:staticcheck
		err = mockImportErr(err, mockSinksErr)
		if err != nil {
			return err
		}
	}
	for key, value := range m.ConnectionConfig {
		err := writeConf(key, value) //nolint:staticcheck
		err = mockImportErr(err, mockConnectionsErr)
		if err != nil {
			return err
		}
	}
	if err := connection.ReloadNamedConnection(); err != nil {
		conf.Log.Errorf("reload connection config error: %s", err.Error())
	}
	return nil
}

func importDataSource(m *MetaConfiguration) error {
	for name, value := range m.Streams {
		streamProcessor.DropStream(name, ast.TypeStream)
		_, err := streamProcessor.ExecReplaceStream(name, value.SQL, ast.TypeStream) //nolint:staticcheck
		err = mockImportErr(err, mockStreamsErr)
		if err != nil {
			return fmt.Errorf("replace stream %v failed, err:%v", name, err.Error())
		}
	}
	for name, value := range m.Tables {
		streamProcessor.DropStream(name, ast.TypeTable)
		_, err := streamProcessor.ExecReplaceStream(name, value.SQL, ast.TypeTable) //nolint:staticcheck
		err = mockImportErr(err, mockTablesErr)
		if err != nil {
			return fmt.Errorf("replace stream %v failed, err:%v", name, err.Error())
		}
	}
	return nil
}

func writeConf(key string, value map[string]any) error {
	value = replaceConfigurations(key, value)
	typ, plu, name, err := splitConfKey(key)
	if err != nil {
		return err
	}
	err = conf.WriteCfgIntoKVStorage(typ, plu, name, value)
	if err != nil {
		return fmt.Errorf("write conf %s failed, err:%v", key, err.Error())
	}
	return nil
}

func splitConfKey(key string) (string, string, string, error) {
	ss := strings.Split(key, ".")
	if len(ss) != 3 {
		return "", "", "", fmt.Errorf("%s isn't valid conf key", key)
	}
	return ss[0], ss[1], ss[2], nil
}

func replaceConfigurations(key string, props map[string]any) map[string]any {
	_, plgName, _, err := splitConfKey(key)
	if err != nil {
		return props
	}
	changed, newProps := replace.ReplacePropsWithPlug(plgName, props)
	if changed {
		return newProps
	}
	return props
}

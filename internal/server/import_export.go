// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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
	"os"
	"reflect"
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/meta"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/v2/internal/processor"
	"github.com/lf-edge/ekuiper/v2/metrics"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
)

type ConfManager interface {
	Import(context.Context, map[string]string) map[string]string
	PartialImport(context.Context, map[string]string) map[string]string
	Export() map[string]string
	Status() map[string]string
	Reset()
}

var managers = map[string]ConfManager{}

func InitConfManagers() {
	for k, v := range components {
		if cm, ok := v.(confExporter); ok {
			logger.Infof("register conf manager %s", k)
			managers[k] = cm.exporter()
		}
	}
}

const ProcessErr = "process error"

type Configuration struct {
	Streams          map[string]string `json:"streams"`
	Tables           map[string]string `json:"tables"`
	Rules            map[string]string `json:"rules"`
	NativePlugins    map[string]string `json:"nativePlugins"`
	PortablePlugins  map[string]string `json:"portablePlugins"`
	SourceConfig     map[string]string `json:"sourceConfig"`
	SinkConfig       map[string]string `json:"sinkConfig"`
	ConnectionConfig map[string]string `json:"connectionConfig"`
	Service          map[string]string `json:"Service"`
	Schema           map[string]string `json:"Schema"`
	Uploads          map[string]string `json:"uploads"`
	Scripts          map[string]string `json:"scripts"`
}

func configurationExport() ([]byte, error) {
	conf := &Configuration{
		Streams:          make(map[string]string),
		Tables:           make(map[string]string),
		Rules:            make(map[string]string),
		NativePlugins:    make(map[string]string),
		PortablePlugins:  make(map[string]string),
		SourceConfig:     make(map[string]string),
		SinkConfig:       make(map[string]string),
		ConnectionConfig: make(map[string]string),
		Service:          make(map[string]string),
		Schema:           make(map[string]string),
		Uploads:          make(map[string]string),
		Scripts:          make(map[string]string),
	}
	ruleSet := rulesetProcessor.ExportRuleSet()
	if ruleSet != nil {
		conf.Streams = ruleSet.Streams
		conf.Tables = ruleSet.Tables
		conf.Rules = ruleSet.Rules
	}

	if managers["plugin"] != nil {
		conf.NativePlugins = managers["plugin"].Export()
	}
	if managers["portable"] != nil {
		conf.PortablePlugins = managers["portable"].Export()
	}
	if managers["service"] != nil {
		conf.Service = managers["service"].Export()
	}
	if managers["schema"] != nil {
		conf.Schema = managers["schema"].Export()
	}
	if managers["script"] != nil {
		conf.Scripts = managers["script"].Export()
	}
	conf.Uploads = uploadsExport()

	yamlCfg := meta.GetConfigurations()
	conf.SourceConfig = yamlCfg.Sources
	conf.SinkConfig = yamlCfg.Sinks
	conf.ConnectionConfig = yamlCfg.Connections

	return json.Marshal(conf)
}

func configurationExportHandler(w http.ResponseWriter, r *http.Request) {
	var jsonBytes []byte
	const name = "ekuiper_export.json"

	switch r.Method {
	case http.MethodGet:
		jsonBytes, _ = configurationExport()
	case http.MethodPost:
		var rules []string
		_ = json.NewDecoder(r.Body).Decode(&rules)
		jsonBytes, _ = ruleMigrationProcessor.ConfigurationPartialExport(rules)
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Disposition", "Attachment")
	http.ServeContent(w, r, name, time.Now(), bytes.NewReader(jsonBytes))
}

func configurationReset() {
	_ = resetAllRules()
	_ = resetAllStreams()
	for _, v := range managers {
		v.Reset()
	}
	meta.ResetConfigs()
	uploadsReset()
}

type ImportConfigurationStatus struct {
	ErrorMsg       string
	ConfigResponse Configuration
}

func configurationImport(ctx context.Context, data []byte, reboot bool) ImportConfigurationStatus {
	conf := &Configuration{
		Streams:          make(map[string]string),
		Tables:           make(map[string]string),
		Rules:            make(map[string]string),
		NativePlugins:    make(map[string]string),
		PortablePlugins:  make(map[string]string),
		SourceConfig:     make(map[string]string),
		SinkConfig:       make(map[string]string),
		ConnectionConfig: make(map[string]string),
		Service:          make(map[string]string),
		Schema:           make(map[string]string),
		Uploads:          make(map[string]string),
		Scripts:          make(map[string]string),
	}

	importStatus := ImportConfigurationStatus{}

	configResponse := Configuration{
		Streams:          make(map[string]string),
		Tables:           make(map[string]string),
		Rules:            make(map[string]string),
		NativePlugins:    make(map[string]string),
		PortablePlugins:  make(map[string]string),
		SourceConfig:     make(map[string]string),
		SinkConfig:       make(map[string]string),
		ConnectionConfig: make(map[string]string),
		Service:          make(map[string]string),
		Schema:           make(map[string]string),
		Uploads:          make(map[string]string),
		Scripts:          make(map[string]string),
	}

	ResponseNil := Configuration{
		Streams:          make(map[string]string),
		Tables:           make(map[string]string),
		Rules:            make(map[string]string),
		NativePlugins:    make(map[string]string),
		PortablePlugins:  make(map[string]string),
		SourceConfig:     make(map[string]string),
		SinkConfig:       make(map[string]string),
		ConnectionConfig: make(map[string]string),
		Service:          make(map[string]string),
		Schema:           make(map[string]string),
		Uploads:          make(map[string]string),
		Scripts:          make(map[string]string),
	}

	err := json.Unmarshal(data, conf)
	if err != nil {
		importStatus.ErrorMsg = fmt.Errorf("configuration unmarshal with error %v", err).Error()
		return importStatus
	}
	configResponse.Uploads = uploadsImport(conf.Uploads)

	if reboot {
		if managers["plugin"] != nil {
			errMap := managers["plugin"].Import(ctx, conf.NativePlugins)
			if len(errMap) > 0 {
				importStatus.ErrorMsg = fmt.Errorf("pluginImport NativePlugins import error %v", errMap).Error()
				return importStatus
			}
		}
		if managers["schema"] != nil {
			errMap := managers["schema"].Import(ctx, conf.Schema)
			if len(errMap) > 0 {
				importStatus.ErrorMsg = fmt.Errorf("schemaImport Schema import error %v", errMap).Error()
				return importStatus
			}
		}
	}
	if managers["portable"] != nil {
		configResponse.PortablePlugins = managers["portable"].Import(ctx, conf.PortablePlugins)
	}
	if managers["service"] != nil {
		configResponse.Service = managers["service"].Import(ctx, conf.Service)
	}
	if managers["script"] != nil {
		configResponse.Scripts = managers["script"].Import(ctx, conf.Scripts)
	}

	yamlCfgSet := meta.YamlConfigurationSet{
		Sources:     conf.SourceConfig,
		Sinks:       conf.SinkConfig,
		Connections: conf.ConnectionConfig,
	}

	confRsp := meta.LoadConfigurations(yamlCfgSet)
	configResponse.SourceConfig = confRsp.Sources
	configResponse.SinkConfig = confRsp.Sinks
	configResponse.ConnectionConfig = confRsp.Connections

	ruleSet := processor.Ruleset{
		Streams: conf.Streams,
		Tables:  conf.Tables,
		Rules:   conf.Rules,
	}

	result := rulesetProcessor.ImportRuleSet(ruleSet)
	configResponse.Streams = result.Streams
	configResponse.Tables = result.Tables
	configResponse.Rules = result.Rules

	if !reboot {
		infra.SafeRun(func() error {
			for name := range ruleSet.Rules {
				rul, ee := ruleProcessor.GetRuleById(name)
				if ee != nil {
					logger.Error(ee)
					continue
				}
				reply := registry.RecoverRule(rul)
				if reply != "" {
					logger.Error(reply)
				}
			}
			return nil
		})
	}

	if reflect.DeepEqual(ResponseNil, configResponse) {
		importStatus.ConfigResponse = ResponseNil
	} else {
		importStatus.ErrorMsg = ProcessErr
		importStatus.ConfigResponse = configResponse
	}

	return importStatus
}

func configurationPartialImport(ctx context.Context, data []byte) ImportConfigurationStatus {
	conf := &Configuration{
		Streams:          make(map[string]string),
		Tables:           make(map[string]string),
		Rules:            make(map[string]string),
		NativePlugins:    make(map[string]string),
		PortablePlugins:  make(map[string]string),
		SourceConfig:     make(map[string]string),
		SinkConfig:       make(map[string]string),
		ConnectionConfig: make(map[string]string),
		Service:          make(map[string]string),
		Schema:           make(map[string]string),
		Uploads:          make(map[string]string),
		Scripts:          make(map[string]string),
	}

	importStatus := ImportConfigurationStatus{}

	configResponse := Configuration{
		Streams:          make(map[string]string),
		Tables:           make(map[string]string),
		Rules:            make(map[string]string),
		NativePlugins:    make(map[string]string),
		PortablePlugins:  make(map[string]string),
		SourceConfig:     make(map[string]string),
		SinkConfig:       make(map[string]string),
		ConnectionConfig: make(map[string]string),
		Service:          make(map[string]string),
		Schema:           make(map[string]string),
		Uploads:          make(map[string]string),
		Scripts:          make(map[string]string),
	}

	ResponseNil := Configuration{
		Streams:          make(map[string]string),
		Tables:           make(map[string]string),
		Rules:            make(map[string]string),
		NativePlugins:    make(map[string]string),
		PortablePlugins:  make(map[string]string),
		SourceConfig:     make(map[string]string),
		SinkConfig:       make(map[string]string),
		ConnectionConfig: make(map[string]string),
		Service:          make(map[string]string),
		Schema:           make(map[string]string),
		Uploads:          make(map[string]string),
		Scripts:          make(map[string]string),
	}

	err := json.Unmarshal(data, conf)
	if err != nil {
		importStatus.ErrorMsg = fmt.Errorf("configuration unmarshal with error %v", err).Error()
		return importStatus
	}

	yamlCfgSet := meta.YamlConfigurationSet{
		Sources:     conf.SourceConfig,
		Sinks:       conf.SinkConfig,
		Connections: conf.ConnectionConfig,
	}

	confRsp := meta.LoadConfigurationsPartial(yamlCfgSet)

	configResponse.Uploads = uploadsImport(conf.Uploads)
	if managers["plugin"] != nil {
		configResponse.NativePlugins = managers["plugin"].PartialImport(ctx, conf.NativePlugins)
	}
	if managers["schema"] != nil {
		configResponse.Schema = managers["schema"].PartialImport(ctx, conf.Schema)
	}
	if managers["portable"] != nil {
		configResponse.PortablePlugins = managers["portable"].PartialImport(ctx, conf.PortablePlugins)
	}
	if managers["service"] != nil {
		configResponse.Service = managers["service"].PartialImport(ctx, conf.Service)
	}
	if managers["script"] != nil {
		configResponse.Scripts = managers["script"].PartialImport(ctx, conf.Scripts)
	}

	configResponse.SourceConfig = confRsp.Sources
	configResponse.SinkConfig = confRsp.Sinks
	configResponse.ConnectionConfig = confRsp.Connections

	ruleSet := processor.Ruleset{
		Streams: conf.Streams,
		Tables:  conf.Tables,
		Rules:   conf.Rules,
	}

	result := importRuleSetPartial(ruleSet)
	configResponse.Streams = result.Streams
	configResponse.Tables = result.Tables
	configResponse.Rules = result.Rules

	if reflect.DeepEqual(ResponseNil, configResponse) {
		importStatus.ConfigResponse = ResponseNil
	} else {

		importStatus.ErrorMsg = ProcessErr
		importStatus.ConfigResponse = configResponse
	}

	return importStatus
}

type configurationInfo struct {
	Content  string `json:"content" yaml:"content"`
	FilePath string `json:"file" yaml:"filePath"`
}

func configurationImportHandler(w http.ResponseWriter, r *http.Request) {
	cb := r.URL.Query().Get("stop")
	stop := cb == "1"
	par := r.URL.Query().Get("partial")
	partial := par == "1"
	rsi := &configurationInfo{}
	err := json.NewDecoder(r.Body).Decode(rsi)
	if err != nil {
		handleError(w, err, "Invalid body: Error decoding json", logger)
		return
	}
	result, err := handleConfigurationImport(context.Background(), rsi, partial, stop)
	if err != nil {
		if result != nil && err.Error() == ProcessErr {
			errStr, _ := json.Marshal(result.ConfigResponse)
			err = errors.New(string(errStr))
		}
		handleError(w, err, "", logger)
		return
	}
	w.WriteHeader(http.StatusOK)
	jsonResponse(result, w, logger)
}

func handleConfigurationImport(ctx context.Context, rsi *configurationInfo, partial bool, stop bool) (*ImportConfigurationStatus, error) {
	if rsi.Content != "" && rsi.FilePath != "" {
		return nil, errors.New("Invalid body: Cannot specify both content and file")
	} else if rsi.Content == "" && rsi.FilePath == "" {
		return nil, errors.New("Invalid body: must specify content or file")
	}
	content := []byte(rsi.Content)
	if rsi.FilePath != "" {
		reader, err := httpx.ReadFile(rsi.FilePath)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		buf := new(bytes.Buffer)
		_, err = io.Copy(buf, reader)
		if err != nil {
			return nil, err
		}
		content = buf.Bytes()
	}
	if !partial {
		configurationReset()
		result := configurationImport(ctx, content, stop)
		if result.ErrorMsg != "" {
			return &result, errors.New(result.ErrorMsg)
		} else {
			if stop {
				go func() {
					time.Sleep(1 * time.Second)
					os.Exit(100)
				}()
			}
			return &result, nil
		}
	} else {
		result := configurationPartialImport(ctx, content)
		if result.ErrorMsg != "" {
			return &result, errors.New(result.ErrorMsg)
		} else {
			return &result, nil
		}
	}
}

func configurationStatusExport() Configuration {
	conf := Configuration{
		Streams:          make(map[string]string),
		Tables:           make(map[string]string),
		Rules:            make(map[string]string),
		NativePlugins:    make(map[string]string),
		PortablePlugins:  make(map[string]string),
		SourceConfig:     make(map[string]string),
		SinkConfig:       make(map[string]string),
		ConnectionConfig: make(map[string]string),
		Service:          make(map[string]string),
		Schema:           make(map[string]string),
		Uploads:          make(map[string]string),
		Scripts:          make(map[string]string),
	}
	ruleSet := rulesetProcessor.ExportRuleSetStatus()
	if ruleSet != nil {
		conf.Streams = ruleSet.Streams
		conf.Tables = ruleSet.Tables
		conf.Rules = ruleSet.Rules
	}

	if managers["plugin"] != nil {
		conf.NativePlugins = managers["plugin"].Export()
	}
	if managers["portable"] != nil {
		conf.PortablePlugins = managers["portable"].Export()
	}
	if managers["service"] != nil {
		conf.Service = managers["service"].Export()
	}
	if managers["schema"] != nil {
		conf.Schema = managers["schema"].Export()
	}
	if managers["script"] != nil {
		conf.Scripts = managers["script"].Export()
	}
	conf.Uploads = uploadsStatusExport()

	yamlCfgStatus := meta.GetConfigurationStatus()
	conf.SourceConfig = yamlCfgStatus.Sources
	conf.SinkConfig = yamlCfgStatus.Sinks
	conf.ConnectionConfig = yamlCfgStatus.Connections

	return conf
}

func configurationUpdateHandler(w http.ResponseWriter, r *http.Request) {
	basic := struct {
		LogLevel          *string `json:"logLevel"`
		Debug             *bool   `json:"debug"`
		ConsoleLog        *bool   `json:"consoleLog"`
		FileLog           *bool   `json:"fileLog"`
		TimeZone          *string `json:"timezone"`
		MetricsDumpConfig *struct {
			Enable *bool `json:"enable"`
		} `json:"metricsDumpConfig"`
	}{}
	if err := json.NewDecoder(r.Body).Decode(&basic); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		handleError(w, err, "Invalid JSON", logger)
		return
	}

	if basic.LogLevel != nil || basic.Debug != nil {
		if basic.LogLevel != nil {
			conf.Config.Basic.LogLevel = *basic.LogLevel
		}
		if basic.Debug != nil {
			conf.Config.Basic.Debug = *basic.Debug
		}
		conf.SetLogLevel(conf.Config.Basic.LogLevel, conf.Config.Basic.Debug)
	}

	if basic.TimeZone != nil {
		if err := cast.SetTimeZone(*basic.TimeZone); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			handleError(w, err, "Invalid TZ", logger)
			return
		}
		conf.Config.Basic.TimeZone = *basic.TimeZone
	}

	if basic.ConsoleLog != nil || basic.FileLog != nil {
		consoleLog := conf.Config.Basic.ConsoleLog
		if basic.ConsoleLog != nil {
			consoleLog = *basic.ConsoleLog
		}
		fileLog := conf.Config.Basic.FileLog
		if basic.FileLog != nil {
			fileLog = *basic.FileLog
		}
		if err := conf.SetConsoleAndFileLog(consoleLog, fileLog); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			handleError(w, err, "", logger)
			return
		}
		conf.Config.Basic.ConsoleLog = consoleLog
		conf.Config.Basic.FileLog = fileLog
	}

	if basic.MetricsDumpConfig != nil {
		if basic.MetricsDumpConfig.Enable != nil {
			if *basic.MetricsDumpConfig.Enable {
				if err := metrics.StartMetricsManager(); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					handleError(w, err, "", logger)
					return
				}
			} else {
				metrics.StopMetricsManager()
			}
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

func configurationStatusHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	content := configurationStatusExport()
	jsonResponse(content, w, logger)
}

func importRuleSetPartial(all processor.Ruleset) processor.Ruleset {
	ruleSetRsp := processor.Ruleset{
		Rules:   map[string]string{},
		Streams: map[string]string{},
		Tables:  map[string]string{},
	}
	// replace streams
	for k, v := range all.Streams {
		_, e := streamProcessor.ExecReplaceStream(k, v, ast.TypeStream)
		if e != nil {
			ruleSetRsp.Streams[k] = e.Error()
			continue
		}
	}
	// replace tables
	for k, v := range all.Tables {
		_, e := streamProcessor.ExecReplaceStream(k, v, ast.TypeTable)
		if e != nil {
			ruleSetRsp.Tables[k] = e.Error()
			continue
		}
	}

	for k, v := range all.Rules {
		err := registry.UpsertRule(k, v)
		if err != nil {
			ruleSetRsp.Rules[k] = err.Error()
			continue
		}
	}

	return ruleSetRsp
}

func uploadsReset() {
	_ = uploadsDb.Clean()
	_ = uploadsStatusDb.Clean()
}

func uploadsExport() map[string]string {
	conf, _ := uploadsDb.All()
	return conf
}

func uploadsStatusExport() map[string]string {
	status, _ := uploadsDb.All()
	return status
}

func uploadsImport(s map[string]string) map[string]string {
	errMap := map[string]string{}
	_ = uploadsStatusDb.Clean()
	for k, v := range s {
		fc := &fileContent{}
		err := json.Unmarshal([]byte(v), fc)
		if err != nil {
			errMsg := fmt.Sprintf("invalid body: Error decoding file json: %s", err.Error())
			errMap[k] = errMsg
			_ = uploadsStatusDb.Set(k, errMsg)
			continue
		}

		err = fc.Validate()
		if err != nil {
			errMap[k] = err.Error()
			_ = uploadsStatusDb.Set(k, err.Error())
			continue
		}

		err = upload(fc)
		if err != nil {
			errMap[k] = err.Error()
			continue
		}
	}
	return errMap
}

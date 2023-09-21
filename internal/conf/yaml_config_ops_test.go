// Copyright 2022 EMQ Technologies Co., Ltd.
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

package conf

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestConfigKeys_LoadSourceFile(t *testing.T) {
	_, err := NewConfigOperatorFromSourceStorage("mqtt")
	if err != nil {
		t.Error(err)
	}
}

func TestConfigKeys_LoadConnectionMqtt(t *testing.T) {
	_, err := NewConfigOperatorFromConnectionStorage("mqtt")
	if err != nil {
		t.Error(err)
	}
}

func TestConfigKeys_LoadConnectionEdgex(t *testing.T) {
	_, err := NewConfigOperatorFromConnectionStorage("edgex")
	if err != nil {
		t.Error(err)
	}
}

func TestConfigKeys_Ops(t *testing.T) {
	httpCfg, err := NewConfigOperatorFromSourceStorage("httppull")
	if err != nil {
		t.Error(err)
	}

	addData := `{"url":"127.0.0.1","method":"post","headers":{"Accept":"json"}}`
	delData := `{"method":"","headers":{"Accept":""}}`

	reqField := make(map[string]interface{})
	_ = json.Unmarshal([]byte(addData), &reqField)

	err = httpCfg.AddConfKey("new", reqField)
	if err != nil {
		t.Error(err)
	}

	if err := isAddData(addData, httpCfg.CopyConfContent()[`new`]); nil != err {
		t.Error(err)
	}

	delField := make(map[string]interface{})
	_ = json.Unmarshal([]byte(delData), &delField)

	err = httpCfg.DeleteConfKeyField("new", delField)
	if err != nil {
		t.Error(err)
	}

	if err := isDelData(delData, httpCfg.CopyConfContent()[`new`]); nil != err {
		t.Error(err)
	}
}

func TestConfigKeys_GetPluginName(t *testing.T) {
	pluginName := "mqtt"
	mqttCfg, err := NewConfigOperatorFromSourceStorage(pluginName)
	if err != nil {
		t.Error(err)
	}
	if mqttCfg.GetPluginName() != pluginName {
		t.Errorf("GetPluginName() gotName = %v, wantName = %v", mqttCfg.GetPluginName(), pluginName)
	}
}

func TestConfigKeys_GetConfContentByte(t *testing.T) {
	mqttCfg, err := NewConfigOperatorFromSourceStorage("mqtt")
	if err != nil {
		t.Error(err)
	}
	_, err = mqttCfg.GetConfContentByte()
	if err != nil {
		t.Error(err)
	}
}

func TestConfigKeys_LoadConfContent(t *testing.T) {
	mqttCfg := NewConfigOperatorForSource("mqtt")
	cf := make(map[string]map[string]interface{})
	source := `{"test": {"qos": 1, "server": "tcp://127.0.0.1:1883"}}`
	_ = json.Unmarshal([]byte(source), &cf)
	mqttCfg.LoadConfContent(cf)
	if !reflect.DeepEqual(cf, mqttCfg.CopyUpdatableConfContent()) {
		t.Errorf("LoadConfContent() fail")
	}
}

func TestConfigKeys_CopyReadOnlyConfContent(t *testing.T) {
	mqttCfg, err := NewConfigOperatorFromSourceStorage("mqtt")
	if err != nil {
		t.Error(err)
	}
	cf := make(map[string]map[string]interface{})
	source := `{"default": {"qos": 1, "server": "tcp://127.0.0.1:1883"}, "demo_conf": {"qos": 0, "server": "tcp://10.211.55.6:1883"}}`
	_ = yaml.Unmarshal([]byte(source), &cf)
	if !reflect.DeepEqual(cf, mqttCfg.CopyReadOnlyConfContent()) {
		t.Errorf("CopyReadOnlyConfContent() fail")
	}
}

func TestConfigKeys_GetConfKeys(t *testing.T) {
	mqttCfg, err := NewConfigOperatorFromSourceStorage("mqtt")
	if err != nil {
		t.Error(err)
	}
	keys := mqttCfg.GetConfKeys()
	// currently only etcCfg, no dataCfg
	source := []string{"default", "demo_conf"}
	if keys == nil {
		t.Errorf("Not Equal")
	}
	if len(keys) != len(source) {
		t.Errorf("Length not equal, got %v, want %v", len(keys), len(source))
	}
	sort.Strings(keys)
	sort.Strings(source)
	for i, key := range keys {
		if key != source[i] {
			t.Errorf("Not equal, got %v, want %v", key, source[i])
		}
	}
}

func TestConfigKeys_GetReadOnlyConfKeys(t *testing.T) {
	mqttCfg, err := NewConfigOperatorFromSourceStorage("mqtt")
	if err != nil {
		t.Error(err)
	}
	keys := mqttCfg.GetReadOnlyConfKeys()
	source := []string{"default", "demo_conf"}
	if keys == nil {
		t.Errorf("Not Equal")
	}
	if len(keys) != len(source) {
		t.Errorf("Length not equal, got %v, want %v", len(keys), len(source))
	}
	sort.Strings(keys)
	sort.Strings(source)
	for i, key := range keys {
		if key != source[i] {
			t.Errorf("Not equal, got %v, want %v", key, source[i])
		}
	}
}

func TestConfigKeys_GetUpdatableConfKeys(t *testing.T) {
	mqttCfg := NewConfigOperatorForSource("mqtt")
	cf := make(map[string]map[string]interface{})
	source := `{"test": {"qos": 1, "server": "tcp://127.0.0.1:18883"}}`
	_ = json.Unmarshal([]byte(source), &cf)
	mqttCfg.LoadConfContent(cf)
	keys := mqttCfg.GetUpdatableConfKeys()
	srcKeys := []string{"test"}
	if keys == nil {
		t.Errorf("Not Equal")
	}
	if len(keys) != len(srcKeys) {
		t.Errorf("Length not equal, got %v, want %v", len(keys), len(srcKeys))
	}
	sort.Strings(keys)
	sort.Strings(srcKeys)
	for i, key := range keys {
		if key != srcKeys[i] {
			t.Errorf("Not equal, got %v, want %v", key, source[i])
		}
	}
}

func TestConfigKeys_DeleteConfKey(t *testing.T) {
	mqttCfg := NewConfigOperatorForSource("mqtt")
	cf := make(map[string]map[string]interface{})
	source := `{"test": {"qos": 1, "server": "tcp://127.0.0.1:18883"}}`
	_ = json.Unmarshal([]byte(source), &cf)
	mqttCfg.LoadConfContent(cf)
	mqttCfg.DeleteConfKey("test")
	err := isDelData(`{"qos": 1, "server": "tcp://127.0.0.1:18883"}`, mqttCfg.CopyUpdatableConfContent()["test"])
	if err != nil {
		t.Error(err)
	}
}

func TestConfigKeys_ClearConfKeys(t *testing.T) {
	mqttCfg := NewConfigOperatorForSource("mqtt")
	cf := make(map[string]map[string]interface{})
	source := `{"test": {"qos": 1, "server": "tcp://127.0.0.1:18883"}}`
	_ = json.Unmarshal([]byte(source), &cf)
	mqttCfg.LoadConfContent(cf)
	mqttCfg.ClearConfKeys()
	if len(mqttCfg.CopyUpdatableConfContent()) > 0 {
		t.Errorf("ClearConfKeys() fail")
	}
}

func TestConfigKeys_AddConfKeyField(t *testing.T) {
	mqttCfg := NewConfigOperatorForSource("mqtt")
	cf := make(map[string]map[string]interface{})
	source := `{"test": {"qos": 1, "server": "tcp://127.0.0.1:1883"}}`
	_ = json.Unmarshal([]byte(source), &cf)
	mqttCfg.LoadConfContent(cf)
	ck := make(map[string]interface{})
	source = `{"username": "user"}`
	_ = json.Unmarshal([]byte(source), &ck)
	confKey := "test"
	mqttCfg.AddConfKeyField(confKey, ck)
	err := isAddData(source, mqttCfg.CopyUpdatableConfContent()[confKey])
	if err != nil {
		t.Error(err)
	}
}

func TestSourceConfigKeysOps_SaveCfgToStorage(t *testing.T) {
	mqttCfg, err := NewConfigOperatorFromSourceStorage("mqtt")
	if err != nil {
		t.Error(err)
	}
	err = os.MkdirAll("../../data/test/sources", os.ModePerm)
	if err != nil {
		t.Error(err)
	}
	_, err = os.Create("../../data/test/sources/mqtt.yaml")
	if err != nil {
		t.Error(err)
	}
	err = mqttCfg.SaveCfgToStorage()
	if err != nil {
		t.Error(err)
	}
	os.RemoveAll("../../data/test/sources/mqtt.yaml")
}

func TestSinkConfigKeysOps_SaveCfgToStorage(t *testing.T) {
	mqttCfg, err := NewConfigOperatorFromSinkStorage("mqtt")
	if err != nil {
		t.Error(err)
	}
	err = os.MkdirAll("../../data/test/sinks", os.ModePerm)
	if err != nil {
		t.Error(err)
	}
	_, err = os.Create("../../data/test/sinks/mqtt.yaml")
	if err != nil {
		t.Error(err)
	}
	err = mqttCfg.SaveCfgToStorage()
	if err != nil {
		t.Error(err)
	}
	os.RemoveAll("../../data/test/sinks/mqtt.yaml")
}

func TestNewConfigOperatorForSink(t *testing.T) {
	sink := NewConfigOperatorForSink("mqtt")
	if sink.GetPluginName() != "mqtt" {
		t.Errorf("NewConfigOperatorForSink() fail")
	}
}

func TestNewConfigOperatorForConnection(t *testing.T) {
	connection := NewConfigOperatorForConnection("mqtt")
	if connection.GetPluginName() != "mqtt" {
		t.Errorf("NewConfigOperatorForSink() fail")
	}
}

func TestConfigKeys_LoadFromKV(t *testing.T) {
	InitConf()
	Config.Basic.CfgStorageType = ""
	InitConf()
	// assert default
	require.Equal(t, Config.Basic.CfgStorageType, cfgFileStorage)
	defer func() {
		Config.Basic.CfgStorageType = cfgFileStorage
	}()
	Config.Basic.CfgStorageType = cfgStoreKVStorage
	mqttCfg, err := NewConfigOperatorFromSourceStorage("mqtt")
	require.NoError(t, err)
	require.NoError(t, mqttCfg.AddConfKey("key1", map[string]interface{}{
		"k1": "v1",
	}))
	require.NoError(t, mqttCfg.AddConfKey("key2", map[string]interface{}{
		"k2": "v2",
	}))
	require.NoError(t, mqttCfg.SaveCfgToStorage())
	mqttCfg2, err := NewConfigOperatorFromSourceStorage("mqtt")
	require.NoError(t, err)
	require.Equal(t, map[string]map[string]interface{}{
		"key1": {
			"k1": "v1",
		},
		"key2": {
			"k2": "v2",
		},
	}, mqttCfg2.CopyUpdatableConfContent())
	mqttCfg2.DeleteConfKey("key1")
	require.NoError(t, mqttCfg2.SaveCfgToStorage())
	mqttCfg3, err := NewConfigOperatorFromSourceStorage("mqtt")
	require.NoError(t, err)
	require.Equal(t, map[string]map[string]interface{}{
		"key2": {
			"k2": "v2",
		},
	}, mqttCfg3.CopyUpdatableConfContent())

	mSource, ok := mqttCfg3.(*SourceConfigKeysOps)
	require.True(t, ok)
	mSource.storageType = "mock"
	err = mSource.SaveCfgToStorage()
	require.Error(t, err, fmt.Errorf("unknown source cfg storage type: %v", "mock"))

	mqttCfg4, err := NewConfigOperatorFromSinkStorage("mqtt")
	require.NoError(t, err)
	mSinks, ok := mqttCfg4.(*SinkConfigKeysOps)
	require.True(t, ok)
	mSinks.storageType = "mock"
	err = mSinks.SaveCfgToStorage()
	require.Error(t, err, fmt.Errorf("unknown source cfg storage type: %v", "mock"))

	mqttCfg5, err := NewConfigOperatorFromConnectionStorage("mqtt")
	require.NoError(t, err)
	mConn, ok := mqttCfg5.(*ConnectionConfigKeysOps)
	require.True(t, ok)
	mConn.storageType = "mock"
	err = mConn.SaveCfgToStorage()
	require.Error(t, err, fmt.Errorf("unknown source cfg storage type: %v", "mock"))
}

func marshalUn(input, output interface{}) error {
	jsonString, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonString, output)
}

func isDelData(js string, cf map[string]interface{}) error {
	var delNode map[string]interface{}
	if err := json.Unmarshal([]byte(js), &delNode); nil != err {
		return err
	}
	for delk, delv := range delNode {
		if nil == delv {
			if _, ok := cf[delk]; ok {
				return fmt.Errorf("%s still exists", delk)
			}
		}

		switch t := delv.(type) {
		case string:
			if 0 == len(t) {
				if _, ok := cf[delk]; ok {
					return fmt.Errorf("%s still exists", delk)
				}
			}
		case map[string]interface{}:
			if b, err := json.Marshal(t); nil != err {
				return fmt.Errorf("request format error")
			} else {
				var auxCf map[string]interface{}
				if err := marshalUn(cf[delk], &auxCf); nil == err {
					if err := isDelData(string(b), auxCf); nil != err {
						return err
					}
				}
			}
		}

	}
	return nil
}

func isAddData(js string, cf map[string]interface{}) error {
	var addNode map[string]interface{}
	if err := json.Unmarshal([]byte(js), &addNode); nil != err {
		return err
	}
	for addk := range addNode {
		if _, ok := cf[addk]; !ok {
			return fmt.Errorf("not found key:%s", addk)
		}
	}
	return nil
}

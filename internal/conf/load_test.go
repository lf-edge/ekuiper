// Copyright 2025 EMQ Technologies Co., Ltd.
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

// INTECH Process Automation Ltd.
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
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestEnv(t *testing.T) {
	clearLoadConfigCache()
	key := "KUIPER__BASIC__CONSOLELOG"
	value := "true"

	err := os.Setenv(key, value)
	if err != nil {
		t.Error(err)
	}
	SetupEnv()
	c := model.KuiperConf{}
	err = LoadConfig(&c)
	if err != nil {
		t.Error(err)
	}

	if c.Basic.ConsoleLog != true {
		t.Errorf("env variable should set it to true")
	}
}

func TestJsonCamelCase(t *testing.T) {
	key := "HTTPPULL__DEFAULT__BODYTYPE"
	value := "event"

	err := os.Setenv(key, value)
	if err != nil {
		t.Error(err)
	}
	SetupEnv()
	const ConfigName = "sources/httppull.yaml"
	c := make(map[string]interface{})
	err = LoadConfigByName(ConfigName, &c)
	if err != nil {
		t.Error(err)
	}

	if casted, success := c["default"].(map[string]interface{}); success {
		if casted["bodyType"] != "event" {
			t.Errorf("env variable should set it to event")
		}
	} else {
		t.Errorf("returned value does not contains map under 'Basic' key")
	}
}

func TestNestedFields(t *testing.T) {
	key := "EDGEX__DEFAULT__OPTIONAL__PASSWORD"
	value := "password"

	err := os.Setenv(key, value)
	if err != nil {
		t.Error(err)
	}
	SetupEnv()
	const ConfigName = "sources/edgex.yaml"
	c := make(map[string]interface{})
	err = LoadConfigByName(ConfigName, &c)
	if err != nil {
		t.Error(err)
	}

	if casted, success := c["default"].(map[string]interface{}); success {
		if optional, ok := casted["optional"].(map[string]interface{}); ok {
			if optional["Password"] != "password" {
				t.Errorf("Password variable should set it to password")
			}
		} else {
			t.Errorf("returned value does not contains map under 'optional' key")
		}
	} else {
		t.Errorf("returned value does not contains map under 'Basic' key")
	}
}

func TestKeysReplacement(t *testing.T) {
	input := createRandomConfigMap()
	expected := createExpectedRandomConfigMap()
	list := []string{"interval", "Seed", "deduplicate", "pattern"}

	applyKeys(input, list)

	if !reflect.DeepEqual(input, expected) {
		t.Errorf("key names within list should be applied \nexpected - %s\n input   - %s", expected, input)
	}
}

func TestKeyReplacement(t *testing.T) {
	m := createRandomConfigMap()
	expected := createExpectedRandomConfigMap()

	applyKey(m, "Seed")
	applyKey(m, "interval")

	if !reflect.DeepEqual(m, expected) {
		t.Errorf("key names within list should be applied \nexpected - %s\nmap      - %s", expected, m)
	}
}

func createRandomConfigMap() map[string]interface{} {
	pattern := make(map[string]interface{})
	pattern["count"] = 50
	defaultM := make(map[string]interface{})
	defaultM["interval"] = 1000
	defaultM["seed"] = 1
	defaultM["pattern"] = pattern
	defaultM["deduplicate"] = 0
	ext := make(map[string]interface{})
	ext["interval"] = 100
	dedup := make(map[string]interface{})
	dedup["interval"] = 100
	dedup["deduplicated"] = 50
	input := make(map[string]interface{})
	input["default"] = defaultM
	input["ext"] = ext
	input["dedup"] = dedup
	return input
}

func createExpectedRandomConfigMap() map[string]interface{} {
	input := createRandomConfigMap()
	def := input["default"]
	if defMap, ok := def.(map[string]interface{}); ok {
		tmp := defMap["seed"]
		delete(defMap, "seed")
		defMap["Seed"] = tmp
	}
	return input
}

func TestPrintable(t *testing.T) {
	bef := map[string]interface{}{
		"password":     "password",
		"Password":     "password",
		"saslPassword": "password",
		"token":        "abc123",
		"username":     "user",
		"server":       "127.0.0.1",
		"optional": map[string]interface{}{
			"password": "password",
			"Password": "password",
			"token":    "nested_token",
		},
	}
	after := Printable(bef)

	assert.Equal(t, "*", after["password"])
	assert.Equal(t, "*", after["Password"])
	assert.Equal(t, "*", after["saslPassword"])
	assert.Equal(t, "*", after["token"])
	assert.Equal(t, "user", after["username"])
	assert.Equal(t, "127.0.0.1", after["server"])
	opt := after["optional"].(map[string]interface{})
	assert.Equal(t, "*", opt["password"])
	assert.Equal(t, "*", opt["Password"])
	assert.Equal(t, "*", opt["token"])
}

func TestIsSensitiveKey(t *testing.T) {
	tests := []struct {
		key  string
		want bool
	}{
		{"password", true},
		{"PASSWORD", true},
		{"saslPassword", true},
		{"token", true},
		{"access_token", true},
		{"refresh_token", true},
		{"secret", true},
		{"private_key", true},
		{"aes_key", true},
		{"credential", true},
		{"username", false},
		{"server", false},
		{"port", false},
		{"url", false},
		{"topic", false},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			got := isSensitiveKey(tt.key)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetValueTypeBySchema(t *testing.T) {
	tests := []struct {
		name       string
		val        string
		schemaType string
		want       interface{}
	}{
		{"numeric password stays string", "123456", "string", "123456"},
		{"bool-like password stays string", "true", "string", "true"},
		{"float-like username stays string", "1.5", "string", "1.5"},
		{"list-like password stays string", "[1,2]", "string", "[1,2]"},
		{"qos still int", "2", "int", int64(2)},
		{"invalid int keeps string", "abc", "int", "abc"},
		{"bool field", "true", "bool", true},
		{"boolean alias", "false", "boolean", false},
		{"float field", "1.25", "float", 1.25},
		{"list_string keeps numeric elements", "[1,2]", "list_string", []interface{}{"1", "2"}},
		{"infer int without schema", "123456", "", int64(123456)},
		{"infer bool without schema", "true", "", true},
		{"infer array without schema", "[1,2]", "", []interface{}{int64(1), int64(2)}},
		{"plain string without schema", "abc123", "", "abc123"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getValueType(tt.val, tt.schemaType)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMqttNumericPasswordFromEnv(t *testing.T) {
	clearLoadConfigCache()
	t.Cleanup(func() {
		clearLoadConfigCache()
		SetupEnv()
	})
	t.Setenv("MQTT_SOURCE__DEFAULT__PASSWORD", "123456")
	t.Setenv("MQTT_SOURCE__DEFAULT__USERNAME", "1001")
	t.Setenv("MQTT_SOURCE__DEFAULT__CLIENTID", "9001")
	t.Setenv("MQTT_SOURCE__DEFAULT__QOS", "2")
	t.Setenv("MQTT_SOURCE__DEFAULT__INSECURESKIPVERIFY", "true")
	SetupEnv()

	c := make(map[string]interface{})
	err := LoadConfigByName("mqtt_source.yaml", &c)
	require.NoError(t, err)

	def, ok := c["default"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "123456", def["password"])
	assert.Equal(t, "1001", def["username"])
	assert.Equal(t, "9001", def["clientid"])
	assert.Equal(t, int64(2), def["qos"])
	assert.Equal(t, true, def["insecureSkipVerify"])

	type mqttConn struct {
		Password string `json:"password"`
		Username string `json:"username"`
		ClientId string `json:"clientid"`
		Qos      int    `json:"qos"`
	}
	cfg := &mqttConn{}
	require.NoError(t, cast.MapToStruct(def, cfg))
	assert.Equal(t, "123456", cfg.Password)
	assert.Equal(t, "1001", cfg.Username)
	assert.Equal(t, "9001", cfg.ClientId)
	assert.Equal(t, 2, cfg.Qos)
}

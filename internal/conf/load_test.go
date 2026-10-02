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
	"path/filepath"
	"reflect"
	"strings"
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
		{"string keeps whitespace", " \t123456 \n", "string", " \t123456 \n"},
		{"text keeps whitespace", " 123456 ", "text", " 123456 "},
		{"empty string stays string", "", "string", ""},
		{"padded integer", " 2 ", "int", int64(2)},
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
		// Unsigned metadata types have no dedicated parser; preserve inference.
		{"uint falls back for negative integer", "-1", "uint", int64(-1)},
		{"uint8 falls back for large integer", "300", "uint8", int64(300)},
		{"uint falls back for boolean", "true", "uint", true},
		{"uint8 falls back for float", "1.5", "uint8", 1.5},
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
	for _, tt := range []struct {
		profile  string
		password string
	}{
		{"default", "123456"},
		{"custom", " 123456 "},
	} {
		t.Run(tt.profile, func(t *testing.T) {
			prefix := "MQTT_SOURCE__" + strings.ToUpper(tt.profile)

			clearLoadConfigCache()
			t.Cleanup(func() {
				clearLoadConfigCache()
				SetupEnv()
			})
			t.Setenv(prefix+"__PASSWORD", tt.password)
			t.Setenv(prefix+"__USERNAME", "1001")
			t.Setenv(prefix+"__CLIENTID", "9001")
			t.Setenv(prefix+"__QOS", "2")
			t.Setenv(prefix+"__INSECURESKIPVERIFY", "true")
			SetupEnv()

			c := make(map[string]interface{})
			err := LoadConfigByName("mqtt_source.yaml", &c)
			require.NoError(t, err)

			def, ok := c[tt.profile].(map[string]interface{})
			require.True(t, ok)
			assert.Equal(t, tt.password, def["password"])
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
			assert.Equal(t, tt.password, cfg.Password)
			assert.Equal(t, "1001", cfg.Username)
			assert.Equal(t, "9001", cfg.ClientId)
			assert.Equal(t, 2, cfg.Qos)
		})
	}
}

func TestEnvTypesFollowPropertyPaths(t *testing.T) {
	// Same leaf name at three depths, plus misleading metadata outside properties.
	metadata := `{
		"about": {"name": "value", "type": "bool"},
		"properties": {"default": [
			{"name": "value", "type": "string", "default": ""},
			{"name": "nested", "type": "object", "default": {
				"value": {"name": "value", "type": "int", "default": 0},
				"inner": {"name": "inner", "type": "object", "default": [
					{"name": "value", "type": "bool", "default": false}
				]}
			}}
		]}
	}`
	p := filepath.Join(t.TempDir(), "example.yaml")
	require.NoError(t, os.WriteFile(jsonPathForFile(p), []byte(metadata), 0o600))
	for _, profile := range []string{"DEFAULT", "CUSTOM"} {
		t.Run(profile, func(t *testing.T) {
			config := make(map[string]interface{})
			env := map[string]string{
				"EXAMPLE__" + profile + "__VALUE":                " 123456 ",
				"EXAMPLE__" + profile + "__NESTED__VALUE":        "2",
				"EXAMPLE__" + profile + "__NESTED__INNER__VALUE": "true",
				"EXAMPLE__" + profile + "__UNKNOWN__VALUE":       "3",
			}
			require.NoError(t, process(config, env, "EXAMPLE", p))
			assert.Equal(t, map[string]interface{}{
				"value": " 123456 ",
				"nested": map[string]interface{}{
					"value": int64(2),
					"inner": map[string]interface{}{"value": true},
				},
				"unknown": map[string]interface{}{"value": int64(3)},
			}, config[getConfigKey(profile)])
		})
	}
}

func TestEnvTypesWithListProperties(t *testing.T) {
	p := filepath.Join(t.TempDir(), "example.yaml")
	require.NoError(t, os.WriteFile(jsonPathForFile(p), []byte(`{
		"properties": [{"name": "value", "type": "string"}]
	}`), 0o600))
	config := make(map[string]interface{})
	require.NoError(t, process(config, map[string]string{
		"EXAMPLE__CUSTOM__VALUE": "123456",
	}, "EXAMPLE", p))
	assert.Equal(t, map[string]interface{}{
		"custom": map[string]interface{}{"value": "123456"},
	}, config)
}

func TestEnvTypesWithoutMetadata(t *testing.T) {
	config := make(map[string]interface{})
	p := filepath.Join(t.TempDir(), "example.yaml")
	require.NoError(t, process(config, map[string]string{
		"EXAMPLE__CUSTOM__VALUE":          "123456",
		"EXAMPLE__CUSTOM__PATTERN__COUNT": "50",
	}, "EXAMPLE", p))
	assert.Equal(t, map[string]interface{}{
		"custom": map[string]interface{}{
			"value":   int64(123456),
			"pattern": map[string]interface{}{"count": int64(50)},
		},
	}, config)
}

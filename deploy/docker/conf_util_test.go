package main

import (
	"fmt"
	"reflect"
	"testing"
)

func TestHandle(t *testing.T) {
	var tests = []struct {
		config map[string]interface{}
		skeys  []string
		val    string
		exp    map[string]interface{}
	}{
		{
			config: map[string]interface{}{
				"default": map[string]interface{}{
					"protocol": "tcp",
					"port":     5563,
					"optional": map[string]interface{}{
						"ClientId": "client1",
					},
				},
			},
			skeys: []string{"default", "protocol"},
			val:   "ssl",
			exp: map[string]interface{}{
				"default": map[string]interface{}{
					"protocol": "ssl",
					"port":     5563,
					"optional": map[string]interface{}{
						"ClientId": "client1",
					},
				},
			},
		},

		{
			config: map[string]interface{}{
				"default": map[string]interface{}{
					"protocol": "tcp",
					"port":     5563,
					"optional": map[string]interface{}{
						"ClientId": "client1",
					},
				},
			},
			skeys: []string{"default", "optional", "CLIENTID"},
			val:   "client2",
			exp: map[string]interface{}{
				"default": map[string]interface{}{
					"protocol": "tcp",
					"port":     5563,
					"optional": map[string]interface{}{
						"ClientId": "client2",
					},
				},
			},
		},

		{
			config: map[string]interface{}{
				"default": map[string]interface{}{
					"protocol": "tcp",
					"port":     5563,
					"optional": map[string]interface{}{
						"ClientId": "client1",
					},
				},
			},
			skeys: []string{"default", "optional", "KEEPALIVE"},
			val:   "6000",
			exp: map[string]interface{}{
				"default": map[string]interface{}{
					"protocol": "tcp",
					"port":     5563,
					"optional": map[string]interface{}{
						"ClientId":  "client1",
						"KeepAlive": int64(6000),
					},
				},
			},
		},

		{
			config: map[string]interface{}{
				"default": map[string]interface{}{
					"protocol": "tcp",
					"port":     5563,
					"optional": map[string]interface{}{
						"ClientId": "client1",
					},
				},
			},
			skeys: []string{"default", "optional", "RETAINED"},
			val:   "true",
			exp: map[string]interface{}{
				"default": map[string]interface{}{
					"protocol": "tcp",
					"port":     5563,
					"optional": map[string]interface{}{
						"ClientId": "client1",
						"Retained": true,
					},
				},
			},
		},

		{
			config: map[string]interface{}{
				"default": map[string]interface{}{
					"protocol": "tcp",
					"port":     5563,
					"optional": map[string]interface{}{
						"ClientId": "client1",
					},
				},
			},
			skeys: []string{"default", "optional", "test"},
			val:   "3.14",
			exp: map[string]interface{}{
				"default": map[string]interface{}{
					"protocol": "tcp",
					"port":     5563,
					"optional": map[string]interface{}{
						"ClientId": "client1",
						"test":     3.14,
					},
				},
			},
		},

		{
			config: map[string]interface{}{
				"default": map[string]interface{}{
					"protocol": "tcp",
					"port":     5563,
					"optional": map[string]interface{}{
						"ClientId": "client1",
					},
				},
			},
			skeys: []string{"application_conf", "test"},
			val:   "ssl",
			exp: map[string]interface{}{
				"default": map[string]interface{}{
					"protocol": "tcp",
					"port":     5563,
					"optional": map[string]interface{}{
						"ClientId": "client1",
					},
				},
				"application_conf": map[string]interface{}{
					"test": "ssl",
				},
			},
		},
	}

	for i, tt := range tests {
		Handle("edgex", tt.config, tt.skeys, tt.val)
		if !reflect.DeepEqual(tt.exp, tt.config) {
			t.Errorf("%d \tresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.exp, tt.config)
		}
	}
}

func TestProcessEnv(t *testing.T) {
	fileMap["edgex"] = "test/edgex.yaml"
	var tests = []struct {
		vars []string
		file string
		expt map[string]interface{}
		out  string
	}{
		{
			vars: []string{
				"EDGEX__DEFAULT__TYPE=zmq",
				"EDGEX__DEFAULT__OPTIONAL__CLIENTID=clientid_0000",
				"EDGEX__DEFAULT__OPTIONAL__PASSWORD=should_not_print",
				"EDGEX__APPLICATION_CONF__PROTOCOL=ssl",
			},
			file: "edgex",
			expt: map[string]interface{}{
				"default": map[string]interface{}{
					"protocol": "tcp",
					"type":     "zmq",
					"optional": map[string]interface{}{
						"ClientId": "clientid_0000",
						"Password": "should_not_print",
					},
				},
				"application_conf": map[string]interface{}{
					"protocol": "ssl",
				},
			},
			out: "application_conf:\n    protocol: ssl\ndefault:\n    optional:\n        ClientId: clientid_0000\n        Password: '*'\n    protocol: tcp\n    type: zmq\n",
		},
	}
	files := make(map[string]map[string]interface{})
	for i, tt := range tests {
		ProcessEnv(files, tt.vars)
		if !reflect.DeepEqual(tt.expt, files[tt.file]) {
			t.Errorf("%d \tresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.expt, files[tt.file])
		}
		for f, v := range files {
			p := toPrintableString(v)
			if !reflect.DeepEqual(tt.out, p) {
				t.Errorf("%d \tresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.out, p)
			}
			message := fmt.Sprintf("-------------------\nConf file %s: \n %s", f, p)
			fmt.Println(message)
		}
	}
}

func TestProcessEnvArrayValue(t *testing.T) {
	fileMap["mqtt_source"] = "test/mqtt_source.yaml"
	var tests = []struct {
		vars []string
		file string
		expt map[string]interface{}
	}{
		{
			vars: []string{
				"MQTT_SOURCE__DEFAULT__SERVERS=[tcp://10.211.55.12:1883,tcp://10.211.55.13:1883]",
				"MQTT_SOURCE__DEFAULT__TEST=[1,2]",
			},
			file: "mqtt_source",
			expt: map[string]interface{}{
				"default": map[string]interface{}{
					"servers": []interface{}{"tcp://10.211.55.12:1883", "tcp://10.211.55.13:1883"},
					"test":    []interface{}{int64(1), int64(2)},
				},
			},
		},
	}
	files := make(map[string]map[string]interface{})
	for i, tt := range tests {
		ProcessEnv(files, tt.vars)
		if !reflect.DeepEqual(tt.expt, files[tt.file]) {
			t.Errorf("%d \tresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.expt, files[tt.file])
		}
	}
}

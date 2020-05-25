package main

import (
	"reflect"
	"testing"
)

func TestHandle(t *testing.T) {
	var tests = []struct {
		config map[interface{}]interface{}
		skeys  []string
		val    string
		exp    map[interface{}]interface{}
	}{
		{
			config: map[interface{}]interface{}{
				"default": map[interface{}]interface{} {
					"protocol": "tcp",
					"port": 5563,
					"optional": map[interface{}] interface{} {
						"ClientId": "client1",
					},
				},
			},
			skeys:[]string{"default", "protocol"},
			val: "ssl",
			exp: map[interface{}]interface{}{
				"default": map[interface{}]interface{} {
					"protocol": "ssl",
					"port": 5563,
					"optional": map[interface{}] interface{} {
						"ClientId": "client1",
					},
				},
			},
		},

		{
			config: map[interface{}]interface{}{
				"default": map[interface{}]interface{} {
					"protocol": "tcp",
					"port": 5563,
					"optional": map[interface{}] interface{} {
						"ClientId": "client1",
					},
				},
			},
			skeys:[]string{"default", "optional", "CLIENTID"},
			val: "client2",
			exp: map[interface{}]interface{}{
				"default": map[interface{}]interface{} {
					"protocol": "tcp",
					"port": 5563,
					"optional": map[interface{}] interface{} {
						"ClientId": "client2",
					},
				},
			},
		},

		{
			config: map[interface{}]interface{}{
				"default": map[interface{}]interface{} {
					"protocol": "tcp",
					"port": 5563,
					"optional": map[interface{}] interface{} {
						"ClientId": "client1",
					},
				},
			},
			skeys:[]string{"default", "optional", "KEEPALIVE"},
			val: "5000",
			exp: map[interface{}]interface{}{
				"default": map[interface{}]interface{} {
					"protocol": "tcp",
					"port": 5563,
					"optional": map[interface{}] interface{} {
						"ClientId": "client1",
						"KeepAlive": "5000",
					},
				},
			},
		},

		{
			config: map[interface{}]interface{}{
				"default": map[interface{}]interface{} {
					"protocol": "tcp",
					"port": 5563,
					"optional": map[interface{}] interface{} {
						"ClientId": "client1",
					},
				},
			},
			skeys:[]string{"application_conf", "test"},
			val: "ssl",
			exp: map[interface{}]interface{}{
				"default": map[interface{}]interface{} {
					"protocol": "tcp",
					"port": 5563,
					"optional": map[interface{}] interface{} {
						"ClientId": "client1",
					},
				},
				"application_conf": map[interface{}]interface{} {
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
		expt map[interface{}]interface{}
	}{
		{
			vars: []string{
				"EDGEX__DEFAULT__TYPE=zmq",
				"EDGEX__DEFAULT__OPTIONAL__CLIENTID=clientid_0000",
				"EDGEX__APPLICATION_CONF__PROTOCOL=ssl",
			},
			file: "edgex",
			expt: map[interface{}]interface{}{
				"default": map[interface {}]interface{} {
					"protocol": "tcp",
					"type": "zmq",
					"optional": map[interface{}] interface{} {
						"ClientId": "clientid_0000",
					},
				},
				"application_conf": map[interface{}]interface{} {
					"protocol":"ssl",
				},
			},
		},
	}
	files := make(map[string]map[interface{}]interface{})
	for i, tt := range tests {
		ProcessEnv(files, tt.vars)
		if !reflect.DeepEqual(tt.expt, files[tt.file]) {
			t.Errorf("%d \tresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.expt, files[tt.file])
		}
	}
}

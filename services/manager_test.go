package services

import (
	"github.com/emqx/kuiper/xsql"
	"reflect"
	"testing"
)

var m *Manager

func init() {
	m, _ = GetServiceManager()
	m.InitByFiles()
	xsql.InitFuncRegisters(m)
}

func TestInitByFiles(t *testing.T) {
	//expects
	name := "sample"
	info := &serviceInfo{
		About: &about{
			Author: &author{
				Name:    "EMQ",
				Email:   "contact@emqx.io",
				Company: "EMQ Technologies Co., Ltd",
				Website: "https://www.emqx.io",
			},
			HelpUrl: &fileLanguage{
				English: "https://github.com/emqx/kuiper/blob/master/docs/en_US/plugins/functions/functions.md",
				Chinese: "https://github.com/emqx/kuiper/blob/master/docs/zh_CN/plugins/functions/functions.md",
			},
			Description: &fileLanguage{
				English: "Sample external services for test only",
				Chinese: "示例外部函数配置，仅供测试",
			},
		},
		Interfaces: map[string]*interfaceInfo{
			"tsrpc": {
				Addr:     "tcp://localhost:50051",
				Protocol: GRPC,
				Schema: &schemaInfo{
					SchemaType: PROTOBUFF,
					SchemaFile: "hw.proto",
				},
				Functions: []string{
					"helloFromGrpc",
					"ComputeFromGrpc",
					"getFeatureFromGrpc",
					"objectDetectFromGrpc",
					"getStatusFromGrpc",
					"notUsedRpc",
				},
			},
			"tsrest": {
				Addr:     "http://localhost:51234",
				Protocol: REST,
				Schema: &schemaInfo{
					SchemaType: PROTOBUFF,
					SchemaFile: "hw.proto",
				},
				Options: map[string]interface{}{
					"insecureSkipVerify": true,
					"headers": map[string]interface{}{
						"Accept-Charset": "utf-8",
					},
				},
				Functions: []string{
					"helloFromRest",
					"ComputeFromRest",
					"getFeatureFromRest",
					"objectDetectFromRest",
					"getStatusFromRest",
					"restEncodedJson",
				},
			},
			"tsmsgpack": {
				Addr:     "tcp://localhost:50000",
				Protocol: MSGPACK,
				Schema: &schemaInfo{
					SchemaType: PROTOBUFF,
					SchemaFile: "hw.proto",
				},
				Functions: []string{
					"helloFromMsgpack",
					"ComputeFromMsgpack",
					"getFeatureFromMsgpack",
					"objectDetectFromMsgpack",
					"getStatusFromMsgpack",
					"notUsedMsgpack",
				},
			},
		},
	}
	funcs := map[string]*functionContainer{
		"helloFromGrpc": {
			ServiceName:   "sample",
			InterfaceName: "tsrpc",
			MethodName:    "SayHello",
		},
		"helloFromRest": {
			ServiceName:   "sample",
			InterfaceName: "tsrest",
			MethodName:    "SayHello",
		},
		"helloFromMsgpack": {
			ServiceName:   "sample",
			InterfaceName: "tsmsgpack",
			MethodName:    "SayHello",
		},
		"objectDetectFromGrpc": {
			ServiceName:   "sample",
			InterfaceName: "tsrpc",
			MethodName:    "object_detection",
		},
		"objectDetectFromRest": {
			ServiceName:   "sample",
			InterfaceName: "tsrest",
			MethodName:    "object_detection",
		},
		"objectDetectFromMsgpack": {
			ServiceName:   "sample",
			InterfaceName: "tsmsgpack",
			MethodName:    "object_detection",
		},
		"getFeatureFromGrpc": {
			ServiceName:   "sample",
			InterfaceName: "tsrpc",
			MethodName:    "get_feature",
		},
		"getFeatureFromRest": {
			ServiceName:   "sample",
			InterfaceName: "tsrest",
			MethodName:    "get_feature",
		},
		"getFeatureFromMsgpack": {
			ServiceName:   "sample",
			InterfaceName: "tsmsgpack",
			MethodName:    "get_feature",
		},
		"getStatusFromGrpc": {
			ServiceName:   "sample",
			InterfaceName: "tsrpc",
			MethodName:    "getStatus",
		},
		"getStatusFromRest": {
			ServiceName:   "sample",
			InterfaceName: "tsrest",
			MethodName:    "getStatus",
		},
		"getStatusFromMsgpack": {
			ServiceName:   "sample",
			InterfaceName: "tsmsgpack",
			MethodName:    "getStatus",
		},
		"ComputeFromGrpc": {
			ServiceName:   "sample",
			InterfaceName: "tsrpc",
			MethodName:    "Compute",
		},
		"ComputeFromRest": {
			ServiceName:   "sample",
			InterfaceName: "tsrest",
			MethodName:    "Compute",
		},
		"ComputeFromMsgpack": {
			ServiceName:   "sample",
			InterfaceName: "tsmsgpack",
			MethodName:    "Compute",
		},
		"notUsedRpc": {
			ServiceName:   "sample",
			InterfaceName: "tsrpc",
			MethodName:    "RestEncodedJson",
		},
		"restEncodedJson": {
			ServiceName:   "sample",
			InterfaceName: "tsrest",
			MethodName:    "RestEncodedJson",
		},
		"notUsedMsgpack": {
			ServiceName:   "sample",
			InterfaceName: "tsmsgpack",
			MethodName:    "RestEncodedJson",
		},
	}

	err := m.serviceKV.Open()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer m.serviceKV.Close()
	actualService := &serviceInfo{}
	ok, err := m.serviceKV.Get(name, actualService)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if !ok {
		t.Errorf("service %s not found", name)
		t.FailNow()
	}
	if !reflect.DeepEqual(info, actualService) {
		t.Errorf("service info mismatch, expect %v but got %v", info, actualService)
	}

	err = m.functionKV.Open()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer m.functionKV.Close()
	actualKeys, _ := m.functionKV.Keys()
	if len(funcs) != len(actualKeys) {
		t.Errorf("functions info mismatch: expect %d funcs but got %v", len(funcs), actualKeys)
	}
	for f, c := range funcs {
		actualFunc := &functionContainer{}
		ok, err := m.functionKV.Get(f, actualFunc)
		if err != nil {
			t.Error(err)
			break
		}
		if !ok {
			t.Errorf("function %s not found", f)
			break
		}
		if !reflect.DeepEqual(c, actualFunc) {
			t.Errorf("func info mismatch, expect %v but got %v", c, actualFunc)
		}
	}
}

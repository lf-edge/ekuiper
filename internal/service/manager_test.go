// Copyright 2021 EMQ Technologies Co., Ltd.
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

package service

import (
	"github.com/lf-edge/ekuiper/internal/binder"
	"github.com/lf-edge/ekuiper/internal/binder/function"
	"reflect"
	"testing"
)

var m *Manager

func init() {
	serviceManager, err := InitManager()
	if err != nil {
		panic(err)
	}
	err = function.Initialize([]binder.FactoryEntry{{Name: "external service", Factory: serviceManager}})
	if err != nil {
		panic(err)
	}
	m = GetManager()
	m.InitByFiles()
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
				English: "https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/plugins/functions/functions.md",
				Chinese: "https://github.com/lf-edge/ekuiper/blob/master/docs/zh_CN/plugins/functions/functions.md",
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
		"ListShelves": {
			ServiceName:   "httpSample",
			InterfaceName: "bookshelf",
			MethodName:    "ListShelves",
		},
		"CreateShelf": {
			ServiceName:   "httpSample",
			InterfaceName: "bookshelf",
			MethodName:    "CreateShelf",
		},
		"GetShelf": {
			ServiceName:   "httpSample",
			InterfaceName: "bookshelf",
			MethodName:    "GetShelf",
		},
		"DeleteShelf": {
			ServiceName:   "httpSample",
			InterfaceName: "bookshelf",
			MethodName:    "DeleteShelf",
		},
		"ListBooks": {
			ServiceName:   "httpSample",
			InterfaceName: "bookshelf",
			MethodName:    "ListBooks",
		},
		"createBook": {
			ServiceName:   "httpSample",
			InterfaceName: "bookshelf",
			MethodName:    "CreateBook",
		},
		"GetBook": {
			ServiceName:   "httpSample",
			InterfaceName: "bookshelf",
			MethodName:    "GetBook",
		},
		"DeleteBook": {
			ServiceName:   "httpSample",
			InterfaceName: "bookshelf",
			MethodName:    "DeleteBook",
		},
		"GetMessage": {
			ServiceName:   "httpSample",
			InterfaceName: "messaging",
			MethodName:    "GetMessage",
		},
		"SearchMessage": {
			ServiceName:   "httpSample",
			InterfaceName: "messaging",
			MethodName:    "SearchMessage",
		},
		"UpdateMessage": {
			ServiceName:   "httpSample",
			InterfaceName: "messaging",
			MethodName:    "UpdateMessage",
		},
		"PatchMessage": {
			ServiceName:   "httpSample",
			InterfaceName: "messaging",
			MethodName:    "PatchMessage",
		},
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

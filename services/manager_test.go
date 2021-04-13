package services

import (
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/common/kv"
	"path"
	"reflect"
	"sync"
	"testing"
)

func TestInitByFiles(t *testing.T) {
	etcDir, _ := common.GetLoc("/services/test")
	dbDir, _ := common.GetDataLoc()
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
				Addr:     "localhost:50051",
				Protocol: GRPC,
				Schema: &schemaInfo{
					SchemaType: PROTOBUFF,
					SchemaFile: "hw.proto",
				},
				Functions: []string{
					"helloFromGrpc",
					"Compute",
					"get_feature",
					"get_similarity",
					"object_detection",
				},
			},
			"tsrest": {
				Addr:     "http://localhost:51234",
				Protocol: REST,
				Schema: &schemaInfo{
					SchemaType: PROTOBUFF,
					SchemaFile: "hw.proto",
				},
				Functions: []string{
					"helloFromRest",
					"Compute",
					"get_feature",
					"get_similarity",
					"objectDetect",
				},
			},
			"tsmsgpack": {
				Addr:     "localhost:50000",
				Protocol: MSGPACK,
				Schema: &schemaInfo{
					SchemaType: PROTOBUFF,
					SchemaFile: "hw.proto",
				},
				Functions: []string{
					"helloFromMsgpack",
					"Compute",
					"get_feature",
					"get_similarity",
					"object_detection",
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
		"objectDetect": {
			ServiceName:   "sample",
			InterfaceName: "tsrest",
			MethodName:    "object_detection",
		},
		"helloFromMsgpack": {
			ServiceName:   "sample",
			InterfaceName: "tsmsgpack",
			MethodName:    "SayHello",
		},
		"Compute": { // Overridden of functions
			ServiceName:   "sample",
			InterfaceName: "tsmsgpack",
			MethodName:    "Compute",
		},
		"get_feature": { // Overridden of functions
			ServiceName:   "sample",
			InterfaceName: "tsmsgpack",
			MethodName:    "get_feature",
		},
		"get_similarity": { // Overridden of functions
			ServiceName:   "sample",
			InterfaceName: "tsmsgpack",
			MethodName:    "get_similarity",
		},
		"object_detection": { // Overridden of functions
			ServiceName:   "sample",
			InterfaceName: "tsmsgpack",
			MethodName:    "object_detection",
		},
	}

	// run and compare

	m := &Manager{
		executorPool: &sync.Map{},

		etcDir:     etcDir,
		serviceKV:  kv.GetDefaultKVStore(path.Join(dbDir, "services")),
		functionKV: kv.GetDefaultKVStore(path.Join(dbDir, "serviceFuncs")),
	}
	m.serviceKV.Open()
	m.functionKV.Open()
	err := m.InitByFiles()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	err = m.serviceKV.Open()
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

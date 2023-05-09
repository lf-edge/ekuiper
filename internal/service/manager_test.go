// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	"archive/zip"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/lf-edge/ekuiper/internal/binder"
	"github.com/lf-edge/ekuiper/internal/binder/function"
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

func TestManage(t *testing.T) {
	// Test HasFunctionSet
	if m.HasFunctionSet("sample") {
		t.Error("HasFunctionSet failed, got true should be false")
	}
	if !m.HasService("sample") {
		t.Error("service sample not found")
	}

	_, err := m.Function("ListShelves")
	if err != nil {
		t.Errorf("Function ListShelves failed: %v", err)
	}

	_, ok := m.ConvName("ListShelves")
	if !ok {
		t.Error("ConvName for ListShelves failed")
	}

	_, ok = m.ConvName("NotExist")
	if ok {
		t.Error("ConvName for NotExist should failed")
	}

	initServices := []string{"httpSample", "sample"}
	list, err := m.List()
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(initServices, list) {
		t.Errorf("Get service list error, \nexpect\t\t%v, \nbut got\t\t%v", initServices, list)
	}
	// Create the zip file
	baseFolder := filepath.Join(m.etcDir, "toadd")
	os.MkdirAll(filepath.Join(m.etcDir, "temp"), 0755)
	outPath := filepath.Join(m.etcDir, "temp", "dynamic.zip")
	outFile, err := os.Create(outPath)
	if err != nil {
		fmt.Println(err)
	}
	defer os.Remove(outPath)

	// Create a new zip archive.
	w := zip.NewWriter(outFile)
	addFiles(w, baseFolder, "")
	err = w.Close()
	if err != nil {
		fmt.Println(err)
	}

	// Install the dynamic zip
	url, err := urlFromFilePath(outPath)
	if err != nil {
		t.Errorf("Create URL from file path %s: %v", outPath, err)
		return
	}
	err = m.Create(&ServiceCreationRequest{
		Name: "dynamic",
		File: url.String(),
	})
	if err != nil {
		t.Errorf("Create dynamic service failed: %v", err)
		return
	}
	dService, err := m.Get("dynamic")
	if err != nil {
		t.Errorf("Get dynamic service error: %v", err)
	} else if len(dService.Interfaces) != 1 {
		t.Errorf("dynamic service should have 1 interface, but got %d", len(dService.Interfaces))
	}

	expectedService := map[string]string{
		"dynamic": `{"name":"dynamic","file":"` + url.String() + `"}`,
	}
	allServices := m.GetAllServices()
	if !reflect.DeepEqual(expectedService, allServices) {
		t.Errorf("Get all installed service faile \nexpect\t\t%v, \nbut got\t\t%v", expectedService, allServices)
	}

	allServicesStatus := m.GetAllServicesStatus()
	if len(allServicesStatus) != 0 {
		t.Errorf("Get all installed service status faile, expect 0 but got %d", len(allServicesStatus))
	}

	expectedFunctions := []string{"ListShelves", "CreateShelf", "GetShelf", "DeleteShelf", "ListBooks", "createBook", "GetBook", "DeleteBook", "GetMessage", "SearchMessage", "UpdateMessage", "PatchMessage", "helloFromGrpc", "ComputeFromGrpc", "getFeatureFromGrpc", "objectDetectFromGrpc", "getStatusFromGrpc", "notUsedRpc", "helloFromRest", "ComputeFromRest", "getFeatureFromRest", "objectDetectFromRest", "getStatusFromRest", "restEncodedJson", "helloFromMsgpack", "ComputeFromMsgpack", "getFeatureFromMsgpack", "objectDetectFromMsgpack", "getStatusFromMsgpack", "notUsedMsgpack", "SayHello2"}
	sort.Strings(expectedFunctions)

	functions, _ := m.ListFunctions()
	sort.Strings(functions)
	if !reflect.DeepEqual(expectedFunctions, functions) {
		t.Errorf("Get all installed functions faile \nexpect\t\t%v, \nbut got\t\t%v", expectedFunctions, functions)
	}

	err = m.Update(&ServiceCreationRequest{
		Name: "dynamic",
		File: url.String(),
	})
	if err != nil {
		t.Errorf("Create dynamic service failed: %v", err)
		return
	}

	m.UninstallAllServices()
	allServices = m.GetAllServices()
	if len(allServices) != 0 {
		t.Errorf("Uninstall all services failed, expect 0 but got %d", len(allServices))
	}

	importedService := map[string]string{
		"wrongFormat": "nnn",
		"dynamic":     `{"name":"dynamic","file":"` + url.String() + `"}`,
		"wrongPath":   `{"name":"dynamic","file":"wrongpath"}`,
	}
	m.ImportServices(importedService)

	allServicesStatus = m.GetAllServicesStatus()
	if len(allServicesStatus) != 2 {
		t.Errorf("Get all installed service status faile, expect 2 error but got %v", allServicesStatus)
	}

	expectedList := []string{"httpSample", "sample", "dynamic"}
	list, err = m.List()
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(expectedList, list) {
		t.Errorf("Get service list error, \nexpect\t\t%v, \nbut got\t\t%v", expectedList, list)
	}

	err = m.Delete("dynamic")
	if err != nil {
		t.Errorf("Delete dynamic service error: %v", err)
	}

	list, err = m.List()
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(initServices, list) {
		t.Errorf("Get service list error, \nexpect\t\t%v, \nbut got\t\t%v", initServices, list)
	}
}

func addFiles(w *zip.Writer, basePath, baseInZip string) {
	// Open the Directory
	files, err := os.ReadDir(basePath)
	if err != nil {
		fmt.Println(err)
	}

	for _, file := range files {
		if !file.IsDir() {
			dat, err := os.ReadFile(filepath.Join(basePath, file.Name()))
			if err != nil {
				fmt.Println(err)
			}

			// Add some files to the archive.
			f, err := w.Create(filepath.Join(baseInZip, file.Name()))
			if err != nil {
				fmt.Println(err)
			}
			_, err = f.Write(dat)
			if err != nil {
				fmt.Println(err)
			}
		} else if file.IsDir() {

			// Recurse
			newBase := filepath.Join(basePath, file.Name())
			fmt.Println("Recursing and Adding SubDir: " + file.Name())
			fmt.Println("Recursing and Adding SubDir: " + newBase)

			addFiles(w, newBase, filepath.Join(baseInZip, file.Name()))
		}
	}
}

func urlFromFilePath(path string) (*url.URL, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("path %s is not absolute", path)
	}

	// If path has a Windows volume name, convert the volume to a host and prefix
	// per https://blogs.msdn.microsoft.com/ie/2006/12/06/file-uris-in-windows/.
	if vol := filepath.VolumeName(path); vol != "" {
		if strings.HasPrefix(vol, `\\`) {
			path = filepath.ToSlash(path[2:])
			i := strings.IndexByte(path, '/')

			if i < 0 {
				// A degenerate case.
				// \\host.example.com (without a share name)
				// becomes
				// file://host.example.com/
				return &url.URL{
					Scheme: "file",
					Host:   path,
					Path:   "/",
				}, nil
			}

			// \\host.example.com\Share\path\to\file
			// becomes
			// file://host.example.com/Share/path/to/file
			return &url.URL{
				Scheme: "file",
				Host:   path[:i],
				Path:   filepath.ToSlash(path[i:]),
			}, nil
		}

		// C:\path\to\file
		// becomes
		// file:///C:/path/to/file
		return &url.URL{
			Scheme: "file",
			Path:   "/" + filepath.ToSlash(path),
		}, nil
	}

	// /path/to/file
	// becomes
	// file:///path/to/file
	return &url.URL{
		Scheme: "file",
		Path:   filepath.ToSlash(path),
	}, nil
}

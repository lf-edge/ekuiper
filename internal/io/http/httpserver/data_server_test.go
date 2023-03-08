// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

package httpserver

import (
	"bytes"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/testx"
	"net/http"
	"testing"
)

var body = []byte(`{
        "title": "Post title",
        "body": "Post description",
        "userId": 1
    }`)

func TestEndpoints(t *testing.T) {
	testx.InitEnv()
	endpoints := []string{
		"/ee1", "/eb2", "/ec3",
	}
	RegisterEndpoint(endpoints[0], "POST", "application/json")
	RegisterEndpoint(endpoints[1], "PUT", "application/json")
	RegisterEndpoint(endpoints[2], "POST", "application/json")

	if server == nil || router == nil {
		t.Error("server or router is nil after registering")
		return
	}
	if refCount != 3 {
		t.Error("refCount is not 3 after registering")
		return
	}
	UnregisterEndpoint(endpoints[0])
	UnregisterEndpoint(endpoints[1])
	UnregisterEndpoint(endpoints[2])
	if refCount != 0 {
		t.Error("refCount is not 0 after unregistering")
		return
	}
	if server != nil || router != nil {
		t.Error("server or router is not nil after unregistering")
		return
	}
	urlPrefix := "http://localhost:10081"

	client := &http.Client{}

	RegisterEndpoint(endpoints[0], "POST", "application/json")
	_, _, err := RegisterEndpoint(endpoints[0], "PUT", "application/json")
	if err != nil {
		t.Error("RegisterEndpoint should not return error for same endpoint")
	}
	RegisterEndpoint(endpoints[1], "PUT", "application/json")

	err = testHttp(client, urlPrefix+endpoints[0], "POST")
	if err != nil {
		t.Error(err)
	}
	err = testHttp(client, urlPrefix+endpoints[1], "PUT")
	if err != nil {
		t.Error(err)
	}

	RegisterEndpoint(endpoints[2], "POST", "application/json")
	err = testHttp(client, urlPrefix+endpoints[2], "POST")
	if err != nil {
		t.Error(err)
	}
}

func testHttp(client *http.Client, url string, method string) error {
	r, err := http.NewRequest(method, url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	resp, err := client.Do(r)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status code is not 200 for %s", url)
	}
	return nil
}

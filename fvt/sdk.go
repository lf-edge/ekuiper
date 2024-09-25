// Copyright 2024 EMQ Technologies Co., Ltd.
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

package fvt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const ContentTypeJson = "application/json"

type SDK struct {
	baseUrl    *url.URL
	httpClient *http.Client
}

func NewSdk(baseUrl string) (*SDK, error) {
	u, err := url.Parse(baseUrl)
	if err != nil {
		return nil, err
	}
	return &SDK{baseUrl: u, httpClient: &http.Client{}}, nil
}

func (sdk *SDK) Get(command string) (resp *http.Response, err error) {
	return http.Get(sdk.baseUrl.JoinPath(command).String())
}

func (sdk *SDK) Post(command string, body string) (resp *http.Response, err error) {
	return http.Post(sdk.baseUrl.JoinPath(command).String(), ContentTypeJson, bytes.NewBufferString(body))
}

func (sdk *SDK) PostWithParam(command string, param string, body string) (resp *http.Response, err error) {
	u := sdk.baseUrl.JoinPath(command)
	u.RawQuery = param
	return http.Post(u.String(), ContentTypeJson, bytes.NewBufferString(body))
}

func (sdk *SDK) Delete(command string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodDelete, sdk.baseUrl.JoinPath(command).String(), nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	return sdk.httpClient.Do(req)
}

func (sdk *SDK) CreateStream(streamJson string) (resp *http.Response, err error) {
	return http.Post(sdk.baseUrl.JoinPath("streams").String(), ContentTypeJson, bytes.NewBufferString(streamJson))
}

func (sdk *SDK) DeleteStream(name string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodDelete, sdk.baseUrl.JoinPath("streams", name).String(), nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	return sdk.httpClient.Do(req)
}

func (sdk *SDK) CreateRule(ruleJson string) (resp *http.Response, err error) {
	return http.Post(sdk.baseUrl.JoinPath("rules").String(), ContentTypeJson, bytes.NewBufferString(ruleJson))
}

func (sdk *SDK) RestartRule(ruleId string) (resp *http.Response, err error) {
	return http.Post(sdk.baseUrl.JoinPath("rules", ruleId, "restart").String(), ContentTypeJson, nil)
}

func (sdk *SDK) DeleteRule(name string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodDelete, sdk.baseUrl.JoinPath("rules", name).String(), nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	return sdk.httpClient.Do(req)
}

func (sdk *SDK) GetRuleStatus(name string) (map[string]any, error) {
	resp, err := http.Get(sdk.baseUrl.JoinPath("rules", name, "status").String())
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return GetResponseResultMap(resp)
}

func GetResponseText(resp *http.Response) (string, error) {
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

func GetResponseResultMap(resp *http.Response) (result map[string]any, err error) {
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(body))
	err = json.Unmarshal(body, &result)
	if err != nil {
		fmt.Println(err)
		return
	}
	return
}

func (sdk *SDK) CreateConf(confpath string, conf map[string]any) (resp *http.Response, err error) {
	cc, err := json.Marshal(conf)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPut, sdk.baseUrl.JoinPath("metadata", confpath).String(), bytes.NewBuffer(cc))
	if err != nil {
		fmt.Println(err)
		return
	}
	return sdk.httpClient.Do(req)
}

func TryAssert(count int, interval time.Duration, tryFunc func() bool) bool {
	for count > 0 {
		time.Sleep(interval)
		if tryFunc() {
			return true
		}
		count--
	}
	return false
}

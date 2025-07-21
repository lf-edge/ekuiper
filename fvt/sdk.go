// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/v2/internal/server"
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
	return &SDK{baseUrl: u, httpClient: &http.Client{Timeout: 10 * time.Second}}, nil
}

func (sdk *SDK) Get(command string) (resp *http.Response, err error) {
	return http.Get(sdk.baseUrl.JoinPath(command).String())
}

func (sdk *SDK) Post(command string, body string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodPost, sdk.baseUrl.JoinPath(command).String(), bytes.NewBufferString(body))
	if err != nil {
		fmt.Println(err)
		return
	}
	req.Header.Set("Content-Type", ContentTypeJson)
	return sdk.httpClient.Do(req)
}

func (sdk *SDK) Import(content string) (resp *http.Response, err error) {
	body := map[string]string{"content": content}
	bodyJson, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return sdk.PostWithParam("data/import", "partial=1", string(bodyJson))
}

func (sdk *SDK) PostWithParam(command string, param string, body string) (resp *http.Response, err error) {
	u := sdk.baseUrl.JoinPath(command)
	u.RawQuery = param
	return http.Post(u.String(), ContentTypeJson, bytes.NewBufferString(body))
}

func (sdk *SDK) Req(command string, method string, body string) (resp *http.Response, err error) {
	u := sdk.baseUrl.JoinPath(command)
	req, err := http.NewRequest(method, u.String(), bytes.NewBufferString(body))
	if err != nil {
		fmt.Println(err)
		return
	}
	return sdk.httpClient.Do(req)
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

func (sdk *SDK) GetStreamSchema(name string) (map[string]any, error) {
	resp, err := http.Get(sdk.baseUrl.JoinPath("streams", name, "schema").String())
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return GetResponseResultMap(resp)
}

func (sdk *SDK) GetRuleSchema(name string) (map[string]any, error) {
	resp, err := http.Get(sdk.baseUrl.JoinPath("rules", name, "schema").String())
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return GetResponseResultMap(resp)
}

func (sdk *SDK) CreateRule(ruleJson string) (resp *http.Response, err error) {
	return http.Post(sdk.baseUrl.JoinPath("rules").String(), ContentTypeJson, bytes.NewBufferString(ruleJson))
}

func (sdk *SDK) RestartRule(ruleId string) (resp *http.Response, err error) {
	return http.Post(sdk.baseUrl.JoinPath("rules", ruleId, "restart").String(), ContentTypeJson, nil)
}

func (sdk *SDK) StopRule(ruleId string) (resp *http.Response, err error) {
	return http.Post(sdk.baseUrl.JoinPath("rules", ruleId, "stop").String(), ContentTypeJson, nil)
}

func (sdk *SDK) UpdateRule(name, ruleJson string) (resp *http.Response, err error) {
	req, err := http.NewRequest(http.MethodPut, sdk.baseUrl.JoinPath("rules", name).String(), bytes.NewBufferString(ruleJson))
	if err != nil {
		fmt.Println(err)
		return
	}
	return sdk.httpClient.Do(req)
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
	err = json.Unmarshal(body, &result)
	if err != nil {
		fmt.Println(err)
		return
	}
	return
}

func GetResponseResultTextAndMap(resp *http.Response) (body []byte, result map[string]any, err error) {
	defer resp.Body.Close()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
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

func (sdk *SDK) BatchRequest(reqs []*server.EachRequest) ([]*server.EachResponse, error) {
	b, err := json.Marshal(reqs)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, sdk.baseUrl.JoinPath("/batch/req").String(), bytes.NewBuffer(b))
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	resp, err := sdk.httpClient.Do(req)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	resps := make([]*server.EachResponse, 0)
	err = json.NewDecoder(resp.Body).Decode(&resps)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return resps, nil
}

func (sdk *SDK) ResetRuleTags(name string, tags []string) (resp *http.Response, err error) {
	v, _ := json.Marshal(&server.RuleTagRequest{Tags: tags})
	url := sdk.baseUrl.JoinPath("rules", name, "tags").String()
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(v))
	if err != nil {
		fmt.Println(err)
		return
	}
	return sdk.httpClient.Do(req)
}

func (sdk *SDK) AddRuleTags(name string, tags []string) (resp *http.Response, err error) {
	v, _ := json.Marshal(&server.RuleTagRequest{Tags: tags})
	url := sdk.baseUrl.JoinPath("rules", name, "tags").String()
	req, err := http.NewRequest(http.MethodPatch, url, bytes.NewBuffer(v))
	if err != nil {
		fmt.Println(err)
		return
	}
	return sdk.httpClient.Do(req)
}

func (sdk *SDK) RemoveRuleTags(name string, keys []string) (resp *http.Response, err error) {
	v, _ := json.Marshal(&server.RuleTagRequest{Tags: keys})
	req, err := http.NewRequest(http.MethodDelete, sdk.baseUrl.JoinPath("rules", name, "tags").String(), bytes.NewBuffer(v))
	if err != nil {
		fmt.Println(err)
		return
	}
	return sdk.httpClient.Do(req)
}

func (sdk *SDK) GetRulesByTags(tags []string) (list []string, err error) {
	v, _ := json.Marshal(&server.RuleTagRequest{Tags: tags})
	req, err := http.NewRequest(http.MethodGet, sdk.baseUrl.JoinPath("rules", "tags", "match").String(), bytes.NewBuffer(v))
	if err != nil {
		fmt.Println(err)
		return
	}
	resp, err := sdk.httpClient.Do(req)
	if err != nil {
		fmt.Println(err)
		return
	}
	rsp := server.RuleTagResponse{Rules: make([]string, 0)}
	if err := json.NewDecoder(resp.Body).Decode(&rsp); err != nil {
		fmt.Println(err)
		return nil, err
	}
	return rsp.Rules, nil
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

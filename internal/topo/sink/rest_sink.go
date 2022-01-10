// Copyright 2022 EMQ Technologies Co., Ltd.
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

package sink

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/pkg/cert"
	"github.com/lf-edge/ekuiper/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type RestSink struct {
	method             string
	url                string
	headers            map[string]string
	headersTemplate    string
	bodyType           string
	timeout            int64
	sendSingle         bool
	debugResp          bool
	insecureSkipVerify bool
	certificationPath  string
	privateKeyPath     string
	rootCaPath         string

	client *http.Client
}

var methodsMap = map[string]bool{"GET": true, "HEAD": true, "POST": true, "PUT": true, "DELETE": true, "PATCH": true}

func (ms *RestSink) Configure(ps map[string]interface{}) error {
	temp, ok := ps["method"]
	if ok {
		ms.method, ok = temp.(string)
		if !ok {
			return fmt.Errorf("rest sink property method %v is not a string", temp)
		}
		ms.method = strings.ToUpper(strings.Trim(ms.method, ""))
	} else {
		ms.method = "GET"
	}
	if _, ok = methodsMap[ms.method]; !ok {
		return fmt.Errorf("invalid property method: %s", ms.method)
	}
	switch ms.method {
	case "GET", "HEAD":
		ms.bodyType = "none"
	default:
		ms.bodyType = "json"
	}

	temp, ok = ps["url"]
	if !ok {
		return fmt.Errorf("rest sink is missing property url")
	}
	ms.url, ok = temp.(string)
	if !ok {
		return fmt.Errorf("rest sink property url %v is not a string", temp)
	}
	ms.url = strings.Trim(ms.url, "")

	temp, ok = ps["headers"]
	if ok {
		switch h := temp.(type) {
		case map[string]interface{}:
			ms.headers = make(map[string]string)
			for k, v := range h {
				if v1, ok1 := v.(string); ok1 {
					ms.headers[k] = v1
				} else {
					return fmt.Errorf("header value %s for header %s is not a string", v, k)
				}
			}
		case string:
			ms.headersTemplate = h
		default:
			return fmt.Errorf("rest sink property headers %v is not a map[string]interface", temp)
		}
	}

	temp, ok = ps["bodyType"]
	if ok {
		ms.bodyType, ok = temp.(string)
		if !ok {
			return fmt.Errorf("rest sink property bodyType %v is not a string", temp)
		}
		ms.bodyType = strings.ToLower(strings.Trim(ms.bodyType, ""))
	}
	if _, ok = httpx.BodyTypeMap[ms.bodyType]; !ok {
		return fmt.Errorf("invalid property bodyType: %s, should be \"none\" or \"form\"", ms.bodyType)
	}

	temp, ok = ps["timeout"]
	if !ok {
		ms.timeout = 5000
	} else {
		to, ok := temp.(float64)
		if !ok {
			return fmt.Errorf("rest sink property timeout %v is not a number", temp)
		}
		ms.timeout = int64(to)
	}

	temp, ok = ps["sendSingle"]
	if !ok {
		ms.sendSingle = false
	} else {
		ms.sendSingle, ok = temp.(bool)
		if !ok {
			return fmt.Errorf("rest sink property sendSingle %v is not a bool", temp)
		}
	}

	temp, ok = ps["debugResp"]
	if !ok {
		ms.debugResp = false
	} else {
		ms.debugResp, ok = temp.(bool)
		if !ok {
			return fmt.Errorf("rest sink property debugResp %v is not a bool", temp)
		}
	}

	temp, ok = ps["insecureSkipVerify"]
	if !ok {
		ms.insecureSkipVerify = true
	} else {
		ms.insecureSkipVerify, ok = temp.(bool)
		if !ok {
			return fmt.Errorf("rest sink property insecureSkipVerify %v is not a bool", temp)
		}
	}

	if certPath, ok := ps["certificationPath"]; ok {
		if certPath1, ok1 := certPath.(string); ok1 {
			ms.certificationPath = certPath1
		} else {
			return fmt.Errorf("not valid rest sink property certificationPath value %v", certPath)
		}
	}

	if privPath, ok := ps["privateKeyPath"]; ok {
		if privPath1, ok1 := privPath.(string); ok1 {
			ms.privateKeyPath = privPath1
		} else {
			return fmt.Errorf("not valid rest sink property privateKeyPath value %v", privPath)
		}
	}

	if rootPath, ok := ps["rootCaPath"]; ok {
		if rootPath1, ok1 := rootPath.(string); ok1 {
			ms.rootCaPath = rootPath1
		} else {
			return fmt.Errorf("not valid rest sink property rootCaPath value %v", rootPath)
		}
	}

	return nil
}

func (ms *RestSink) Open(ctx api.StreamContext) error {
	logger := ctx.GetLogger()

	tlsOpts := cert.TlsConfigurationOptions{
		SkipCertVerify: ms.insecureSkipVerify,
		CertFile:       ms.certificationPath,
		KeyFile:        ms.privateKeyPath,
		CaFile:         ms.rootCaPath,
	}
	tlscfg, err := cert.GenerateTLSForClient(tlsOpts)
	if err != nil {
		return err
	}

	tr := &http.Transport{
		TLSClientConfig: tlscfg,
	}

	ms.client = &http.Client{
		Transport: tr,
		Timeout:   time.Duration(ms.timeout) * time.Millisecond}
	logger.Infof("open rest sink with configuration: {method: %s, url: %s, bodyType: %s, timeout: %d, header: %v, sendSingle: %v, tls cfg: %v", ms.method, ms.url, ms.bodyType, ms.timeout, ms.headers, ms.sendSingle, tlsOpts)

	if _, err := url.Parse(ms.url); err != nil {
		return err
	}
	return nil
}

type MultiErrors []error

func (me MultiErrors) AddError(err error) MultiErrors {
	me = append(me, err)
	return me
}

func (me MultiErrors) Error() string {
	s := make([]string, len(me))
	for i, v := range me {
		s = append(s, fmt.Sprintf("Error %d with info %s. \n", i, v))
	}
	return strings.Join(s, "  ")
}

func (ms *RestSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	logger.Debugf("rest sink receive %s", item)
	output, transed, err := ctx.TransformOutput(item)
	if err != nil {
		logger.Warnf("rest sink decode data error: %v", err)
		return nil
	}
	var d = item
	if transed {
		d = output
	}
	resp, err := ms.Send(ctx, d, logger)
	if err != nil {
		return fmt.Errorf("rest sink fails to send out the data: %s", err)
	} else {
		defer resp.Body.Close()
		logger.Debugf("rest sink got response %v", resp)
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			if buf, bodyErr := ioutil.ReadAll(resp.Body); bodyErr != nil {
				logger.Errorf("%s\n", bodyErr)
				return fmt.Errorf("%s: http return code: %d and error message %s", errorx.IOErr, resp.StatusCode, bodyErr)
			} else {
				logger.Errorf("%s\n", string(buf))
				return fmt.Errorf("%s: http return code: %d and error message %s", errorx.IOErr, resp.StatusCode, string(buf))
			}
		} else {
			if ms.debugResp {
				if buf, bodyErr := ioutil.ReadAll(resp.Body); bodyErr != nil {
					logger.Errorf("%s\n", bodyErr)
				} else {
					logger.Infof("Response content: %s\n", string(buf))
				}
			}
		}
	}
	return nil
}

func (ms *RestSink) Send(ctx api.StreamContext, v interface{}, logger api.Logger) (*http.Response, error) {
	temp, err := ctx.ParseDynamicProp(ms.bodyType, v)
	if err != nil {
		return nil, err
	}
	bodyType, ok := temp.(string)
	if !ok {
		return nil, fmt.Errorf("the value %v of dynamic prop %s for bodyType is not a string", ms.bodyType, temp)
	}
	temp, err = ctx.ParseDynamicProp(ms.method, v)
	if err != nil {
		return nil, err
	}
	method, ok := temp.(string)
	if !ok {
		return nil, fmt.Errorf("the value %v of dynamic prop %s for method is not a string", ms.method, temp)
	}
	temp, err = ctx.ParseDynamicProp(ms.url, v)
	if err != nil {
		return nil, err
	}
	u, ok := temp.(string)
	if !ok {
		return nil, fmt.Errorf("the value %v of dynamic prop %s for url is not a string", ms.url, temp)
	}
	var headers map[string]string
	if ms.headers != nil {
		headers = ms.headers
	} else if ms.headersTemplate != "" {
		temp, err = ctx.ParseDynamicProp(ms.headersTemplate, v)
		if err != nil {
			return nil, err
		}
		tstr, ok := temp.(string)
		if !ok {
			return nil, fmt.Errorf("the value %v of dynamic prop %s for headersTemplate is not a string", ms.headersTemplate, temp)
		}
		err = json.Unmarshal([]byte(tstr), &headers)
		if err != nil {
			return nil, fmt.Errorf("rest sink headers template decode error: %v", err)
		}
	}
	return httpx.Send(logger, ms.client, bodyType, method, u, headers, ms.sendSingle, v)
}

func (ms *RestSink) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing rest sink")
	return nil
}

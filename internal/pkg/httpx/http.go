// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package httpx

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
)

var BodyTypeMap = map[string]string{"none": "", "text": "text/plain", "json": "application/json", "html": "text/html", "xml": "application/xml", "javascript": "application/javascript", "form": ""}

// Send v must be a []byte or map
func Send(logger api.Logger, client *http.Client, bodyType string, method string, u string, headers map[string]string, sendSingle bool, v interface{}) (*http.Response, error) {
	var req *http.Request
	var err error
	switch bodyType {
	case "none":
		req, err = http.NewRequest(method, u, nil)
		if err != nil {
			return nil, fmt.Errorf("fail to create request: %v", err)
		}
	case "json", "text", "javascript", "html", "xml":
		var body = &(bytes.Buffer{})
		switch t := v.(type) {
		case []byte:
			body = bytes.NewBuffer(t)
		default:
			vj, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("invalid content: %v", v)
			}
			body = bytes.NewBuffer(vj)
		}
		req, err = http.NewRequest(method, u, body)
		if err != nil {
			return nil, fmt.Errorf("fail to create request: %v", err)
		}
		req.Header.Set("Content-Type", BodyTypeMap[bodyType])
	case "form":
		form := url.Values{}
		im, err := convertToMap(v, sendSingle)
		if err != nil {
			return nil, err
		}
		for key, value := range im {
			var vstr string
			switch value.(type) {
			case []interface{}, map[string]interface{}:
				if temp, err := json.Marshal(value); err != nil {
					return nil, fmt.Errorf("fail to parse from value: %v", err)
				} else {
					vstr = string(temp)
				}
			default:
				vstr = fmt.Sprintf("%v", value)
			}
			form.Set(key, vstr)
		}
		body := io.NopCloser(strings.NewReader(form.Encode()))
		req, err = http.NewRequest(method, u, body)
		if err != nil {
			return nil, fmt.Errorf("fail to create request: %v", err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded;param=value")
	default:
		return nil, fmt.Errorf("unsupported body type %s", bodyType)
	}

	if len(headers) > 0 {
		for k, v := range headers {
			req.Header.Set(k, v)
		}
	}
	logger.Debugf("do request: %#v", req)
	return client.Do(req)
}

func convertToMap(v interface{}, sendSingle bool) (map[string]interface{}, error) {
	switch t := v.(type) {
	case []byte:
		r := make(map[string]interface{})
		if err := json.Unmarshal(t, &r); err != nil {
			if sendSingle {
				return nil, fmt.Errorf("fail to decode content: %v", err)
			} else {
				r["result"] = string(t)
			}
		}
		return r, nil
	case map[string]interface{}:
		return t, nil
	case []map[string]interface{}:
		r := make(map[string]interface{})
		if sendSingle {
			return nil, fmt.Errorf("invalid content: %v", t)
		} else {
			j, err := json.Marshal(t)
			if err != nil {
				return nil, err
			}
			r["result"] = string(j)
		}
		return r, nil
	default:
		return nil, fmt.Errorf("invalid content: %v", v)
	}
	return nil, fmt.Errorf("invalid content: %v", v)
}

func IsValidUrl(uri string) bool {
	pu, err := url.ParseRequestURI(uri)
	if err != nil {
		return false
	}

	switch pu.Scheme {
	case "http", "https":
		u, err := url.Parse(uri)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return false
		}
	case "file":
		if pu.Host != "" || pu.Path == "" {
			return false
		}
	default:
		return false
	}
	return true
}

// ReadFile Need to close the return reader
func ReadFile(uri string) (io.ReadCloser, error) {
	conf.Log.Infof("Start to download file %s\n", uri)
	u, err := url.ParseRequestURI(uri)
	if err != nil {
		return nil, err
	}
	var src io.ReadCloser
	switch u.Scheme {
	case "file":
		// deal with windows path
		if strings.Index(u.Path, ":") == 2 {
			u.Path = u.Path[1:]
		}
		conf.Log.Debugf(u.Path)
		sourceFileStat, err := os.Stat(u.Path)
		if err != nil {
			return nil, err
		}

		if !sourceFileStat.Mode().IsRegular() {
			return nil, fmt.Errorf("%s is not a regular file", u.Path)
		}
		srcFile, err := os.Open(u.Path)
		if err != nil {
			return nil, err
		}
		src = srcFile
	case "http", "https":
		// Get the data
		timeout := time.Duration(5 * time.Minute)
		client := &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}
		resp, err := client.Get(uri)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("cannot download the file with status: %s", resp.Status)
		}
		src = resp.Body
	default:
		return nil, fmt.Errorf("unsupported url scheme %s", u.Scheme)
	}
	return src, nil
}

func DownloadFile(filepath string, uri string) error {
	src, err := ReadFile(uri)
	if err != nil {
		return err
	}
	defer src.Close()
	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, src)
	return err
}

func IsHttpUrl(str string) error {
	url, err := url.ParseRequestURI(str)
	if err != nil {
		return err
	}
	if url.Scheme != "http" && url.Scheme != "https" {
		return fmt.Errorf("Invalid scheme %s", url.Scheme)
	}
	if url.Host == "" {
		return fmt.Errorf("Invalid url, host not found")
	}
	return nil
}

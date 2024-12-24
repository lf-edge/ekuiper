// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

var BodyTypeMap = map[string]string{"none": "", "text": "text/plain", "json": "application/json", "html": "text/html", "xml": "application/xml", "javascript": "application/javascript", "form": "application/x-www-form-urlencoded;param=value"}

// Send v must be a []byte or map
func Send(logger api.Logger, client *http.Client, bodyType string, method string, u string, headers map[string]string, v any) (*http.Response, error) {
	return SendWithFormData(logger, client, bodyType, method, u, headers, nil, "", v)
}

func SendWithFormData(logger api.Logger, client *http.Client, bodyType string, method string, u string, headers map[string]string, formData map[string]string, formFieldName string, v any) (*http.Response, error) {
	var req *http.Request
	var err error
	switch bodyType {
	case "none":
		req, err = http.NewRequest(method, u, nil)
		if err != nil {
			return nil, fmt.Errorf("fail to create request: %v", err)
		}
	case "json", "text", "javascript", "html", "xml", "form", "binary":
		var body io.Reader
		switch t := v.(type) {
		case []byte:
			if bodyType == "binary" {
				body = bytes.NewBuffer(t)
			} else {
				body = strings.NewReader(string(t))
			}
		case string:
			if bodyType == "binary" {
				body = bytes.NewBuffer([]byte(t))
			} else {
				body = strings.NewReader(t)
			}
		default:
			return nil, fmt.Errorf("http send only supports bytes but receive invalid content: %v", v)
		}
		req, err = http.NewRequest(method, u, body)
		if err != nil {
			return nil, fmt.Errorf("fail to create request: %v", err)
		}
		if req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", BodyTypeMap[bodyType])
		}
	case "formdata":
		var requestBody bytes.Buffer
		writer := multipart.NewWriter(&requestBody)
		fileField, err := writer.CreateFormFile(formFieldName, strconv.FormatInt(timex.GetNowInMilli(), 10))
		if err != nil {
			return nil, fmt.Errorf("fail to create file field: %v", err)
		}
		var payload io.Reader
		switch t := v.(type) {
		case []byte:
			payload = bytes.NewBuffer(t)
		case string:
			payload = bytes.NewBufferString(t)
		default:
			return nil, fmt.Errorf("http send only supports bytes but receive invalid content: %v", v)
		}
		_, err = io.Copy(fileField, payload)
		if err != nil {
			return nil, fmt.Errorf("fail to copy payload to file field: %v", err)
		}
		for k, v := range formData {
			err := writer.WriteField(k, v)
			if err != nil {
				logger.Errorf("fail write form data field %s: %v", k, err)
			}
		}
		err = writer.Close()
		if err != nil {
			logger.Errorf("fail to close writer: %v", err)
		}
		req, err = http.NewRequest(method, u, &requestBody)
		if err != nil {
			return nil, fmt.Errorf("fail to create request: %v", err)
		}
		req.Header.Set("Content-Type", writer.FormDataContentType())
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
		conf.Log.Debug(u.Path)
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
		timeout := 5 * time.Minute
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

func DownloadFile(filepath string, uri string) (err error) {
	defer func() {
		failpoint.Inject("DownloadFileErr", func() {
			err = errors.New("DownloadFileErr")
		})
	}()
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

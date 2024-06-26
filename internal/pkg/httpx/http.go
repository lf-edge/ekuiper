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

package httpx

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
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
		var body io.Reader
		switch t := v.(type) {
		case []byte:
			body = bytes.NewBuffer(t)
		case string:
			body = strings.NewReader(t)
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

// WithBody specifies the request body.
//
// NOTICE: If the body type is form, this function will try to convert body as map[string]any, and if retErrOnConvertFailed
// is true, it returns an error on convert failure, it converts data to a raw string if set to false.
//
// If compressor is not nil and compressAlgorithm is not empty will compress body with given message.Compressor and
// set request header with key 'Accept-Encoding' and value to given compressAlgorithm.
func WithBody(body any, bodyType string, retErrOnConvertFailed bool, compressor message.Compressor, compressAlgorithm string) HTTPRequestOptions {
	return func(req *http.Request) error {
		switch bodyType {
		case "none":
			setAcceptEncodingHeader(req, compressAlgorithm)
			return nil
		case "json", "text", "javascript", "html", "xml":
			var bodyReader io.Reader
			switch t := body.(type) {
			case []byte:
				bodyReader = bytes.NewBuffer(t)
			case string:
				bodyReader = strings.NewReader(t)
			default:
				vj, err := json.Marshal(body)
				if err != nil {
					return fmt.Errorf("invalid content: %v", body)
				}
				body = bytes.NewBuffer(vj)
			}

			rc, ok := bodyReader.(io.ReadCloser)
			if !ok && bodyReader != nil {
				rc = io.NopCloser(bodyReader)
			}
			req.Body = rc
			// set content type with body type
			req.Header.Set("Content-Type", BodyTypeMap[bodyType])
		case "form":
			form := url.Values{}
			im, err := convertToMap(body, retErrOnConvertFailed)
			if err != nil {
				return err
			}

			for key, value := range im {
				var vstr string
				switch value.(type) {
				case []interface{}, map[string]interface{}:
					if temp, err := json.Marshal(value); err != nil {
						return fmt.Errorf("fail to parse from value: %v", err)
					} else {
						vstr = string(temp)
					}
				default:
					vstr = fmt.Sprintf("%v", value)
				}
				form.Set(key, vstr)
			}

			encodedFormBody := form.Encode()
			req.Body = io.NopCloser(strings.NewReader(encodedFormBody))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded;param=value")
		default:
			return fmt.Errorf("unsupported body type %s", bodyType)
		}

		// if caller passed compressor and specified compressAlgorithm, that means
		// we need to request compression with caller specified algorithm
		if compressor != nil && compressAlgorithm != "" {
			if err := compressRequest(req, bodyType, compressAlgorithm, compressor); err != nil {
				return err
			}
		}

		return nil
	}
}

func compressRequest(r *http.Request, bodyType string, algo string, compressor message.Compressor) error {
	var err error
	bodyBuf := r.Body

	// couldn't compress request when the body is nil or body type set to none
	if bodyBuf == nil || bodyType == "none" {
		return fmt.Errorf("given request doesn't has any body to compression or body type is none")
	}
	// close the original body buf when compressed
	defer bodyBuf.Close()

	var bodyBytes []byte
	bodyBytes, err = io.ReadAll(bodyBuf)
	if err != nil {
		return fmt.Errorf("read body has error when request compression, %w", err)
	}

	bodyBytes, err = compressor.Compress(bodyBytes)
	if err != nil {
		return fmt.Errorf("request compression has unexpected error, %w", err)
	}

	// rewrap the body to io.ReadCloser and set to request
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
	setAcceptEncodingHeader(r, algo)
	return nil
}

// setAcceptEncodingHeader sets the new header with specified
// algorithm.
func setAcceptEncodingHeader(r *http.Request, algo string) {
	// set Accept-Encoding header to request
	if algo == "flate" {
		// replace flate to deflate, but is really need to do this?
		algo = "deflate"
	}
	r.Header.Set(acceptEncoding, algo)
	// set header Content-Encoding and key is specified algo.
	// see: https://github.com/lf-edge/ekuiper/pull/2779#issuecomment-2071751663
	r.Header.Set(contentEncoding, algo)
}

// HTTPRequestOptions using for customized http request.
type HTTPRequestOptions func(req *http.Request) error

const (
	acceptEncoding  = "Accept-Encoding"
	contentEncoding = "Content-Encoding"

	// EmptyCompressorAlgorithm just using to beautify codes.
	EmptyCompressorAlgorithm = ""
)

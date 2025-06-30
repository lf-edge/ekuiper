// Copyright 2023-2025 EMQ Technologies Co., Ltd.
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

package http

import (
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/sirupsen/logrus"

	"github.com/lf-edge/ekuiper/v2/internal/compressor"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/cert"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

// ClientConf is the configuration for http client
// It is shared by httppull source and rest sink to configure their http client
type ClientConf struct {
	config       *RawConf
	client       *http.Client
	decompressor message.Decompressor // decompressor used to payload decompression when specifies compressAlgorithm

	// auth related
	accessConf        *AccessTokenConf
	refreshConf       *RefreshTokenConf
	tokenLastUpdateAt time.Time
	tokens            map[string]interface{}
}

type AccessTokenConf struct {
	Url            string            `json:"url"`
	Body           string            `json:"body"`
	Expire         string            `json:"expire"`
	Headers        map[string]string `json:"headers"`
	ExpireInSecond int
}

type RefreshTokenConf struct {
	Url     string            `json:"url"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

type RawConf struct {
	Url           string            `json:"url"`
	Method        string            `json:"method"`
	Body          string            `json:"body"`
	BodyType      string            `json:"bodyType"`
	Format        string            `json:"format"`
	Headers       map[string]string `json:"headers"`
	FormData      map[string]string `json:"formData"`
	FileFieldName string            `json:"fileFieldName"`
	Timeout       cast.DurationConf `json:"timeout"`
	Incremental   bool              `json:"incremental"`

	OAuth      map[string]map[string]interface{} `json:"oauth"`
	SendSingle bool                              `json:"sendSingle"`
	// Could be code or body
	ResponseType string `json:"responseType"`
	Compression  string `json:"compression"` // Compression specifies the algorithms used to payload compression

	DebugResp bool `json:"debugResp"`
}

const (
	DefaultTimeout = 5000 * time.Millisecond
)

type bodyResp struct {
	Code int `json:"code"`
}

var bodyTypeMap = map[string]string{"none": "", "text": "text/plain", "json": "application/json", "html": "text/html", "xml": "application/xml", "javascript": "application/javascript", "form": "", "binary": "application/octet-stream", "formdata": "multipart/form-data"}

// newTransport allows EdgeX Foundry, protected by OpenZiti to override and obtain a transport
// protected by OpenZiti's zero trust connectivity. See client_edgex.go where this function is
// set in an init() call
var newTransport = getTransport

func getTransport(tlscfg *tls.Config, logger *logrus.Logger) *http.Transport {
	return &http.Transport{
		TLSClientConfig: tlscfg,
	}
}

func (cc *ClientConf) InitConf(ctx api.StreamContext, device string, props map[string]interface{}) error {
	c := &RawConf{
		Url:          "http://localhost",
		Method:       http.MethodGet,
		Timeout:      cast.DurationConf(DefaultTimeout),
		ResponseType: "code",
	}

	if err := cast.MapToStruct(props, c); err != nil {
		return fmt.Errorf("fail to parse the properties: %v", err)
	}
	if c.Url == "" {
		return fmt.Errorf("url is required")
	}
	c.Url = c.Url + device
	switch strings.ToUpper(c.Method) {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete:
		c.Method = strings.ToUpper(c.Method)
	default:
		return fmt.Errorf("Not supported HTTP method %s.", c.Method)
	}
	if c.Timeout < 0 {
		return fmt.Errorf("timeout must be greater than or equal to 0")
	}

	// Set default body type if not set
	if c.BodyType == "" {
		switch c.Method {
		case http.MethodGet, http.MethodHead:
			c.BodyType = "none"
		default:
			c.BodyType = "json"
		}
	}
	if _, ok2 := bodyTypeMap[strings.ToLower(c.BodyType)]; ok2 {
		c.BodyType = strings.ToLower(c.BodyType)
	} else {
		return fmt.Errorf("Invalid body type value %v.", c.BodyType)
	}
	switch c.ResponseType {
	case "code", "body":
		// correct
	default:
		return fmt.Errorf("Invalid response type value %v.", c.ResponseType)
	}
	err := httpx.IsHttpUrl(c.Url)
	if err != nil {
		return err
	}
	// validate oAuth. In order to adapt to manager, the validation is closed to allow empty value
	if c.OAuth != nil {
		// validate access token
		if ap, ok := c.OAuth["access"]; ok {
			accessConf := &AccessTokenConf{}
			if err := cast.MapToStruct(ap, accessConf); err != nil {
				return fmt.Errorf("fail to parse the access properties of oAuth: %v", err)
			}
			if accessConf.Url == "" {
				conf.Log.Warnf("access token url is not set, so ignored the oauth setting")
				c.OAuth = nil
			} else {
				// expire time will update every time when access token is refreshed if expired is set
				cc.accessConf = accessConf
			}
		} else {
			return fmt.Errorf("if setting oAuth, `access` property is required")
		}
		// validate refresh token, it is optional
		if rp, ok := c.OAuth["refresh"]; ok {
			refreshConf := &RefreshTokenConf{}
			if err := cast.MapToStruct(rp, refreshConf); err != nil {
				return fmt.Errorf("fail to parse the refresh token properties: %v", err)
			}
			if refreshConf.Url == "" {
				conf.Log.Warnf("refresh token url is not set, so ignored the refresh setting")
				delete(c.OAuth, "refresh")
			} else {
				cc.refreshConf = refreshConf
			}
		}
	}

	tlscfg, err := cert.GenTLSConfig(ctx, props)
	if err != nil {
		return err
	}
	tr := newTransport(tlscfg, conf.Log)
	cc.client = &http.Client{
		Transport: tr,
		Timeout:   time.Duration(c.Timeout),
	}
	cc.config = c
	// that means payload need compression and decompression, so we need initialize compressor and decompressor
	if c.Compression != "" {
		cc.decompressor, err = compressor.GetDecompressor(c.Compression)
		if err != nil {
			return fmt.Errorf("init payload decompressor failed, %w", err)
		}
	}
	if cc.accessConf != nil {
		conf.Log.Infof("Try to get access token from %s", cc.accessConf.Url)
		ctx := mockContext.NewMockContext("none", "httppull_init")
		cc.tokens = make(map[string]interface{})
		err := cc.auth(ctx)
		if err != nil {
			return fmt.Errorf("fail to authorize by oAuth: %v", err)
		}
	}
	return nil
}

// initialize the oAuth access token
func (cc *ClientConf) auth(ctx api.StreamContext) error {
	resp, err := httpx.Send(conf.Log, cc.client, "json", http.MethodPost, cc.accessConf.Url, cc.accessConf.Headers, cc.accessConf.Body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	tokens, _, err := cc.parseResponse(ctx, resp, "", true, true)
	if err != nil {
		return err
	}
	cc.tokens = tokens[0]
	ctx.GetLogger().Infof("Got access token %v", cc.tokens)
	expireIn, err := ctx.ParseTemplate(cc.accessConf.Expire, cc.tokens)
	if err != nil {
		return fmt.Errorf("fail to parse the expire time for access token: %v", err)
	}
	cc.accessConf.ExpireInSecond, err = cast.ToInt(expireIn, cast.CONVERT_ALL)
	if err != nil {
		return fmt.Errorf("fail to covert the expire time %s for access token: %v", expireIn, err)
	}
	if cc.refreshConf != nil {
		err := cc.refresh(ctx)
		if err != nil {
			return err
		}
	} else {
		cc.tokenLastUpdateAt = time.Now()
	}
	return nil
}

func parseHeaders(ctx api.StreamContext, oHeaders map[string]string, data map[string]interface{}) (map[string]string, error) {
	headers := make(map[string]string, len(oHeaders))
	var err error
	for k, v := range oHeaders {
		headers[k], err = ctx.ParseTemplate(v, data)
		if err != nil {
			return nil, fmt.Errorf("fail to parse the header for refresh token request %s: %v", k, err)
		}
	}
	return headers, nil
}

func (cc *ClientConf) refresh(ctx api.StreamContext) error {
	if cc.refreshConf != nil {
		headers := make(map[string]string, len(cc.refreshConf.Headers))
		var err error
		for k, v := range cc.refreshConf.Headers {
			headers[k], err = ctx.ParseTemplate(v, cc.tokens)
			if err != nil {
				return fmt.Errorf("fail to parse the header for refresh token request %s: %v", k, err)
			}
		}
		resp, err := httpx.Send(conf.Log, cc.client, "json", http.MethodPost, cc.refreshConf.Url, headers, cc.refreshConf.Body)
		if err != nil {
			return fmt.Errorf("fail to get refresh token: %v", err)
		}
		defer resp.Body.Close()
		nt, _, err := cc.parseResponse(ctx, resp, "", true, true)
		if err != nil {
			return fmt.Errorf("Cannot parse refresh token response to json: %v", err)
		}
		for k, v := range nt[0] {
			if v != nil {
				cc.tokens[k] = v
			}
		}
		cc.tokenLastUpdateAt = time.Now()
		return nil
	} else {
		return fmt.Errorf("no oAuth config")
	}
}

const (
	BODY_ERR = "response body error"
	CODE_ERR = "response code error"
)

// responseBodyDecompress used to decompress the specified response body bytes, decompression algorithm indicated
// by response header 'Content-Encoding' value.
func (cc *ClientConf) responseBodyDecompress(ctx api.StreamContext, resp *http.Response, body []byte) ([]byte, error) {
	var err error
	// we need check response header key Content-Encoding is exist, if not that means remote server probably not support
	// configured compression algorithm and we should throw error.
	if resp.Header.Get("Content-Encoding") == "" {
		ctx.GetLogger().Warnf("Cannot find header with key 'Content-Encoding' when trying to detect response content encoding and decompress it, probably remote server does not support configured algorithm %q", cc.config.Compression)
		return nil, fmt.Errorf("try to detect and decompress payload has error, cannot find header with key 'Content-Encoding' in response")
	}
	body, err = cc.decompressor.Decompress(body)
	if err != nil {
		return nil, fmt.Errorf("try to decompress payload failed, %w", err)
	}
	return body, nil
}

// parse the response status. For rest sink, it will not return the body by default if not need to debug
func (cc *ClientConf) parseResponse(ctx api.StreamContext, resp *http.Response, lastMD5 string, returnBody bool, skipDecompression bool) ([]map[string]interface{}, string, error) {
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, "", fmt.Errorf("%s: %d", CODE_ERR, resp.StatusCode)
	} else if !returnBody { // For rest sink who only need to know if the request is successful
		return nil, "", nil
	}

	c, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("%s: %v", BODY_ERR, err)
	}
	newMD5 := ""
	if returnBody && cc.config.Incremental {
		newMD5 = getMD5Hash(c)
		if newMD5 == lastMD5 {
			return nil, newMD5, nil
		}
	}

	switch cc.config.ResponseType {
	case "code":
		if returnBody {
			if cc.config.Compression != "" && !skipDecompression {
				if c, err = cc.responseBodyDecompress(ctx, resp, c); err != nil {
					return nil, "", fmt.Errorf("try to decompress payload failed, %w", err)
				}
			}
			m, e := decode(c)
			if e != nil {
				return nil, "", fmt.Errorf("%s: decode fail for %v", BODY_ERR, e)
			}
			return m, newMD5, e
		}
		return nil, "", nil
	case "body":
		if cc.config.Compression != "" && !skipDecompression {
			if c, err = cc.responseBodyDecompress(ctx, resp, c); err != nil {
				return nil, "", fmt.Errorf("try to decompress payload failed, %w", err)
			}
		}
		payloads, err := decode(c)
		if err != nil {
			return nil, "", fmt.Errorf("%s: decode fail for %v", BODY_ERR, err)
		}
		for _, payload := range payloads {
			ro := &bodyResp{}
			err = cast.MapToStruct(payload, ro)
			if err != nil {
				return nil, "", fmt.Errorf("%s: decode fail for %v", BODY_ERR, err)
			}
			//{"code":0,"message":"success","data":null}
			if ro.Code < 200 || ro.Code > 299 {
				return nil, "", fmt.Errorf("%s: %d", CODE_ERR, ro.Code)
			}
		}
		if returnBody {
			return payloads, newMD5, nil
		}
		return nil, "", nil
	default:
		return nil, "", fmt.Errorf("%s: unsupported response type %s", BODY_ERR, cc.config.ResponseType)
	}
}

func (cc *ClientConf) parseHeaders(ctx api.StreamContext, data map[string]interface{}) (map[string]string, error) {
	return parseHeaders(ctx, cc.config.Headers, data)
}

func decode(data []byte) ([]map[string]interface{}, error) {
	var r1 interface{}
	err := json.Unmarshal(data, &r1)
	if err != nil {
		return nil, err
	}
	switch rt := r1.(type) {
	case map[string]interface{}:
		return []map[string]interface{}{rt}, nil
	case []map[string]interface{}:
		return rt, nil
	case []interface{}:
		r2 := make([]map[string]interface{}, len(rt))
		for i, m := range rt {
			if rm, ok := m.(map[string]interface{}); ok {
				r2[i] = rm
			} else {
				return nil, fmt.Errorf("only map[string]interface{} and []map[string]interface{} is supported")
			}
		}
		return r2, nil
	}
	return nil, fmt.Errorf("only map[string]interface{} and []map[string]interface{} is supported")
}

func getMD5Hash(text []byte) string {
	hash := md5.Sum(text)
	return hex.EncodeToString(hash[:])
}

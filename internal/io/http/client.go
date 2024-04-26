// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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

	"github.com/sirupsen/logrus"

	"github.com/lf-edge/ekuiper/internal/compressor"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/cert"
	"github.com/lf-edge/ekuiper/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
	mockContext "github.com/lf-edge/ekuiper/pkg/mock/context"
)

// ClientConf is the configuration for http client
// It is shared by httppull source and rest sink to configure their http client
type ClientConf struct {
	config            *RawConf
	accessConf        *AccessTokenConf
	refreshConf       *RefreshTokenConf
	tokenLastUpdateAt time.Time

	tokens map[string]interface{}
	client *http.Client

	compressor   message.Compressor   // compressor used to payload compression when specifies compressAlgorithm
	decompressor message.Decompressor // decompressor used to payload decompression when specifies compressAlgorithm
}

type RawConf struct {
	Url       string      `json:"url"`
	Method    string      `json:"method"`
	Body      string      `json:"body"`
	BodyType  string      `json:"bodyType"`
	Headers   interface{} `json:"headers"`
	Timeout   int         `json:"timeout"`
	DebugResp bool        `json:"debugResp"`
	// Could be code or body
	ResponseType string                            `json:"responseType"`
	OAuth        map[string]map[string]interface{} `json:"oauth"`
	// source specific properties
	Interval    int    `json:"interval"`
	Incremental bool   `json:"incremental"`
	ResendUrl   string `json:"resendDestination"`
	// sink specific properties
	SendSingle bool `json:"sendSingle"`
	// inferred properties
	HeadersTemplate string
	HeadersMap      map[string]string
	Compression     string `json:"compression"` // Compression specifies the algorithms used to payload compression
}

const (
	DefaultInterval = 10000
	DefaultTimeout  = 5000
)

type AccessTokenConf struct {
	Url            string `json:"url"`
	Body           string `json:"body"`
	Expire         string `json:"expire"`
	ExpireInSecond int
}

type RefreshTokenConf struct {
	Url     string            `json:"url"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

type bodyResp struct {
	Code int `json:"code"`
}

var bodyTypeMap = map[string]string{"none": "", "text": "text/plain", "json": "application/json", "html": "text/html", "xml": "application/xml", "javascript": "application/javascript", "form": ""}

type ClientConfOption struct {
	checkInterval bool
}

type WithClientConfOption func(clientConf *ClientConfOption)

// newTransport allows EdgeX Foundry, protected by OpenZiti to override and obtain a transport
// protected by OpenZiti's zero trust connectivity. See client_edgex.go where this function is
// set in an init() call
var newTransport = getTransport

func getTransport(tlscfg *tls.Config, logger *logrus.Logger) *http.Transport {
	return &http.Transport{
		TLSClientConfig: tlscfg,
	}
}

func WithCheckInterval(checkInterval bool) WithClientConfOption {
	return func(clientConf *ClientConfOption) {
		clientConf.checkInterval = checkInterval
	}
}

func (cc *ClientConf) InitConf(device string, props map[string]interface{}, withOptions ...WithClientConfOption) error {
	option := &ClientConfOption{}
	for _, withOption := range withOptions {
		withOption(option)
	}
	c := &RawConf{
		Url:          "http://localhost",
		Method:       http.MethodGet,
		Interval:     DefaultInterval,
		Timeout:      DefaultTimeout,
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
	if option.checkInterval && c.Interval <= 0 {
		return fmt.Errorf("interval must be greater than 0")
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
		return fmt.Errorf("Not valid body type value %v.", c.BodyType)
	}
	switch c.ResponseType {
	case "code", "body":
		// correct
	default:
		return fmt.Errorf("Not valid response type value %v.", c.ResponseType)
	}
	err := httpx.IsHttpUrl(c.Url)
	if err != nil {
		return err
	}
	if c.Headers != nil {
		switch h := c.Headers.(type) {
		case map[string]interface{}:
			c.HeadersMap = make(map[string]string, len(h))
			for k, v := range h {
				c.HeadersMap[k] = v.(string)
			}
		case string:
			c.HeadersTemplate = h
		default:
			return fmt.Errorf("headers must be a map or a string")
		}
	}
	tlscfg, err := cert.GenTLSConfig(props, "http")
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

	tr := newTransport(tlscfg, conf.Log)

	cc.client = &http.Client{
		Transport: tr,
		Timeout:   time.Duration(c.Timeout) * time.Millisecond,
	}
	cc.config = c

	// that means payload need compression and decompression, so we need initialize compressor and decompressor
	if c.Compression != "" {
		cc.compressor, err = compressor.GetCompressor(c.Compression)
		if err != nil {
			return fmt.Errorf("init payload compressor failed, %w", err)
		}

		cc.decompressor, err = compressor.GetDecompressor(c.Compression)
		if err != nil {
			return fmt.Errorf("init payload decompressor failed, %w", err)
		}
	}

	// try to get access token
	if cc.accessConf != nil {
		conf.Log.Infof("Try to get access token from %s", cc.accessConf.Url)
		ctx := mockContext.NewMockContext("none", "httppull_init")
		cc.tokens = make(map[string]interface{})
		err := cc.auth(ctx)
		if err != nil {
			return fmt.Errorf("fail to authorize by oAuth: %v", err)
		}
	}

	if cc.config.ResendUrl == "" {
		cc.config.ResendUrl = cc.config.Url
	}

	return nil
}

// initialize the oAuth access token
func (cc *ClientConf) auth(ctx api.StreamContext) error {
	// send authentication request and authentication request no need to compress
	if resp, e := httpx.Send(conf.Log, cc.client, cc.accessConf.Url, http.MethodPost,
		httpx.WithBody(cc.accessConf.Body, "json", true, nil, httpx.EmptyCompressorAlgorithm)); e == nil {
		conf.Log.Infof("try to get access token got response %v", resp)
		tokens, _, e := cc.parseResponse(ctx, resp, true, nil, true)
		if e != nil {
			return fmt.Errorf("Cannot parse access token response to json: %v", e)
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
	} else {
		return fmt.Errorf("fail to get access token: %v", e)
	}
	return nil
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
		rr, ee := httpx.Send(conf.Log, cc.client, cc.refreshConf.Url, http.MethodPost,
			httpx.WithBody(cc.accessConf.Body, "json", true, nil, httpx.EmptyCompressorAlgorithm),
			httpx.WithHeadersMap(headers),
		)
		if ee != nil {
			return fmt.Errorf("fail to get refresh token: %v", ee)
		}
		nt, _, err := cc.parseResponse(ctx, rr, true, nil, true)
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
	} else if cc.accessConf != nil {
		return cc.auth(ctx)
	} else {
		return fmt.Errorf("no oAuth config")
	}
}

func (cc *ClientConf) parseHeaders(ctx api.StreamContext, data interface{}) (map[string]string, error) {
	headers := make(map[string]string)
	var err error
	if cc.config.HeadersMap != nil {
		for k, v := range cc.config.HeadersMap {
			headers[k], err = ctx.ParseTemplate(v, data)
			if err != nil {
				return nil, fmt.Errorf("fail to parse the header entry %s: %v", k, err)
			}
		}
	} else if cc.config.HeadersTemplate != "" {
		tstr, err := ctx.ParseTemplate(cc.config.HeadersTemplate, data)
		if err != nil {
			return nil, fmt.Errorf("fail to parse the header template %s: %v", cc.config.HeadersTemplate, err)
		}
		err = json.Unmarshal(cast.StringToBytes(tstr), &headers)
		if err != nil {
			return nil, fmt.Errorf("parsed header template is not json: %s", tstr)
		}
	}
	return headers, nil
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
func (cc *ClientConf) parseResponse(ctx api.StreamContext, resp *http.Response, returnBody bool, omd5 *string, skipDecompression bool) ([]map[string]interface{}, []byte, error) {
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		c, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, []byte("fail to read body"),
				fmt.Errorf("%s: %d", CODE_ERR, resp.StatusCode)
		}
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				conf.Log.Errorf("fail to close the response body: %v", err)
			}
		}(resp.Body)
		return nil, c, fmt.Errorf("%s: %d", CODE_ERR, resp.StatusCode)
	} else if !returnBody { // For rest sink who only need to know if the request is successful
		return nil, nil, nil
	}

	c, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %v", BODY_ERR, err)
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			conf.Log.Errorf("fail to close the response body: %v", err)
		}
	}(resp.Body)

	if returnBody && cc.config.Incremental {
		nmd5 := getMD5Hash(c)
		if *omd5 == nmd5 {
			ctx.GetLogger().Debugf("Content has not changed since last fetch, so skip processing.")
			return nil, nil, nil
		} else {
			*omd5 = nmd5
		}
	}
	switch cc.config.ResponseType {
	case "code":
		if returnBody {
			if cc.config.Compression != "" && !skipDecompression {
				if c, err = cc.responseBodyDecompress(ctx, resp, c); err != nil {
					return nil, nil, fmt.Errorf("try to decompress payload failed, %w", err)
				}
			}
			m, e := decode(ctx, c)
			if e != nil {
				return nil, c, fmt.Errorf("%s: decode fail for %v", BODY_ERR, e)
			}
			return m, c, e
		}
		return nil, nil, nil
	case "body":
		if cc.config.Compression != "" && !skipDecompression {
			if c, err = cc.responseBodyDecompress(ctx, resp, c); err != nil {
				return nil, nil, fmt.Errorf("try to decompress payload failed, %w", err)
			}
		}
		payloads, err := decode(ctx, c)
		if err != nil {
			if err != nil {
				return nil, c, fmt.Errorf("%s: decode fail for %v", BODY_ERR, err)
			}
			return nil, c, err
		}
		for _, payload := range payloads {
			ro := &bodyResp{}
			err = cast.MapToStruct(payload, ro)
			if err != nil {
				return nil, c, fmt.Errorf("%s: decode fail for %v", BODY_ERR, err)
			}
			if ro.Code < 200 || ro.Code > 299 {
				return nil, c, fmt.Errorf("%s: %d", CODE_ERR, ro.Code)
			}
		}
		if returnBody {
			return payloads, c, nil
		}
		return nil, nil, nil
	default:
		return nil, c, fmt.Errorf("%s: unsupported response type %s", BODY_ERR, cc.config.ResponseType)
	}
}

func getMD5Hash(text []byte) string {
	hash := md5.Sum(text)
	return hex.EncodeToString(hash[:])
}

// TODO remove this function after all the sources are migrated to use the new API
func decode(ctx api.StreamContext, data []byte) ([]map[string]interface{}, error) {
	r, err := ctx.DecodeIntoList(data)
	if err == nil {
		return r, nil
	}
	var r1 interface{}
	err = json.Unmarshal(data, &r1)
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

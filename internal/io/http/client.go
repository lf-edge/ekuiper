// Copyright 2023 EMQ Technologies Co., Ltd.
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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io/mock"
	"github.com/lf-edge/ekuiper/internal/pkg/cert"
	"github.com/lf-edge/ekuiper/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"io"
	"net/http"
	"strings"
	"time"
)

// ClientConf is the configuration for http client
// It is shared by httppull source and rest sink to configure their http client
type ClientConf struct {
	config      *RawConf
	accessConf  *AccessTokenConf
	refreshConf *RefreshTokenConf

	tokens map[string]interface{}
	client *http.Client
}

type RawConf struct {
	Url                string      `json:"url"`
	Method             string      `json:"method"`
	Body               string      `json:"body"`
	BodyType           string      `json:"bodyType"`
	Headers            interface{} `json:"headers"`
	InsecureSkipVerify bool        `json:"insecureSkipVerify"`
	CertificationPath  string      `json:"certificationPath"`
	PrivateKeyPath     string      `json:"privateKeyPath"`
	RootCaPath         string      `json:"rootCaPath"`
	Timeout            int         `json:"timeout"`
	DebugResp          bool        `json:"debugResp"`
	// Could be code or body
	ResponseType string                            `json:"responseType"`
	OAuth        map[string]map[string]interface{} `json:"oauth"`
	// source specific properties
	Interval    int  `json:"interval"`
	Incremental bool `json:"incremental"`
	// sink specific properties
	SendSingle bool `json:"sendSingle"`
	// inferred properties
	HeadersTemplate string
	HeadersMap      map[string]string
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

func (cc *ClientConf) InitConf(device string, props map[string]interface{}) error {
	c := &RawConf{
		Url:                "http://localhost",
		Method:             http.MethodGet,
		Interval:           DefaultInterval,
		Timeout:            DefaultTimeout,
		InsecureSkipVerify: true,
		ResponseType:       "code",
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
	if c.Interval <= 0 {
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
		// TODO remove later, adapt to the wrong format in manager
		case []interface{}:
			c.HeadersMap = make(map[string]string, len(h))
			for _, v := range h {
				if mv, ok := v.(map[string]interface{}); ok && len(mv) == 3 {
					c.HeadersMap[mv["name"].(string)] = mv["default"].(string)
				}
			}
		default:
			return fmt.Errorf("headers must be a map or a string")
		}
	}
	tlsOpts := cert.TlsConfigurationOptions{
		SkipCertVerify: c.InsecureSkipVerify,
		CertFile:       c.CertificationPath,
		KeyFile:        c.PrivateKeyPath,
		CaFile:         c.RootCaPath,
	}

	tlscfg, err := cert.GenerateTLSForClient(tlsOpts)
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

	tr := &http.Transport{
		TLSClientConfig: tlscfg,
	}

	cc.client = &http.Client{
		Transport: tr,
		Timeout:   time.Duration(c.Timeout) * time.Millisecond,
	}
	cc.config = c

	// try to get access token
	if cc.accessConf != nil {
		conf.Log.Infof("Try to get access token from %s", cc.accessConf.Url)
		ctx := mock.NewMockContext("none", "httppull_init")
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
	if resp, e := httpx.Send(conf.Log, cc.client, "json", http.MethodPost, cc.accessConf.Url, nil, true, []byte(cc.accessConf.Body)); e == nil {
		conf.Log.Infof("try to get access token got response %v", resp)
		tokens, _, e := cc.parseResponse(ctx, resp, true, nil)
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
		rr, ee := httpx.Send(conf.Log, cc.client, "json", http.MethodPost, cc.refreshConf.Url, headers, true, []byte(cc.accessConf.Body))
		if ee != nil {
			return fmt.Errorf("fail to get refresh token: %v", ee)
		}
		nt, _, err := cc.parseResponse(ctx, rr, true, nil)
		for k, v := range nt[0] {
			if v != nil {
				cc.tokens[k] = v
			}
		}
		if err != nil {
			return fmt.Errorf("Cannot parse refresh token response to json: %v", err)
		}
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
		err = json.Unmarshal([]byte(tstr), &headers)
		if err != nil {
			return nil, fmt.Errorf("parsed header template is not json: %s", tstr)
		}
	}
	return headers, nil
}

// parse the response status. For rest sink, it will not return the body by default if not need to debug
func (cc *ClientConf) parseResponse(ctx api.StreamContext, resp *http.Response, returnBody bool, omd5 *string) ([]map[string]interface{}, []byte, error) {
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		c, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, []byte("fail to read body"),
				fmt.Errorf("http return code error: %d", resp.StatusCode)
		}
		defer resp.Body.Close()
		return nil, c, fmt.Errorf("http return code error: %d", resp.StatusCode)
	} else if !returnBody { // For rest sink who only need to know if the request is successful
		return nil, nil, nil
	}
	c, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, []byte("fail to read body"), err
	}
	defer resp.Body.Close()
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
			m, e := decode(ctx, c)
			return m, c, e
		}
		return nil, nil, nil
	case "body":
		payload, err := decode(ctx, c)
		if err != nil {
			return nil, c, err
		}
		ro := &bodyResp{}
		err = cast.MapToStruct(payload, ro)
		if err != nil {
			return nil, c, fmt.Errorf("invalid body response: %v", err)
		}
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return nil, c, fmt.Errorf("http status code is not 200: %v", payload)
		}
		if returnBody {
			return payload, c, nil
		}
		return nil, nil, nil
	default:
		return nil, c, fmt.Errorf("unsupported response type: %s", cc.config.ResponseType)
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
	if r2, ok := r1.(map[string]interface{}); ok {
		return []map[string]interface{}{r2}, nil
	}
	if rlist, ok := r1.([]interface{}); ok {
		r2 := make([]map[string]interface{}, len(rlist))
		for i, m := range rlist {
			r2[i] = m.(map[string]interface{})
		}
		return r2, nil
	}
	return nil, fmt.Errorf("only map[string]interface{} and []map[string]interface{} is supported")
}

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

package http

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/io/mock"
	"github.com/lf-edge/ekuiper/internal/pkg/cert"
	"github.com/lf-edge/ekuiper/pkg/infra"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

const DefaultInterval = 10000
const DefaultTimeout = 5000

type HTTPPullConf struct {
	Url                string            `json:"url"`
	Method             string            `json:"method"`
	Body               string            `json:"body"`
	BodyType           string            `json:"bodyType"`
	Headers            map[string]string `json:"headers"`
	InsecureSkipVerify bool              `json:"insecureSkipVerify"`
	CertificationPath  string            `json:"certificationPath"`
	PrivateKeyPath     string            `json:"privateKeyPath"`
	RootCaPath         string            `json:"rootCaPath"`
	Timeout            int               `json:"timeout"`
	// Could be code or body
	ResponseType string                            `json:"responseType"`
	OAuth        map[string]map[string]interface{} `json:"oauth"`
	// Pull specific properties
	Interval    int  `json:"interval"`
	Incremental bool `json:"incremental"`
}

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

type HTTPPullSource struct {
	config      *HTTPPullConf
	accessConf  *AccessTokenConf
	refreshConf *RefreshTokenConf

	tokens map[string]interface{}
	client *http.Client
}

type bodyResp struct {
	Code int `json:"code"`
}

var bodyTypeMap = map[string]string{"none": "", "text": "text/plain", "json": "application/json", "html": "text/html", "xml": "application/xml", "javascript": "application/javascript", "form": ""}

func (hps *HTTPPullSource) Configure(device string, props map[string]interface{}) error {
	conf.Log.Infof("Initialized Httppull source with configurations %#v.", props)
	c := &HTTPPullConf{
		Url:                "http://localhost",
		Method:             http.MethodGet,
		Interval:           DefaultInterval,
		Timeout:            DefaultTimeout,
		BodyType:           "json",
		InsecureSkipVerify: true,
		ResponseType:       "code",
		Headers:            map[string]string{},
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
	// validate oAuth
	if c.OAuth != nil {
		// validate access token
		if ap, ok := c.OAuth["access"]; ok {
			accessConf := &AccessTokenConf{}
			if err := cast.MapToStruct(ap, accessConf); err != nil {
				return fmt.Errorf("fail to parse the access properties of oAuth: %v", err)
			}
			if accessConf.Url == "" {
				return fmt.Errorf("access token url is required")
			}
			// expire time will update every time when access token is refreshed if expired is set
			hps.accessConf = accessConf
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
				return fmt.Errorf("refresh token url is required")
			}
			hps.refreshConf = refreshConf
		}
	}

	tr := &http.Transport{
		TLSClientConfig: tlscfg,
	}

	hps.client = &http.Client{
		Transport: tr,
		Timeout:   time.Duration(c.Timeout) * time.Millisecond,
	}
	hps.config = c

	// try to get access token
	if hps.accessConf != nil {
		conf.Log.Infof("Try to get access token from %s", hps.accessConf.Url)
		ctx := mock.NewMockContext("none", "httppull_init")
		hps.tokens = make(map[string]interface{})
		err := hps.auth(ctx)
		if err != nil {
			return fmt.Errorf("fail to authorize by oAuth: %v", err)
		}
	}

	return nil
}

func (hps *HTTPPullSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	ctx.GetLogger().Infof("Opening HTTP pull source with conf %+v", hps.config)
	// trigger refresh token timer
	if hps.accessConf != nil && hps.accessConf.ExpireInSecond > 0 {
		go infra.SafeRun(func() error {
			ctx.GetLogger().Infof("Starting refresh token for %d seconds", hps.accessConf.ExpireInSecond/2)
			ticker := time.NewTicker(time.Duration(hps.accessConf.ExpireInSecond/2) * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					ctx.GetLogger().Debugf("Refreshing token")
					hps.refresh(ctx)
				case <-ctx.Done():
					ctx.GetLogger().Infof("Closing refresh token timer")
					return nil
				}
			}
		})
	}
	hps.initTimerPull(ctx, consumer, errCh)
}

func (hps *HTTPPullSource) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing HTTP pull source")
	return nil
}

// initialize the oAuth access token
func (hps *HTTPPullSource) auth(ctx api.StreamContext) error {
	if resp, e := httpx.Send(conf.Log, hps.client, "json", http.MethodPost, hps.accessConf.Url, nil, true, []byte(hps.accessConf.Body)); e == nil {
		conf.Log.Infof("try to get access token got response %v", resp)
		hps.tokens, e = parseResponse(ctx, resp, hps.config.ResponseType, false, nil)
		if e != nil {
			return fmt.Errorf("Cannot parse access token response to json: %v", e)
		}
		ctx.GetLogger().Infof("Got access token %v", hps.tokens)
		expireIn, err := ctx.ParseTemplate(hps.accessConf.Expire, hps.tokens)
		if err != nil {
			return fmt.Errorf("fail to parse the expire time for access token: %v", err)
		}
		hps.accessConf.ExpireInSecond, err = cast.ToInt(expireIn, cast.CONVERT_ALL)
		if err != nil {
			return fmt.Errorf("fail to covert the expire time %s for access token: %v", expireIn, err)
		}
		if hps.refreshConf != nil {
			err := hps.refresh(ctx)
			if err != nil {
				return err
			}
		}
	} else {
		return fmt.Errorf("fail to get access token: %v", e)
	}
	return nil
}

func (hps *HTTPPullSource) refresh(ctx api.StreamContext) error {
	if hps.refreshConf != nil {
		headers := make(map[string]string, len(hps.refreshConf.Headers))
		var err error
		for k, v := range hps.refreshConf.Headers {
			headers[k], err = ctx.ParseTemplate(v, hps.tokens)
			if err != nil {
				return fmt.Errorf("fail to parse the header for refresh token request %s: %v", k, err)
			}
		}
		rr, ee := httpx.Send(conf.Log, hps.client, "json", http.MethodPost, hps.refreshConf.Url, headers, true, []byte(hps.accessConf.Body))
		if ee != nil {
			return fmt.Errorf("fail to get refresh token: %v", ee)
		}
		hps.tokens, err = parseResponse(ctx, rr, hps.config.ResponseType, false, nil)
		if err != nil {
			return fmt.Errorf("Cannot parse refresh token response to json: %v", err)
		}
		return nil
	} else if hps.accessConf != nil {
		return hps.auth(ctx)
	} else {
		return fmt.Errorf("no oAuth config")
	}
}

func (hps *HTTPPullSource) initTimerPull(ctx api.StreamContext, consumer chan<- api.SourceTuple, _ chan<- error) {
	logger := ctx.GetLogger()
	logger.Infof("Starting HTTP pull source with interval %d", hps.config.Interval)
	ticker := time.NewTicker(time.Millisecond * time.Duration(hps.config.Interval))
	defer ticker.Stop()
	var omd5 = ""
	headers := make(map[string]string, len(hps.config.Headers))
	var err error
	for {
		select {
		case <-ticker.C:
			for k, v := range hps.config.Headers {
				headers[k], err = ctx.ParseTemplate(v, hps.tokens)
				if err != nil {
					logger.Errorf("fail to parse the header for refresh token request %s: %v", k, err)
					break
				}
			}
			if err != nil {
				continue
			}
			ctx.GetLogger().Debugf("rest sink sending request url: %s, headers: %v, body %s", hps.config.Url, headers, hps.config.Body)
			if resp, e := httpx.Send(logger, hps.client, hps.config.BodyType, hps.config.Method, hps.config.Url, headers, true, []byte(hps.config.Body)); e != nil {
				logger.Warnf("Found error %s when trying to reach %v ", e, hps)
			} else {
				logger.Debugf("rest sink got response %v", resp)
				result, e := parseResponse(ctx, resp, hps.config.ResponseType, hps.config.Incremental, &omd5)
				if e != nil {
					logger.Errorf("Parse response error %v", e)
					continue
				}
				if result == nil {
					logger.Debugf("no data to send for incremental")
					continue
				}
				meta := make(map[string]interface{})
				select {
				case consumer <- api.NewDefaultSourceTuple(result, meta):
					logger.Debugf("send data to device node")
				case <-ctx.Done():
					return
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func getMD5Hash(text []byte) string {
	hash := md5.Sum(text)
	return hex.EncodeToString(hash[:])
}

func parseResponse(ctx api.StreamContext, resp *http.Response, responseType string, isIncremental bool, omd5 *string) (map[string]interface{}, error) {
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("http return code error: %d", resp.StatusCode)
	}
	c, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if isIncremental {
		nmd5 := getMD5Hash(c)
		if *omd5 == nmd5 {
			ctx.GetLogger().Debugf("Content has not changed since last fetch, so skip processing.")
			return nil, nil
		} else {
			*omd5 = nmd5
		}
	}
	switch responseType {
	case "code":
		return decode(ctx, c)
	case "body":
		payload, err := decode(ctx, c)
		if err != nil {
			return nil, err
		}
		ro := &bodyResp{}
		err = cast.MapToStruct(payload, ro)
		if err != nil {
			return nil, fmt.Errorf("invalid body response: %v", err)
		}
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return nil, fmt.Errorf("http status code is not 200: %v", payload)
		}
		return payload, nil
	default:
		return nil, fmt.Errorf("unsupported response type: %s", responseType)
	}
}

// TODO remove this function after all the sources are migrated to use the new API
func decode(ctx api.StreamContext, data []byte) (map[string]interface{}, error) {
	r, err := ctx.Decode(data)
	if err == nil {
		return r, nil
	}
	r = make(map[string]interface{})
	err = json.Unmarshal(data, &r)
	return r, nil
}

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

package source

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/pkg/cert"
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
	Interval           int               `json:"interval"`
	Timeout            int               `json:"timeout"`
	Incremental        bool              `json:"incremental"`
	Body               string            `json:"body"`
	BodyType           string            `json:"bodyType"`
	Headers            map[string]string `json:"headers"`
	InsecureSkipVerify bool              `json:"insecureSkipVerify"`
	CertificationPath  string            `json:"certificationPath"`
	PrivateKeyPath     string            `json:"privateKeyPath"`
	RootCaPath         string            `json:"rootCaPath"`
}

type HTTPPullSource struct {
	config *HTTPPullConf
	client *http.Client
}

var bodyTypeMap = map[string]string{"none": "", "text": "text/plain", "json": "application/json", "html": "text/html", "xml": "application/xml", "javascript": "application/javascript", "form": ""}

func (hps *HTTPPullSource) Configure(device string, props map[string]interface{}) error {
	conf.Log.Infof("Initialized Httppull source with configurations %#v.", props)
	c := &HTTPPullConf{
		Url:                "http://localhost",
		Method:             http.MethodGet,
		Interval:           DefaultInterval,
		Timeout:            DefaultTimeout,
		Body:               "json",
		InsecureSkipVerify: true,
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

	tr := &http.Transport{
		TLSClientConfig: tlscfg,
	}

	hps.client = &http.Client{
		Transport: tr,
		Timeout:   time.Duration(c.Timeout) * time.Millisecond,
	}
	hps.config = c
	return nil
}

func (hps *HTTPPullSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	hps.initTimerPull(ctx, consumer, errCh)
}

func (hps *HTTPPullSource) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing HTTP pull source")
	return nil
}

func (hps *HTTPPullSource) initTimerPull(ctx api.StreamContext, consumer chan<- api.SourceTuple, _ chan<- error) {
	ticker := time.NewTicker(time.Millisecond * time.Duration(hps.config.Interval))
	logger := ctx.GetLogger()
	defer ticker.Stop()
	var omd5 = ""
	for {
		select {
		case <-ticker.C:
			if resp, e := httpx.Send(logger, hps.client, hps.config.BodyType, hps.config.Method, hps.config.Url, hps.config.Headers, true, []byte(hps.config.Body)); e != nil {
				logger.Warnf("Found error %s when trying to reach %v ", e, hps)
			} else {
				logger.Debugf("rest sink got response %v", resp)
				if resp.StatusCode < 200 || resp.StatusCode > 299 {
					logger.Warnf("Found error http return code: %d when trying to reach %v ", resp.StatusCode, hps)
					break
				}
				c, err := io.ReadAll(resp.Body)
				if err != nil {
					logger.Warnf("Found error %s when trying to reach %v ", err, hps)
				}
				resp.Body.Close()
				if hps.config.Incremental {
					nmd5 := getMD5Hash(c)
					if omd5 == nmd5 {
						logger.Debugf("Content has not changed since last fetch, so skip processing.")
						continue
					} else {
						omd5 = nmd5
					}
				}

				result, e := ctx.Decode(c)
				meta := make(map[string]interface{})
				if e != nil {
					logger.Errorf("Invalid data format, cannot decode %s with error %s", string(c), e)
					return
				}

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

// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const DEFAULT_INTERVAL = 10000
const DEFAULT_TIMEOUT = 5000

type HTTPPullSource struct {
	url                string
	method             string
	interval           int
	timeout            int
	incremental        bool
	body               string
	bodyType           string
	headers            map[string]string
	messageFormat      string
	insecureSkipVerify bool

	client *http.Client
}

var bodyTypeMap = map[string]string{"none": "", "text": "text/plain", "json": "application/json", "html": "text/html", "xml": "application/xml", "javascript": "application/javascript", "form": ""}

func (hps *HTTPPullSource) Configure(device string, props map[string]interface{}) error {
	hps.url = "http://localhost" + device
	if u, ok := props["url"]; ok {
		if p, ok := u.(string); ok {
			hps.url = p + device
		}
	}

	hps.method = http.MethodGet
	if m, ok := props["method"]; ok {
		if m1, ok1 := m.(string); ok1 {
			switch strings.ToUpper(m1) {
			case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete:
				hps.method = strings.ToUpper(m1)
			default:
				return fmt.Errorf("Not supported HTTP method %s.", m1)
			}
		}
	}

	hps.interval = DEFAULT_INTERVAL
	if i, ok := props["interval"]; ok {
		i1, err := cast.ToInt(i, cast.CONVERT_SAMEKIND)
		if err != nil {
			return fmt.Errorf("Not valid interval value %v.", i1)
		} else {
			hps.interval = i1
		}
	}

	hps.timeout = DEFAULT_TIMEOUT
	if i, ok := props["timeout"]; ok {
		if i1, ok1 := i.(int); ok1 {
			hps.timeout = i1
		} else {
			return fmt.Errorf("Not valid timeout value %v.", i1)
		}
	}

	hps.incremental = false
	if i, ok := props["incremental"]; ok {
		if i1, ok1 := i.(bool); ok1 {
			hps.incremental = i1
		} else {
			return fmt.Errorf("Not valid incremental value %v.", i1)
		}
	}

	hps.bodyType = "json"
	if c, ok := props["bodyType"]; ok {
		if c1, ok1 := c.(string); ok1 {
			if _, ok2 := bodyTypeMap[strings.ToLower(c1)]; ok2 {
				hps.bodyType = strings.ToLower(c1)
			} else {
				return fmt.Errorf("Not valid body type value %v.", c)
			}
		} else {
			return fmt.Errorf("Not valid body type value %v.", c)
		}
	}

	hps.messageFormat = message.FormatJson
	if c, ok := props["format"]; ok {
		if c1, ok1 := c.(string); ok1 {
			hps.messageFormat = c1
		} else {
			return fmt.Errorf("Not valid format value %v.", c)
		}
	}

	if b, ok := props["body"]; ok {
		if b1, ok1 := b.(string); ok1 {
			hps.body = b1
		} else {
			return fmt.Errorf("Not valid incremental value %v, expect string.", b1)
		}
	}

	hps.insecureSkipVerify = true
	if i, ok := props["insecureSkipVerify"]; ok {
		if i1, ok1 := i.(bool); ok1 {
			hps.insecureSkipVerify = i1
		} else {
			return fmt.Errorf("Not valid insecureSkipVerify value %v.", i1)
		}
	}
	hps.headers = make(map[string]string)
	if h, ok := props["headers"]; ok {
		if h1, ok1 := h.(map[string]interface{}); ok1 {
			for k, v := range h1 {
				if v1, err := cast.ToString(v, cast.CONVERT_ALL); err == nil {
					hps.headers[k] = v1
				}
			}
		} else {
			return fmt.Errorf("Not valid header value %v.", h1)
		}
	}

	conf.Log.Debugf("Initialized with configurations %#v.", hps)
	return nil
}

func (hps *HTTPPullSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	_, e := url.Parse(hps.url)
	if e != nil {
		errCh <- e
		return
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: hps.insecureSkipVerify},
	}

	hps.client = &http.Client{
		Transport: tr,
		Timeout:   time.Duration(hps.timeout) * time.Millisecond}
	hps.initTimerPull(ctx, consumer, errCh)
}

func (hps *HTTPPullSource) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing HTTP pull source")
	return nil
}

func (hps *HTTPPullSource) initTimerPull(ctx api.StreamContext, consumer chan<- api.SourceTuple, _ chan<- error) {
	ticker := time.NewTicker(time.Millisecond * time.Duration(hps.interval))
	logger := ctx.GetLogger()
	logger.Infof("################*******************%v ", hps)
	defer ticker.Stop()
	var omd5 = ""
	for {
		select {
		case <-ticker.C:
			if resp, e := httpx.Send(logger, hps.client, hps.bodyType, hps.method, hps.url, hps.headers, true, []byte(hps.body)); e != nil {
				logger.Warnf("Found error %s when trying to reach %v ", e, hps)
			} else {
				logger.Debugf("rest sink got response %v", resp)
				if resp.StatusCode < 200 || resp.StatusCode > 299 {
					logger.Warnf("Found error http return code: %d when trying to reach %v ", resp.StatusCode, hps)
					break
				}
				c, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					logger.Warnf("Found error %s when trying to reach %v ", err, hps)
				}
				resp.Body.Close()
				if hps.incremental {
					nmd5 := getMD5Hash(c)
					if omd5 == nmd5 {
						logger.Debugf("Content has not changed since last fetch, so skip processing.")
						continue
					} else {
						omd5 = nmd5
					}
				}

				result, e := message.Decode(c, hps.messageFormat)
				meta := make(map[string]interface{})
				if e != nil {
					logger.Errorf("Invalid data format, cannot decode %s to %s format with error %s", string(c), hps.messageFormat, e)
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

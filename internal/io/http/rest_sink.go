// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

type RestSink struct {
	ClientConf
}

func (ms *RestSink) Validate(props map[string]interface{}) error {
	conf.Log.Infof("valiadte rest sink with configurations %#v.", props)
	return ms.InitConf("", props)
}

func (ms *RestSink) Configure(ps map[string]interface{}) error {
	conf.Log.Infof("Initialized rest sink with configurations %#v.", ps)
	return ms.Validate(ps)
}

func (ms *RestSink) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Opening REST sink with conf %+v", ms.config)
	return nil
}

func (ms *RestSink) collectWithUrl(ctx api.StreamContext, item interface{}, desUrl string) error {
	logger := ctx.GetLogger()
	decodedData, _, err := ctx.TransformOutput(item)
	if err != nil {
		logger.Warnf("rest sink decode data error: %v", err)
		return fmt.Errorf("rest sink decode data error: %v", err)
	}

	resp, err := ms.sendWithUrl(ctx, decodedData, item, desUrl)
	failpoint.Inject("injectRestTemporaryError", func(val failpoint.Value) {
		if val.(bool) {
			err = &url.Error{Err: &errorx.MockTemporaryError{}}
		}
	})
	if err != nil {
		originErr := err
		recoverAble := isRecoverAbleError(originErr)
		if recoverAble {
			logger.Errorf("rest sink meet error:%v, recoverAble:%v, ruleID:%v", originErr.Error(), recoverAble, ctx.GetRuleId())
			return errorx.NewIOErr(fmt.Sprintf(`rest sink fails to send out the data:err=%s recoverAble=%v method=%s path="%s" request_body="%s"`,
				originErr.Error(),
				recoverAble,
				ms.config.Method,
				ms.config.Url,
				decodedData))
		}
		return fmt.Errorf(`rest sink fails to send out the data:err=%s recoverAble=%v method=%s path="%s" request_body="%s"`,
			originErr.Error(),
			recoverAble,
			ms.config.Method,
			ms.config.Url,
			decodedData,
		)
	} else {
		logger.Debugf("rest sink got response %v", resp)
		_, b, err := ms.parseResponse(ctx, resp, ms.config.DebugResp, nil, false)
		// do not record response body error as it is not an error in the sink action.
		if err != nil && !strings.HasPrefix(err.Error(), BODY_ERR) {
			if strings.HasPrefix(err.Error(), BODY_ERR) {
				logger.Warnf("rest sink response body error: %v", err)
			} else {
				return fmt.Errorf(`parse response error: %s. | method=%s path="%s" status=%d response_body="%s"`,
					err,
					ms.config.Method,
					ms.config.Url,
					resp.StatusCode,
					b,
				)
			}
		}
		if ms.config.DebugResp {
			logger.Infof("Response raw content: %s\n", string(b))
		}
	}
	return nil
}

func isRecoverAbleError(err error) bool {
	return errorx.IsRestRecoverAbleError(err)
}

func (ms *RestSink) Collect(ctx api.StreamContext, item interface{}) error {
	ctx.GetLogger().Debugf("rest sink receive %s", item)
	return ms.collectWithUrl(ctx, item, ms.config.Url)
}

func (ms *RestSink) CollectResend(ctx api.StreamContext, item interface{}) error {
	ctx.GetLogger().Debugf("rest sink resend %s", item)
	return ms.collectWithUrl(ctx, item, ms.config.ResendUrl)
}

func (ms *RestSink) sendWithUrl(ctx api.StreamContext, decodedData []byte, v interface{}, desUrl string) (*http.Response, error) {
	// Allow to use tokens in headers and check oAuth token expiration
	if ms.accessConf != nil && ms.accessConf.ExpireInSecond > 0 &&
		// S1012 static check fix: We should use time.Since to replace time.Now().Sub()
		int(time.Since(ms.tokenLastUpdateAt).Abs().Seconds()) >= ms.accessConf.ExpireInSecond {
		ctx.GetLogger().Debugf("Refreshing token for REST sink")
		if err := ms.refresh(ctx); err != nil {
			ctx.GetLogger().Warnf("Refresh REST sink token error: %v", err)
		}
	}
	if ms.tokens != nil {
		switch dt := v.(type) {
		case map[string]interface{}:
			for k, vv := range ms.tokens {
				dt[k] = vv
			}
		case []map[string]interface{}:
			for m := range dt {
				for k, vv := range ms.tokens {
					dt[m][k] = vv
				}
			}
		}
	}
	bodyType, err := ctx.ParseTemplate(ms.config.BodyType, v)
	if err != nil {
		return nil, err
	}
	method, err := ctx.ParseTemplate(ms.config.Method, v)
	if err != nil {
		return nil, err
	}
	u, err := ctx.ParseTemplate(desUrl, v)
	if err != nil {
		return nil, err
	}
	headers, err := ms.parseHeaders(ctx, v)
	if err != nil {
		return nil, fmt.Errorf("rest sink headers template decode error: %v", err)
	}

	return httpx.Send(ctx.GetLogger(), ms.client, u, method,
		httpx.WithHeadersMap(headers),
		httpx.WithBody(decodedData, bodyType, ms.config.SendSingle, ms.compressor, ms.config.Compression))
}

func (ms *RestSink) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing rest sink")
	return nil
}

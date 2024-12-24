// Copyright 2024 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

type RestSink struct {
	*ClientConf
	noHeaderTemplate bool
}

var bodyTypeFormat = map[string]string{
	"json": "json",
	"form": "urlencoded",
}

func (r *RestSink) Provision(ctx api.StreamContext, configs map[string]any) error {
	r.ClientConf = &ClientConf{}
	err := r.InitConf("", configs)
	if err != nil {
		return err
	}
	if r.ClientConf.config.Format == "" {
		r.ClientConf.config.Format = "json"
	}
	if rf, ok := bodyTypeFormat[r.ClientConf.config.BodyType]; ok && r.ClientConf.config.Format != rf {
		return fmt.Errorf("format must be %s if bodyType is %s", rf, r.ClientConf.config.BodyType)
	}
	return nil
}

func (r *RestSink) Close(ctx api.StreamContext) error {
	return nil
}

func (r *RestSink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	sch(api.ConnectionConnected, "")
	return nil
}

func (r *RestSink) Collect(ctx api.StreamContext, item api.RawTuple) error {
	logger := ctx.GetLogger()
	headers := r.config.Headers
	bodyType := r.config.BodyType
	method := r.config.Method
	u := r.config.Url

	if dp, ok := item.(api.HasDynamicProps); ok {
		if !r.noHeaderTemplate {
			r.noHeaderTemplate = true
			for k, v := range r.config.Headers {
				headers = make(map[string]string, len(r.config.Headers))
				nv, ok := dp.DynamicProps(v)
				if ok {
					headers[k] = nv
					r.noHeaderTemplate = false
				} else {
					headers[k] = v
				}
			}
		}
		nb, ok := dp.DynamicProps(bodyType)
		if ok {
			bodyType = nb
		}
		nm, ok := dp.DynamicProps(method)
		if ok {
			method = nm
		}
		nu, ok := dp.DynamicProps(u)
		if ok {
			u = nu
		}
	}

	switch r.config.Compression {
	case "zstd":
		if headers == nil {
			headers = make(map[string]string)
		}
		headers["Content-Encoding"] = "zstd"
	case "gzip":
		if headers == nil {
			headers = make(map[string]string)
		}
		headers["Content-Encoding"] = "gzip"
	}

	resp, err := httpx.Send(ctx.GetLogger(), r.client, bodyType, method, u, headers, item.Raw())
	failpoint.Inject("recoverAbleErr", func() {
		err = errors.New("connection reset by peer")
	})
	if err != nil {
		originErr := err
		recoverAble := errorx.IsRecoverAbleError(originErr)
		if recoverAble {
			logger.Errorf("rest sink meet error:%v, recoverAble:%v, ruleID:%v", originErr.Error(), recoverAble, ctx.GetRuleId())
			return errorx.NewIOErr(fmt.Sprintf(`rest sink fails to send out the data:err=%s recoverAble=%v method=%s path="%s" request_body="%s"`,
				originErr.Error(),
				recoverAble,
				method,
				u, string(item.Raw())))
		}
		return fmt.Errorf(`rest sink fails to send out the data:err=%s recoverAble=%v method=%s path="%s" request_body="%s"`,
			originErr.Error(),
			recoverAble,
			method, u, string(item.Raw()))
	} else {
		logger.Debugf("rest sink got response %v", resp)
		_, b, err := r.parseResponse(ctx, resp, "", r.config.DebugResp, false)
		// do not record response body error as it is not an error in the sink action.
		if err != nil && !strings.HasPrefix(err.Error(), BODY_ERR) {
			if strings.HasPrefix(err.Error(), BODY_ERR) {
				logger.Warnf("rest sink response body error: %v", err)
			} else {
				return fmt.Errorf(`parse response error: %s. | method=%s path="%s" status=%d response_body="%s"`,
					err,
					method,
					u,
					resp.StatusCode,
					b,
				)
			}
		}
		if r.config.DebugResp {
			logger.Infof("Response raw content: %s\n", b)
		}
	}
	return nil
}

func GetSink() api.Sink {
	return &RestSink{}
}

var _ api.BytesCollector = &RestSink{}

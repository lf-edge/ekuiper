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

	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

type RestSink struct {
	*ClientConf
}

func (r *RestSink) Provision(ctx api.StreamContext, configs map[string]any) error {
	r.ClientConf = &ClientConf{}
	return r.InitConf("", configs)
}

func (r *RestSink) Close(ctx api.StreamContext) error {
	return nil
}

func (r *RestSink) Connect(ctx api.StreamContext) error {
	return nil
}

func (r *RestSink) Collect(ctx api.StreamContext, item api.MessageTuple) error {
	return r.collect(ctx, item.ToMap())
}

func (r *RestSink) CollectList(ctx api.StreamContext, items api.MessageTupleList) error {
	var err error
	items.RangeOfTuples(func(_ int, tuple api.MessageTuple) bool {
		err = r.collect(ctx, tuple.ToMap())
		if err != nil {
			return false
		}
		return true
	})
	return err
}

func (r *RestSink) collect(ctx api.StreamContext, data map[string]any) error {
	logger := ctx.GetLogger()
	headers, err := r.parseHeaders(ctx, data)
	if err != nil {
		return err
	}
	bodyType, err := ctx.ParseTemplate(r.config.BodyType, data)
	if err != nil {
		return err
	}
	method, err := ctx.ParseTemplate(r.config.Method, data)
	if err != nil {
		return err
	}
	u, err := ctx.ParseTemplate(r.config.Url, data)
	if err != nil {
		return err
	}
	resp, err := httpx.Send(ctx.GetLogger(), r.client, bodyType, method, u, headers, true, data)
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
				u, data))
		}
		return fmt.Errorf(`rest sink fails to send out the data:err=%s recoverAble=%v method=%s path="%s" request_body="%s"`,
			originErr.Error(),
			recoverAble,
			method, u, data)
	} else {
		logger.Debugf("rest sink got response %v", resp)
		_, b, err := r.parseResponse(ctx, resp)
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
			logger.Infof("Response raw content: %s\n", string(b))
		}
	}
	return nil
}

func GetSink() api.Sink {
	return &RestSink{}
}

var _ api.TupleCollector = &RestSink{}

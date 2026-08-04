// Copyright 2024-2025 EMQ Technologies Co., Ltd.
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
	"regexp"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
)

type RestSink struct {
	*ClientConf
	noFormdataTemplate bool
	headerTemplates    map[string]restHeaderTemplate
	hasHeaderTemplates bool
	hasDynamicHeaders  bool
}

type restHeaderTemplate struct {
	value        string
	plannerValue string
	kind         restHeaderKind
}

type restHeaderKind uint8

const (
	restHeaderStatic restHeaderKind = iota
	restHeaderOAuth
	restHeaderSQL
)

var (
	oauthTemplatePattern       = regexp.MustCompile(`{{\s*\.(access_token|refresh_token|token_type|id_token|expires_in)\s*}}`)
	oauthFieldReferencePattern = regexp.MustCompile(`{{[^{}]*\b(access_token|refresh_token|token_type|id_token|expires_in)\b[^{}]*}}`)
)

const oauthTemplateMarkerPrefix = "__ekuiper_oauth_"

var bodyTypeFormat = map[string]string{
	"json": "json",
	"form": "urlencoded",
}

func (r *RestSink) Provision(ctx api.StreamContext, configs map[string]any) error {
	r.ClientConf = &ClientConf{}
	err := r.InitConf(ctx, "", configs)
	if err != nil {
		return err
	}
	if r.ClientConf.config.Format == "" {
		r.ClientConf.config.Format = "json"
	}
	if rf, ok := bodyTypeFormat[r.ClientConf.config.BodyType]; ok && r.ClientConf.config.Format != rf {
		return fmt.Errorf("format must be %s if bodyType is %s", rf, r.ClientConf.config.BodyType)
	}
	r.headerTemplates = make(map[string]restHeaderTemplate, len(r.config.Headers))
	if r.accessConf != nil {
		r.tokenHeaderTemplates = make(map[string]string, len(r.config.Headers))
		r.requiredTokenFields = make(map[string]struct{})
	}
	for k, v := range r.config.Headers {
		if r.accessConf != nil {
			h, fields, err := newOAuthHeaderTemplate(k, v)
			if err != nil {
				return err
			}
			r.headerTemplates[k] = h
			if h.kind != restHeaderSQL {
				r.tokenHeaderTemplates[k] = v
			}
			for _, field := range fields {
				r.requiredTokenFields[field] = struct{}{}
			}
		} else {
			kind := restHeaderStatic
			if strings.Contains(v, "{{") {
				kind = restHeaderSQL
			}
			h := restHeaderTemplate{value: v, plannerValue: v, kind: kind}
			r.headerTemplates[k] = h
		}
		if h := r.headerTemplates[k]; h.kind != restHeaderStatic {
			r.hasHeaderTemplates = true
			if h.kind == restHeaderSQL {
				r.hasDynamicHeaders = true
			}
		}
	}
	return nil
}

// Consume separates OAuth response fields from templates that are evaluated
// against rule output by the common sink transform operator.
func (r *RestSink) Consume(props map[string]any) {
	deletePropFold(props, "oauth")
	if r.accessConf != nil {
		deletePropFold(props, "body")
		for key, value := range props {
			if strings.EqualFold(key, "headers") {
				switch headers := value.(type) {
				case map[string]any:
					maskedHeaders := make(map[string]any, len(headers))
					for k := range headers {
						maskedHeaders[k] = r.headerTemplates[k].plannerValue
					}
					props[key] = maskedHeaders
				case map[string]string:
					maskedHeaders := make(map[string]string, len(headers))
					for k := range headers {
						maskedHeaders[k] = r.headerTemplates[k].plannerValue
					}
					props[key] = maskedHeaders
				}
			}
		}
	}
}

func newOAuthHeaderTemplate(name, value string) (restHeaderTemplate, []string, error) {
	matches := oauthTemplatePattern.FindAllStringSubmatch(value, -1)
	withoutOAuth := oauthTemplatePattern.ReplaceAllString(value, "")
	if oauthFieldReferencePattern.MatchString(withoutOAuth) {
		return restHeaderTemplate{}, nil, fmt.Errorf("header %q uses an unsupported OAuth template; only simple placeholders such as {{.access_token}} are supported", name)
	}
	hasOAuth := len(matches) > 0
	hasSQL := strings.Contains(withoutOAuth, "{{")
	if hasOAuth && hasSQL {
		return restHeaderTemplate{}, nil, fmt.Errorf("header %q cannot mix OAuth and SQL templates", name)
	}
	h := restHeaderTemplate{value: value, plannerValue: value, kind: restHeaderStatic}
	fields := make([]string, 0, len(matches))
	if hasOAuth {
		h.kind = restHeaderOAuth
		h.plannerValue = oauthTemplateMarkerPrefix + name
		for _, match := range matches {
			fields = append(fields, match[1])
		}
	} else if hasSQL {
		h.kind = restHeaderSQL
	}
	return h, fields, nil
}

func deletePropFold(props map[string]any, name string) {
	for key := range props {
		if strings.EqualFold(key, name) {
			delete(props, key)
		}
	}
}

func (r *RestSink) Close(ctx api.StreamContext) error {
	return nil
}

func (r *RestSink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	err := r.Conn(ctx)
	if err != nil {
		return err
	}
	sch(api.ConnectionConnected, "")
	return nil
}

func (r *RestSink) Collect(ctx api.StreamContext, item api.RawTuple) error {
	logger := ctx.GetLogger()
	bodyType := r.config.BodyType
	method := r.config.Method
	u := r.config.Url
	headers := r.prepareHeaders(item)
	formData := r.config.FormData

	dp, hasDynamicProps := item.(api.HasDynamicProps)
	if hasDynamicProps {
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
		if bodyType == "formdata" && !r.noFormdataTemplate {
			r.noFormdataTemplate = true
			formData = make(map[string]string, len(r.config.FormData))
			for k, v := range r.config.FormData {
				nv, ok := dp.DynamicProps(v)
				if ok {
					formData[k] = nv
					r.noFormdataTemplate = false
				} else {
					formData[k] = v
				}
			}
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

	resp, err := r.Send(ctx, bodyType, method, u, headers, formData, r.config.FileFieldName, item.Raw())
	failpoint.Inject("recoverAbleErr", func() {
		err = errors.New("connection reset by peer")
	})
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()
	if err != nil {
		originErr := err
		recoverAble := errorx.IsRecoverAbleError(originErr)
		if recoverAble {
			logger.Errorf("rest sink meet error:%v, recoverAble:%v, ruleID:%v", originErr.Error(), recoverAble, ctx.GetRuleId())
			return errorx.NewIOErr(fmt.Sprintf(`rest sink fails to send out the data:err=%s recoverAble=%v method=%s path="%s"`,
				originErr.Error(),
				recoverAble,
				method,
				u))
		}
		return fmt.Errorf(`rest sink fails to send out the data:err=%s recoverAble=%v method=%s path="%s"`,
			originErr.Error(),
			recoverAble,
			method, u)
	} else {
		logger.Debugf("rest sink got response %v", resp)
		_, b, err := r.parseResponse(ctx, resp, "", r.config.DebugResp, true)
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

func (r *RestSink) prepareHeaders(item api.RawTuple) map[string]string {
	if !r.hasHeaderTemplates {
		if r.config.Compression != "" {
			return cloneHeaders(r.config.Headers)
		}
		return r.config.Headers
	}
	oauthState := r.oauthRuntimeState()
	if r.accessConf != nil && !r.hasDynamicHeaders && oauthState != nil && r.config.Compression == "" {
		return oauthState.headers
	}
	headers := make(map[string]string, len(r.headerTemplates))
	dp, hasDynamicProps := item.(api.HasDynamicProps)
	for k, headerTemplate := range r.headerTemplates {
		if headerTemplate.kind != restHeaderSQL && oauthState != nil {
			if resolved, ok := oauthState.headers[k]; ok {
				headers[k] = resolved
				continue
			}
		}
		value := headerTemplate.value
		if headerTemplate.kind == restHeaderSQL && hasDynamicProps {
			if dynamicValue, ok := dp.DynamicProps(headerTemplate.value); ok {
				value = dynamicValue
			}
		}
		headers[k] = value
	}
	return headers
}

func cloneHeaders(headers map[string]string) map[string]string {
	result := make(map[string]string, len(headers))
	for k, v := range headers {
		result[k] = v
	}
	return result
}

func GetSink() api.Sink {
	return &RestSink{}
}

var _ api.BytesCollector = &RestSink{}

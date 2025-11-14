// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

package service

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	// TODO: replace with `google.golang.org/protobuf/proto` pkg.
	"github.com/golang/protobuf/proto"                  //nolint:staticcheck
	"github.com/jhump/protoreflect/dynamic"             //nolint:staticcheck
	"github.com/jhump/protoreflect/dynamic/grpcdynamic" //nolint:staticcheck
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
)

type exeIns func(desc descriptor, opt *interfaceOpt, i *interfaceInfo) (executor, error)

var executors = map[protocol]exeIns{
	GRPC: newGrpcExecutor,
	REST: newHttpExecutor,
}

func newHttpExecutor(desc descriptor, opt *interfaceOpt, i *interfaceInfo) (executor, error) {
	d, ok := desc.(multiplexDescriptor)
	if !ok {
		return nil, fmt.Errorf("invalid descriptor type for rest")
	}
	o := &restOption{}
	e := cast.MapToStruct(i.Options, o)
	if len(o.RetryInterval) > 0 {
		d, err := time.ParseDuration(o.RetryInterval)
		if err != nil {
			return nil, fmt.Errorf("incorrect rest option: %v", err)
		}
		o.retryIntervalDuration = d
	}
	if e != nil {
		return nil, fmt.Errorf("incorrect rest option: %v", e)
	}
	exe := &httpExecutor{
		descriptor:   d,
		interfaceOpt: opt,
		restOpt:      o,
	}
	return exe, nil
}

func newGrpcExecutor(desc descriptor, opt *interfaceOpt, _ *interfaceInfo) (executor, error) {
	d, ok := desc.(protoDescriptor)
	if !ok {
		return nil, fmt.Errorf("invalid descriptor type for grpc")
	}
	exe := &grpcExecutor{
		descriptor:   d,
		interfaceOpt: opt,
	}
	return exe, nil
}

// NewExecutor
// Each interface definition maps to one executor instance. It is supposed to have only one thread running.
func NewExecutor(i *interfaceInfo) (executor, error) {
	// No validation here, suppose the validation has been done in json parsing
	descriptor, err := parse(i.Schema.SchemaType, i.Schema.SchemaFile, i.Schema.Schemaless)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(i.Addr)
	if err != nil {
		return nil, fmt.Errorf("invalid url %s", i.Addr)
	}
	opt := &interfaceOpt{
		addr:    u,
		timeout: 5 * time.Second,
	}

	if ins, ok := executors[i.Protocol]; ok {
		return ins(descriptor, opt, i)
	} else {
		return nil, fmt.Errorf("unsupported protocol %s", i.Protocol)
	}
}

type executor interface {
	InvokeFunction(ctx api.FunctionContext, name string, params []interface{}) (interface{}, error)
}

type interfaceOpt struct {
	addr    *url.URL
	timeout time.Duration
}

type grpcExecutor struct {
	descriptor protoDescriptor
	*interfaceOpt

	conn *grpc.ClientConn
}

func (d *grpcExecutor) InvokeFunction(_ api.FunctionContext, name string, params []interface{}) (interface{}, error) {
	if d.conn == nil {
		dialCtx, cancel := context.WithTimeout(context.Background(), d.timeout)
		var (
			conn *grpc.ClientConn
			e    error
		)
		go infra.SafeRun(func() error {
			defer cancel()
			conn, e = grpc.DialContext(dialCtx, d.addr.Host, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock()) //nolint:staticcheck
			return e
		})

		<-dialCtx.Done()
		err := dialCtx.Err()
		switch err {
		case context.Canceled:
			// connect successfully, do nothing
		case context.DeadlineExceeded:
			return nil, fmt.Errorf("connect to %s timeout", d.addr.String())
		default:
			return nil, fmt.Errorf("connect to %s error: %v", d.addr.String(), err)
		}
		if e != nil {
			return nil, e
		}
		d.conn = conn
	}
	// TODO reconnect if fail and error handling

	stub := grpcdynamic.NewStubWithMessageFactory(d.conn, d.descriptor.MessageFactory())
	message, err := d.descriptor.ConvertParamsToMessage(name, params)
	if err != nil {
		return nil, err
	}
	timeoutCtx, cancel := context.WithTimeout(context.Background(), d.timeout)
	var (
		o proto.Message
		e error
	)
	go infra.SafeRun(func() error {
		defer cancel()
		o, e = stub.InvokeRpc(timeoutCtx, d.descriptor.MethodDescriptor(name), message)
		return e
	})

	<-timeoutCtx.Done()
	err = timeoutCtx.Err()
	switch err {
	case context.Canceled:
		// connect successfully, do nothing
	case context.DeadlineExceeded:
		return nil, fmt.Errorf("invoke %s timeout", name)
	default:
		return nil, fmt.Errorf("invoke %s error: %v", name, err)
	}
	if e != nil {
		return nil, fmt.Errorf("error invoking method %s in proto: %v", name, e)
	}
	odm, err := dynamic.AsDynamicMessage(o)
	if err != nil {
		return nil, fmt.Errorf("error parsing method %s result: %v", name, err)
	}
	return d.descriptor.ConvertReturnMessage(name, odm)
}

type httpExecutor struct {
	descriptor multiplexDescriptor
	*interfaceOpt
	restOpt *restOption

	conn *http.Client
}

var testIndex int

func (h *httpExecutor) InvokeFunction(ctx api.FunctionContext, name string, params []interface{}) (interface{}, error) {
	if h.restOpt.RetryCount < 1 {
		return h.invokeFunction(ctx, name, params)
	}
	var err error
	var result interface{}
	for i := 0; i < h.restOpt.RetryCount; i++ {
		if i > 0 {
			time.Sleep(h.restOpt.retryIntervalDuration)
		}
		result, err = h.invokeFunction(ctx, name, params)
		failpoint.Inject("httpExecutorRetry", func(val failpoint.Value) {
			if val.(bool) {
				if testIndex < 1 {
					err = &url.Error{Err: &errorx.MockTemporaryError{}}
					testIndex++
				}
			}
		})
		if err == nil {
			return result, nil
		}
		if !errorx.IsRecoverAbleError(err) {
			return nil, err
		}
	}
	return nil, err
}

func (h *httpExecutor) invokeFunction(ctx api.FunctionContext, name string, params []interface{}) (interface{}, error) {
	failpoint.Inject("httpExecutorRetry", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, nil)
		}
	})

	if h.conn == nil {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: h.restOpt.InsecureSkipVerify},
		}
		h.conn = &http.Client{
			Transport: tr,
			Timeout:   h.timeout,
		}
	}

	hm, err := h.descriptor.ConvertHttpMapping(name, params)
	if err != nil {
		return nil, err
	}
	u := h.addr.String() + hm.Uri
	_, err = url.Parse(u)
	if err != nil {
		return nil, err
	}
	resp, err := httpx.Send(ctx.GetLogger(), h.conn, "json", hm.Method, u, h.restOpt.Headers, hm.Body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		buf, _ := io.ReadAll(resp.Body)
		ctx.GetLogger().Debugf("%s\n", string(buf))
		return nil, fmt.Errorf("http executor fails to err http return code: %d and error message %s", resp.StatusCode, string(buf))
	} else {
		buf, bodyErr := io.ReadAll(resp.Body)
		if bodyErr != nil {
			return nil, fmt.Errorf("http executor read response body error: %v", bodyErr)
		}
		contentType := resp.Header.Get("Content-Type")
		if strings.HasPrefix(contentType, "application/json") {
			return h.descriptor.ConvertReturnJson(name, buf)
		} else if strings.HasPrefix(contentType, "text/plain") {
			return h.descriptor.ConvertReturnText(name, buf)
		} else {
			return nil, fmt.Errorf("unsupported resposne content type %s", contentType)
		}
	}
}

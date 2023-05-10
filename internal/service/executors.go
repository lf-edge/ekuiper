// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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
	"net"
	"net/http"
	"net/rpc"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"github.com/ugorji/go/codec"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/lf-edge/ekuiper/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/infra"
)

// NewExecutor
// Each interface definition maps to one executor instance. It is suppose to have only one thread running.
func NewExecutor(i *interfaceInfo) (executor, error) {
	// No validation here, suppose the validation has been done in json parsing
	descriptor, err := parse(i.Schema.SchemaType, i.Schema.SchemaFile)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(i.Addr)
	if err != nil {
		return nil, fmt.Errorf("invalid url %s", i.Addr)
	}
	opt := &interfaceOpt{
		addr:    u,
		timeout: 5000,
	}
	switch i.Protocol {
	case GRPC:
		d, ok := descriptor.(protoDescriptor)
		if !ok {
			return nil, fmt.Errorf("invalid descriptor type for grpc")
		}
		exe := &grpcExecutor{
			descriptor:   d,
			interfaceOpt: opt,
		}
		return exe, nil
	case REST:
		d, ok := descriptor.(multiplexDescriptor)
		if !ok {
			return nil, fmt.Errorf("invalid descriptor type for rest")
		}
		o := &restOption{}
		e := cast.MapToStruct(i.Options, o)
		if e != nil {
			return nil, fmt.Errorf("incorrect rest option: %v", e)
		}
		exe := &httpExecutor{
			descriptor:   d,
			interfaceOpt: opt,
			restOpt:      o,
		}
		return exe, nil
	case MSGPACK:
		d, ok := descriptor.(interfaceDescriptor)
		if !ok {
			return nil, fmt.Errorf("invalid descriptor type for msgpack-rpc")
		}
		exe := &msgpackExecutor{
			descriptor:   d,
			interfaceOpt: opt,
		}
		return exe, nil
	default:
		return nil, fmt.Errorf("unsupported protocol %s", i.Protocol)
	}
}

type executor interface {
	InvokeFunction(ctx api.FunctionContext, name string, params []interface{}) (interface{}, error)
}

type interfaceOpt struct {
	addr    *url.URL
	timeout int64
}

type grpcExecutor struct {
	descriptor protoDescriptor
	*interfaceOpt

	conn *grpc.ClientConn
}

func (d *grpcExecutor) InvokeFunction(_ api.FunctionContext, name string, params []interface{}) (interface{}, error) {
	if d.conn == nil {
		dialCtx, cancel := context.WithTimeout(context.Background(), time.Duration(d.timeout)*time.Millisecond)
		var (
			conn *grpc.ClientConn
			e    error
		)
		go infra.SafeRun(func() error {
			defer cancel()
			conn, e = grpc.DialContext(dialCtx, d.addr.Host, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
			return e
		})

		select {
		case <-dialCtx.Done():
			err := dialCtx.Err()
			switch err {
			case context.Canceled:
				// connect successfully, do nothing
			case context.DeadlineExceeded:
				return nil, fmt.Errorf("connect to %s timeout", d.addr.String())
			default:
				return nil, fmt.Errorf("connect to %s error: %v", d.addr.String(), err)
			}
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
	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Duration(d.timeout)*time.Millisecond)
	var (
		o proto.Message
		e error
	)
	go infra.SafeRun(func() error {
		defer cancel()
		o, e = stub.InvokeRpc(timeoutCtx, d.descriptor.MethodDescriptor(name), message)
		return e
	})

	select {
	case <-timeoutCtx.Done():
		err := timeoutCtx.Err()
		switch err {
		case context.Canceled:
			// connect successfully, do nothing
		case context.DeadlineExceeded:
			return nil, fmt.Errorf("invoke %s timeout", name)
		default:
			return nil, fmt.Errorf("invoke %s error: %v", name, err)
		}
	}
	if e != nil {
		return nil, fmt.Errorf("error invoking method %s in proto: %v", name, err)
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

func (h *httpExecutor) InvokeFunction(ctx api.FunctionContext, name string, params []interface{}) (interface{}, error) {
	if h.conn == nil {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: h.restOpt.InsecureSkipVerify},
		}
		h.conn = &http.Client{
			Transport: tr,
			Timeout:   time.Duration(h.timeout) * time.Millisecond,
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
	resp, err := httpx.Send(ctx.GetLogger(), h.conn, "json", hm.Method, u, h.restOpt.Headers, false, hm.Body)
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

type msgpackExecutor struct {
	descriptor interfaceDescriptor
	*interfaceOpt

	sync.Mutex
	connected bool
	conn      *rpc.Client
}

// InvokeFunction flat the params and result
func (m *msgpackExecutor) InvokeFunction(_ api.FunctionContext, name string, params []interface{}) (interface{}, error) {
	if !m.connected {
		m.Lock()
		if !m.connected {
			h := &codec.MsgpackHandle{}
			h.MapType = reflect.TypeOf(map[string]interface{}(nil))

			conn, err := net.Dial(m.addr.Scheme, m.addr.Host)
			if err != nil {
				return nil, err
			}
			rpcCodec := codec.MsgpackSpecRpc.ClientCodec(conn, h)
			m.conn = rpc.NewClientWithCodec(rpcCodec)
		}
		m.connected = true
		m.Unlock()
	}
	ps, err := m.descriptor.ConvertParams(name, params)
	if err != nil {
		return nil, err
	}
	var (
		reply interface{}
		args  interface{}
	)
	// TODO argument flat
	switch len(ps) {
	case 0:
		// do nothing
	case 1:
		args = ps[0]
	default:
		args = codec.MsgpackSpecRpcMultiArgs(ps)
	}
	err = m.conn.Call(name, args, &reply)
	if err != nil {
		if err == rpc.ErrShutdown {
			m.connected = false
		}
		return nil, err
	}
	return m.descriptor.ConvertReturn(name, reply)
}

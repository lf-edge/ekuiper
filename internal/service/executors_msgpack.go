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

//go:build msgpack

package service

import (
	"fmt"
	"github.com/ugorji/go/codec"
	"net"
	"net/rpc"
	"reflect"
	"sync"

	"github.com/lf-edge/ekuiper/pkg/api"
)

func init() {
	executors[MSGPACK] = func(desc descriptor, opt *interfaceOpt, _ *interfaceInfo) (executor, error) {
		d, ok := desc.(interfaceDescriptor)
		if !ok {
			return nil, fmt.Errorf("invalid descriptor type for msgpack-rpc")
		}
		exe := &msgpackExecutor{
			descriptor:   d,
			interfaceOpt: opt,
		}
		return exe, nil
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

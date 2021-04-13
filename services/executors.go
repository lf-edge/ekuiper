package services

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"github.com/ugorji/go/codec"
	"google.golang.org/grpc"
	"io/ioutil"
	"net"
	"net/http"
	"net/rpc"
	"net/url"
	"reflect"
	"strings"
	"time"
)

// NewExecutor
// Each interface definition maps to one executor instance. It is suppose to have only one thread running.
func NewExecutor(i *interfaceInfo) (executor, error) {
	// No validation here, suppose the validation has been done in json parsing
	descriptor, err := parse(i.Schema.SchemaType, i.Schema.SchemaFile)
	if err != nil {
		return nil, err
	}
	switch i.Protocol {
	case GRPC:
		d, ok := descriptor.(protoDescriptor)
		if !ok {
			return nil, fmt.Errorf("invalid descriptor type for grpc")
		}
		exe := &grpcExecutor{
			descriptor: d,
			timeout:    5000,
			addr:       i.Addr,
		}
		return exe, nil
	case REST:
		if _, err := url.Parse(i.Addr); err != nil {
			return nil, fmt.Errorf("invalid url %s", i.Addr)
		}
		d, ok := descriptor.(multiplexDescriptor)
		if !ok {
			return nil, fmt.Errorf("invalid descriptor type for rest")
		}
		exe := &httpExecutor{
			descriptor: d,
			url:        i.Addr,
			timeout:    5000,
			method:     http.MethodPost,
			bodyType:   "json",
		}
		return exe, nil
	case MSGPACK:
		d, ok := descriptor.(interfaceDescriptor)
		if !ok {
			return nil, fmt.Errorf("invalid descriptor type for msgpack-rpc")
		}
		exe := &msgpackExecutor{
			descriptor: d,
			timeout:    5000,
			addr:       i.Addr,
		}
		return exe, nil
	default:
		return nil, fmt.Errorf("unsupported protocol %s", i.Protocol)
	}
}

type executor interface {
	InvokeFunction(name string, params []interface{}) (interface{}, error)
}

type grpcExecutor struct {
	descriptor protoDescriptor
	addr       string
	timeout    int64

	conn *grpc.ClientConn
}

func (d *grpcExecutor) InvokeFunction(name string, params []interface{}) (interface{}, error) {
	if d.conn == nil {
		conn, err := grpc.Dial(d.addr, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			return nil, err
		}
		d.conn = conn
	}
	// TODO reconnect if fail and error handling

	stub := grpcdynamic.NewStubWithMessageFactory(d.conn, d.descriptor.MessageFactory())
	message, err := d.descriptor.ConvertParamsToMessage(name, params)
	if err != nil {
		return nil, err
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(d.timeout)*time.Millisecond)
	o, err := stub.InvokeRpc(ctx, d.descriptor.MethodDescriptor(name), message)
	if err != nil {
		return nil, fmt.Errorf("error invoking method %s in proto: %v", name, err)
	}
	odm, err := dynamic.AsDynamicMessage(o)
	if err != nil {
		return nil, fmt.Errorf("error parsing method %s result: %v", name, err)
	}
	return d.descriptor.ConvertReturnMessage(name, odm)
}

type httpExecutor struct {
	descriptor         multiplexDescriptor
	url                string
	method             string
	headers            map[string]string
	bodyType           string
	timeout            int64
	insecureSkipVerify bool

	conn *http.Client
}

func (h *httpExecutor) InvokeFunction(name string, params []interface{}) (interface{}, error) {
	if h.conn == nil {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: h.insecureSkipVerify},
		}
		h.conn = &http.Client{
			Transport: tr,
			Timeout:   time.Duration(h.timeout) * time.Millisecond}
	}

	json, err := h.descriptor.ConvertParamsToJson(name, params)
	if err != nil {
		return nil, err
	}
	resp, err := common.Send(common.Log, h.conn, h.bodyType, h.method, h.url+"/"+name, h.headers, false, json)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		buf, _ := ioutil.ReadAll(resp.Body)
		common.Log.Debugf("%s\n", string(buf))
		return nil, fmt.Errorf("http executor fails to err http return code: %d and error message %s", resp.StatusCode, string(buf))
	} else {
		buf, bodyErr := ioutil.ReadAll(resp.Body)
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
	addr       string
	timeout    int64

	conn *rpc.Client
}

// InvokeFunction flat the params and result
func (m *msgpackExecutor) InvokeFunction(name string, params []interface{}) (interface{}, error) {
	if m.conn == nil {
		h := &codec.MsgpackHandle{}
		h.MapType = reflect.TypeOf(map[string]interface{}(nil))
		conn, err := net.Dial("tcp", m.addr)
		if err != nil {
			return nil, err
		}
		rpcCodec := codec.MsgpackSpecRpc.ClientCodec(conn, h)
		m.conn = rpc.NewClientWithCodec(rpcCodec)
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
		return nil, err
	}
	return m.descriptor.ConvertReturn(name, reply)
}

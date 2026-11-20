// Copyright 2026 EMQ Technologies Co., Ltd.
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

package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/httpx"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/cert"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

const defaultTimeout = 5 * time.Second

type connConf struct {
	Server  string            `json:"server"`
	Timeout cast.DurationConf `json:"timeout"`
}

func getConnConf(props map[string]any) (*connConf, error) {
	c := &connConf{
		Timeout: cast.DurationConf(defaultTimeout),
	}
	if err := cast.MapToStruct(props, c); err != nil {
		return nil, fmt.Errorf("read properties fail with error: %v", err)
	}
	if c.Server == "" {
		return nil, errors.New("server is required")
	}
	if _, _, err := net.SplitHostPort(c.Server); err != nil {
		return nil, fmt.Errorf("server must be in the host:port format: %v", err)
	}
	if c.Timeout <= 0 {
		return nil, fmt.Errorf("timeout must be positive, got %s", time.Duration(c.Timeout))
	}
	return c, nil
}

type sinkConf struct {
	Method   string            `json:"method"`
	Metadata map[string]string `json:"metadata"`
	// The common sink properties below are read to make sure each row is encoded as a protobuf request message
	Format         string            `json:"format"`
	SendSingle     bool              `json:"sendSingle"`
	BatchSize      int               `json:"batchSize"`
	LingerInterval cast.DurationConf `json:"lingerInterval"`
}

func getSinkConf(props map[string]any) (*sinkConf, error) {
	c := &sinkConf{}
	if err := cast.MapToStruct(props, c); err != nil {
		return nil, fmt.Errorf("read properties fail with error: %v", err)
	}
	if c.Method == "" {
		return nil, errors.New("method is required")
	}
	// The encoded bytes are sent as the request message, so they must be encoded by a protobuf schema
	switch strings.ToLower(c.Format) {
	case message.FormatProtobuf, message.FormatCustom:
	default:
		return nil, fmt.Errorf("format must be %s or %s to encode the gRPC request, got %q", message.FormatProtobuf, message.FormatCustom, c.Format)
	}
	if !c.SendSingle {
		return nil, errors.New("sendSingle must be true to encode each row as a gRPC request")
	}
	if c.BatchSize > 0 || c.LingerInterval > 0 {
		return nil, errors.New("batchSize and lingerInterval are not supported because each row is sent as a gRPC request")
	}
	return c, nil
}

// fullMethod converts the method to the "/package.Service/Method" form used by gRPC.
func fullMethod(method string) string {
	if strings.HasPrefix(method, "/") {
		return method
	}
	return "/" + method
}

func (c *connConf) dialOptions(ctx api.StreamContext, props map[string]any) ([]grpc.DialOption, error) {
	tlsConfig, err := cert.GenTLSConfig(ctx, props)
	if err != nil {
		return nil, err
	}
	creds := insecure.NewCredentials()
	if tlsConfig != nil {
		creds = credentials.NewTLS(tlsConfig)
	}
	// Apply the same destination policy as the rest sink and the gRPC external service
	dialer := httpx.GetSSRFDialContext(time.Duration(c.Timeout))
	return []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return dialer(ctx, "tcp", addr)
		}),
	}, nil
}

// rawCodec sends the bytes encoded by the sink format as the request message and keeps the response undecoded.
// Its name is "proto", so the request has the standard application/grpc+proto content type.
type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("grpc sink codec expects []byte but got %T", v)
	}
	return b, nil
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("grpc sink codec expects *[]byte but got %T", v)
	}
	*b = append((*b)[:0], data...)
	return nil
}

func (rawCodec) Name() string {
	return "proto"
}

type sink struct {
	cc       *connConf
	sc       *sinkConf
	dialOpts []grpc.DialOption
	conn     *grpc.ClientConn
	cancel   context.CancelFunc
	done     chan struct{}
}

func (s *sink) Provision(ctx api.StreamContext, configs map[string]any) error {
	cc, err := getConnConf(configs)
	if err != nil {
		return err
	}
	sc, err := getSinkConf(configs)
	if err != nil {
		return err
	}
	opts, err := cc.dialOptions(ctx, configs)
	if err != nil {
		return err
	}
	s.cc = cc
	s.sc = sc
	s.dialOpts = opts
	return nil
}

// Connect creates the client and starts connecting. The client reconnects by itself and the connection status is
// reported whenever it changes, so a server that is not available yet does not fail the rule.
func (s *sink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	conn, err := grpc.NewClient(s.cc.Server, s.dialOpts...)
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		return fmt.Errorf("grpc sink fails to create the client for %s: %v", s.cc.Server, err)
	}
	conn.Connect()
	s.conn = conn
	watchCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.done = make(chan struct{})
	go s.watchState(ctx, watchCtx, sch)
	return nil
}

func (s *sink) watchState(ctx api.StreamContext, watchCtx context.Context, sch api.StatusChangeHandler) {
	defer close(s.done)
	for {
		state := s.conn.GetState()
		switch state {
		case connectivity.Ready:
			ctx.GetLogger().Infof("grpc sink connected to %s", s.cc.Server)
			sch(api.ConnectionConnected, "")
		case connectivity.Connecting:
			sch(api.ConnectionConnecting, "")
		case connectivity.TransientFailure:
			msg := fmt.Sprintf("grpc sink fails to connect to %s", s.cc.Server)
			ctx.GetLogger().Error(msg)
			sch(api.ConnectionDisconnected, msg)
		case connectivity.Shutdown:
			return
		}
		if !s.conn.WaitForStateChange(watchCtx, state) {
			return
		}
	}
}

func (s *sink) Collect(ctx api.StreamContext, item api.RawTuple) error {
	method := s.sc.Method
	md := s.sc.Metadata
	// If the props support dynamic props(template), planner will guarantee the result has the parsed dynamic props
	if dp, ok := item.(api.HasDynamicProps); ok {
		if v, transformed := dp.DynamicProps(method); transformed {
			method = v
		}
		if len(md) > 0 {
			newMd := make(map[string]string, len(md))
			for k, v := range md {
				if nv, transformed := dp.DynamicProps(v); transformed {
					v = nv
				}
				newMd[k] = v
			}
			md = newMd
		}
	}
	callCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cc.Timeout))
	defer cancel()
	if len(md) > 0 {
		callCtx = metadata.NewOutgoingContext(callCtx, metadata.New(md))
	}
	var resp []byte
	err := s.conn.Invoke(callCtx, fullMethod(method), item.Raw(), &resp, grpc.ForceCodec(rawCodec{}))
	if err != nil {
		e := fmt.Sprintf("grpc sink fails to call %s on %s: %v", method, s.cc.Server, err)
		if isRetryable(err) {
			return errorx.NewIOErr(e)
		}
		return errors.New(e)
	}
	ctx.GetLogger().Debugf("grpc sink called %s with %d bytes", method, len(item.Raw()))
	return nil
}

// isRetryable reports whether the call may succeed if it is sent again, so the error is reported as an IO error to
// make the cache and resend work. Errors such as an invalid argument or an unknown method are permanent.
func isRetryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
	default:
		return false
	}
}

func (s *sink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("closing grpc sink")
	if s.cancel != nil {
		s.cancel()
		<-s.done
		s.cancel = nil
	}
	if s.conn != nil {
		err := s.conn.Close()
		s.conn = nil
		return err
	}
	return nil
}

// Ping checks if the server can be connected in the timeout.
func (s *sink) Ping(ctx api.StreamContext, props map[string]any) error {
	cc, err := getConnConf(props)
	if err != nil {
		return err
	}
	opts, err := cc.dialOptions(ctx, props)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(cc.Server, opts...)
	if err != nil {
		return err
	}
	defer conn.Close()
	pingCtx, cancel := context.WithTimeout(ctx, time.Duration(cc.Timeout))
	defer cancel()
	conn.Connect()
	for {
		state := conn.GetState()
		switch state {
		case connectivity.Ready:
			return nil
		case connectivity.TransientFailure:
			return fmt.Errorf("fail to connect to %s", cc.Server)
		}
		if !conn.WaitForStateChange(pingCtx, state) {
			return fmt.Errorf("connect to %s timeout", cc.Server)
		}
	}
}

func GetSink() api.Sink {
	return &sink{}
}

var (
	_ api.BytesCollector = &sink{}
	_ util.PingableConn  = &sink{}
)

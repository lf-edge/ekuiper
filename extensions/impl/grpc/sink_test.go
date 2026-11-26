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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jhump/protoreflect/desc/protoparse" //nolint:staticcheck
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/converter/protobuf"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

const protoFile = "test/greeter.proto"

func init() {
	// The test server listens on the loopback address
	testx.InitEnv("grpc")
	conf.Config.Basic.EnablePrivateNet = true
}

type request struct {
	name  string
	count int64
	md    metadata.MD
}

// greeterServer is a gRPC server of the greeter.Greeter service defined in test/greeter.proto
type greeterServer struct {
	addr string

	mu       sync.Mutex
	requests []request
	// err is returned by the server if it is set
	err error
}

func startServer(t *testing.T, opts ...grpc.ServerOption) *greeterServer {
	fd, err := (&protoparse.Parser{}).ParseFiles(protoFile)
	require.NoError(t, err)
	file := fd[0].UnwrapFile()
	reqDesc := file.Messages().ByName("HelloRequest")
	replyDesc := file.Messages().ByName("HelloReply")

	gs := &greeterServer{}
	server := grpc.NewServer(opts...)
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "greeter.Greeter",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "SayHello",
				Handler: func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
					in := dynamicpb.NewMessage(reqDesc)
					if err := dec(in); err != nil {
						return nil, err
					}
					md, _ := metadata.FromIncomingContext(ctx)
					gs.mu.Lock()
					defer gs.mu.Unlock()
					if gs.err != nil {
						return nil, gs.err
					}
					gs.requests = append(gs.requests, request{
						name:  in.Get(reqDesc.Fields().ByName("name")).String(),
						count: in.Get(reqDesc.Fields().ByName("count")).Int(),
						md:    md,
					})
					reply := dynamicpb.NewMessage(replyDesc)
					reply.Set(replyDesc.Fields().ByName("message"), protoreflect.ValueOfString("hello"))
					return reply, nil
				},
			},
		},
	}, struct{}{})
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	gs.addr = l.Addr().String()
	go func() {
		_ = server.Serve(l)
	}()
	t.Cleanup(server.Stop)
	return gs
}

func (gs *greeterServer) setErr(err error) {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.err = err
}

func (gs *greeterServer) received() []request {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	return append([]request(nil), gs.requests...)
}

// closedServer returns an address that refuses connections
func closedServer(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

type statusRecorder struct {
	mu       sync.Mutex
	statuses []string
}

func (r *statusRecorder) handle(status string, _ string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statuses = append(r.statuses, status)
}

func (r *statusRecorder) last() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.statuses) == 0 {
		return ""
	}
	return r.statuses[len(r.statuses)-1]
}

// encode encodes the data with the protobuf format, as the sink encode operator does
func encode(t *testing.T, data map[string]any) []byte {
	c, err := protobuf.NewConverter(protoFile, "", "greeter.HelloRequest")
	require.NoError(t, err)
	b, err := c.Encode(mockContext.NewMockContext("rule", "op"), data)
	require.NoError(t, err)
	return b
}

func TestProvision(t *testing.T) {
	ctx := mockContext.NewMockContext("rule", "op")
	tests := []struct {
		name  string
		props map[string]any
		cc    *connConf
		sc    *sinkConf
		err   string
	}{
		{
			name:  "missing server",
			props: map[string]any{"method": "greeter.Greeter/SayHello", "format": "protobuf", "sendSingle": true},
			err:   "server is required",
		},
		{
			name:  "invalid server",
			props: map[string]any{"server": "localhost", "method": "greeter.Greeter/SayHello", "format": "protobuf", "sendSingle": true},
			err:   "server must be in the host:port format: address localhost: missing port in address",
		},
		{
			name:  "invalid timeout",
			props: map[string]any{"server": "localhost:50051", "method": "greeter.Greeter/SayHello", "format": "protobuf", "sendSingle": true, "timeout": "-1s"},
			err:   "timeout must be positive, got -1s",
		},
		{
			name:  "missing method",
			props: map[string]any{"server": "localhost:50051", "format": "protobuf", "sendSingle": true},
			err:   "method is required",
		},
		{
			name:  "default format",
			props: map[string]any{"server": "localhost:50051", "method": "greeter.Greeter/SayHello", "sendSingle": true},
			err:   `format must be protobuf or custom to encode the gRPC request, got ""`,
		},
		{
			name:  "json format",
			props: map[string]any{"server": "localhost:50051", "method": "greeter.Greeter/SayHello", "format": "json", "sendSingle": true},
			err:   `format must be protobuf or custom to encode the gRPC request, got "json"`,
		},
		{
			name:  "not send single",
			props: map[string]any{"server": "localhost:50051", "method": "greeter.Greeter/SayHello", "format": "protobuf"},
			err:   "sendSingle must be true to encode each row as a gRPC request",
		},
		{
			name:  "batch size",
			props: map[string]any{"server": "localhost:50051", "method": "greeter.Greeter/SayHello", "format": "protobuf", "sendSingle": true, "batchSize": 10},
			err:   "batchSize and lingerInterval are not supported because each row is sent as a gRPC request",
		},
		{
			name:  "linger interval",
			props: map[string]any{"server": "localhost:50051", "method": "greeter.Greeter/SayHello", "format": "protobuf", "sendSingle": true, "lingerInterval": 1000},
			err:   "batchSize and lingerInterval are not supported because each row is sent as a gRPC request",
		},
		{
			name:  "invalid metadata",
			props: map[string]any{"server": "localhost:50051", "method": "greeter.Greeter/SayHello", "format": "protobuf", "sendSingle": true, "metadata": "a"},
			err:   "read properties fail with error: 1 error(s) decoding:\n\n* 'metadata' expected a map, got 'string'",
		},
		{
			name:  "invalid tls",
			props: map[string]any{"server": "localhost:50051", "method": "greeter.Greeter/SayHello", "format": "protobuf", "sendSingle": true, "rootCaPath": "not_exist.pem"},
			err:   "open not_exist.pem: no such file or directory",
		},
		{
			name:  "default",
			props: map[string]any{"server": "localhost:50051", "method": "greeter.Greeter/SayHello", "format": "protobuf", "sendSingle": true},
			cc:    &connConf{Server: "localhost:50051", Timeout: cast.DurationConf(defaultTimeout)},
			sc:    &sinkConf{Method: "greeter.Greeter/SayHello", Format: "protobuf", SendSingle: true},
		},
		{
			name: "all",
			props: map[string]any{
				"server": "localhost:50051", "method": "/greeter.Greeter/SayHello", "format": "custom", "sendSingle": true, "timeout": 1000,
				"metadata": map[string]any{"token": "{{.token}}"},
			},
			cc: &connConf{Server: "localhost:50051", Timeout: cast.DurationConf(time.Second)},
			sc: &sinkConf{Method: "/greeter.Greeter/SayHello", Format: "custom", SendSingle: true, Metadata: map[string]string{"token": "{{.token}}"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := GetSink().(*sink)
			err := s.Provision(ctx, tt.props)
			if tt.err != "" {
				require.EqualError(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.cc, s.cc)
			require.Equal(t, tt.sc, s.sc)
		})
	}
}

func TestCollect(t *testing.T) {
	gs := startServer(t)
	ctx := mockContext.NewMockContext("rule", "op")
	s := GetSink().(*sink)
	require.NoError(t, s.Provision(ctx, map[string]any{
		"server":     gs.addr,
		"method":     "greeter.Greeter/SayHello",
		"format":     "protobuf",
		"sendSingle": true,
		"metadata":   map[string]any{"token": "{{.token}}", "source": "ekuiper"},
	}))
	r := &statusRecorder{}
	require.NoError(t, s.Connect(ctx, r.handle))
	require.Eventually(t, func() bool {
		return r.last() == api.ConnectionConnected
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, s.Collect(ctx, &testx.MockRawTuple{
		Content:  encode(t, map[string]any{"name": "a", "count": int64(1)}),
		Template: map[string]string{"{{.token}}": "t1"},
	}))
	require.NoError(t, s.Collect(ctx, &testx.MockRawTuple{
		Content: encode(t, map[string]any{"name": "b", "count": int64(2)}),
	}))
	reqs := gs.received()
	require.Len(t, reqs, 2)
	require.Equal(t, "a", reqs[0].name)
	require.Equal(t, int64(1), reqs[0].count)
	require.Equal(t, []string{"t1"}, reqs[0].md.Get("token"))
	require.Equal(t, []string{"ekuiper"}, reqs[0].md.Get("source"))
	require.Equal(t, []string{"application/grpc+proto"}, reqs[0].md.Get("content-type"))
	require.Equal(t, "b", reqs[1].name)
	require.Equal(t, int64(2), reqs[1].count)
	// The template is sent as is if it is not parsed
	require.Equal(t, []string{"{{.token}}"}, reqs[1].md.Get("token"))

	require.NoError(t, s.Close(ctx))
	// Close again does nothing
	require.NoError(t, s.Close(ctx))
}

func TestCollectError(t *testing.T) {
	gs := startServer(t)
	ctx := mockContext.NewMockContext("rule", "op")
	tests := []struct {
		name      string
		method    string
		serverErr error
		ioErr     bool
		err       string
	}{
		{
			name:      "unavailable",
			method:    "/greeter.Greeter/SayHello",
			serverErr: status.Error(codes.Unavailable, "busy"),
			ioErr:     true,
			err:       "rpc error: code = Unavailable desc = busy",
		},
		{
			name:      "resource exhausted",
			method:    "/greeter.Greeter/SayHello",
			serverErr: status.Error(codes.ResourceExhausted, "quota"),
			ioErr:     true,
			err:       "rpc error: code = ResourceExhausted desc = quota",
		},
		{
			name:      "invalid argument",
			method:    "/greeter.Greeter/SayHello",
			serverErr: status.Error(codes.InvalidArgument, "bad name"),
			err:       "rpc error: code = InvalidArgument desc = bad name",
		},
		{
			name:   "unknown method",
			method: "/greeter.Greeter/SayBye",
			err:    "rpc error: code = Unimplemented desc = unknown method SayBye for service greeter.Greeter",
		},
		{
			name:   "dynamic method",
			method: "{{.method}}",
			err:    "rpc error: code = Unimplemented desc = unknown service greeter.Unknown",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gs.setErr(tt.serverErr)
			s := GetSink().(*sink)
			require.NoError(t, s.Provision(ctx, map[string]any{"server": gs.addr, "method": tt.method, "format": "protobuf", "sendSingle": true}))
			require.NoError(t, s.Connect(ctx, func(string, string) {}))
			defer s.Close(ctx)
			err := s.Collect(ctx, &testx.MockRawTuple{
				Content:  encode(t, map[string]any{"name": "a"}),
				Template: map[string]string{"{{.method}}": "greeter.Unknown/SayHello"},
			})
			require.Error(t, err)
			require.ErrorContains(t, err, tt.err)
			require.ErrorContains(t, err, "grpc sink fails to call")
			require.Equal(t, tt.ioErr, errorx.IsIOError(err))
		})
	}
	require.Empty(t, gs.received())
}

func TestNotConnected(t *testing.T) {
	ctx := mockContext.NewMockContext("rule", "op")
	addr := closedServer(t)
	s := GetSink().(*sink)
	require.NoError(t, s.Provision(ctx, map[string]any{"server": addr, "method": "greeter.Greeter/SayHello", "format": "protobuf", "sendSingle": true, "timeout": "500ms"}))
	r := &statusRecorder{}
	// Connect does not fail the rule so that it recovers once the server is available
	require.NoError(t, s.Connect(ctx, r.handle))
	require.Eventually(t, func() bool {
		return r.last() == api.ConnectionDisconnected
	}, 5*time.Second, 10*time.Millisecond)
	err := s.Collect(ctx, &testx.MockRawTuple{Content: encode(t, map[string]any{"name": "a"})})
	require.Error(t, err)
	require.True(t, errorx.IsIOError(err))
	require.NoError(t, s.Close(ctx))
}

func TestPrivateNet(t *testing.T) {
	gs := startServer(t)
	conf.Config.Basic.EnablePrivateNet = false
	defer func() {
		conf.Config.Basic.EnablePrivateNet = true
	}()
	ctx := mockContext.NewMockContext("rule", "op")
	s := GetSink().(*sink)
	require.Error(t, s.Ping(ctx, map[string]any{"server": gs.addr, "timeout": "500ms"}))
	require.NoError(t, s.Provision(ctx, map[string]any{"server": gs.addr, "method": "greeter.Greeter/SayHello", "format": "protobuf", "sendSingle": true, "timeout": "500ms"}))
	require.NoError(t, s.Connect(ctx, func(string, string) {}))
	defer s.Close(ctx)
	err := s.Collect(ctx, &testx.MockRawTuple{Content: encode(t, map[string]any{"name": "a"})})
	require.ErrorContains(t, err, "in internal network")
	require.Empty(t, gs.received())
}

// selfSignedCert generates a certificate for the loopback address
func selfSignedCert(t *testing.T) tls.Certificate {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func TestTLS(t *testing.T) {
	gs := startServer(t, grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{selfSignedCert(t)}})))
	ctx := mockContext.NewMockContext("rule", "op")
	props := map[string]any{"server": gs.addr, "method": "greeter.Greeter/SayHello", "format": "protobuf", "sendSingle": true, "timeout": "1s"}
	// The plaintext connection is refused by the TLS server
	s := GetSink().(*sink)
	require.NoError(t, s.Provision(ctx, props))
	require.NoError(t, s.Connect(ctx, func(string, string) {}))
	err := s.Collect(ctx, &testx.MockRawTuple{Content: encode(t, map[string]any{"name": "plain"})})
	require.Error(t, err)
	require.NoError(t, s.Close(ctx))

	props["insecureSkipVerify"] = true
	require.NoError(t, s.Ping(ctx, props))
	s = GetSink().(*sink)
	require.NoError(t, s.Provision(ctx, props))
	require.NoError(t, s.Connect(ctx, func(string, string) {}))
	require.NoError(t, s.Collect(ctx, &testx.MockRawTuple{Content: encode(t, map[string]any{"name": "tls"})}))
	require.NoError(t, s.Close(ctx))
	reqs := gs.received()
	require.Len(t, reqs, 1)
	require.Equal(t, "tls", reqs[0].name)
}

func TestPing(t *testing.T) {
	gs := startServer(t)
	ctx := mockContext.NewMockContext("rule", "op")
	s := GetSink().(*sink)
	// Ping only needs the connection properties
	require.NoError(t, s.Ping(ctx, map[string]any{"server": gs.addr}))
	addr := closedServer(t)
	require.EqualError(t, s.Ping(ctx, map[string]any{"server": addr, "timeout": "1s"}), "fail to connect to "+addr)
	require.EqualError(t, s.Ping(ctx, map[string]any{}), "server is required")
}

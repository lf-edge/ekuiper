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

package neuron

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"go.nanomsg.org/mangos/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/lf-edge/ekuiper/v2/internal/topo/node/tracenode"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/nng"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

const (
	DefaultNeuronUrl = "ipc:///tmp/neuron-ekuiper.ipc"
	PROTOCOL         = "pair"
)

var (
	NeuronTraceHeader           = []byte{0x0A, 0xCE}
	NeuronTraceIDStartIndex     = len(NeuronTraceHeader)
	NeuronTraceIDEndIndex       = NeuronTraceIDStartIndex + 16
	NeuronTraceSpanIDStartIndex = NeuronTraceIDEndIndex
	NeuronTraceSpanIDEndIndex   = NeuronTraceSpanIDStartIndex + 8
	NeuronTraceHeaderLen        = 2 + 16 + 8
)

type source struct {
	c     *nng.SockConf
	cli   *nng.Sock
	props map[string]any
}

func (s *source) Provision(_ api.StreamContext, props map[string]any) error {
	props["protocol"] = PROTOCOL
	sc, err := nng.ValidateConf(props)
	if err != nil {
		return err
	}
	s.c = sc
	s.props = props
	return nil
}

func (s *source) Ping(ctx api.StreamContext, props map[string]any) error {
	props["protocol"] = PROTOCOL
	return ping(ctx, props)
}

func (s *source) ConnId(props map[string]any) string {
	var url string
	u, ok := props["url"]
	if ok {
		url = u.(string)
	}
	return "nng:" + PROTOCOL + url
}

func (s *source) SubId(_ map[string]any) string {
	return "singleton"
}

func (s *source) Connect(ctx api.StreamContext, sc api.StatusChangeHandler) error {
	cli, err := connect(ctx, s.c.Url, s.props, sc)
	if err != nil {
		return err
	}
	s.cli = cli.(*nng.Sock)
	return nil
}

func (s *source) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, ingestErr api.ErrorIngest) error {
	ctx.GetLogger().Infof("neuron source receiving loop started")
	go func() {
		err := infra.SafeRun(func() error {
			for {
				// no receiving deadline, will wait until the socket closed
				if msg, err := s.cli.Recv(); err == nil {
					ctx.GetLogger().Debugf("nng received message %s", string(msg))
					rawData, meta := extractTraceMeta(ctx, msg)
					ingest(ctx, rawData, meta, timex.GetNow())
				} else if err == mangos.ErrClosed {
					ctx.GetLogger().Infof("neuron connection closed, retry after 1 second")
					ingestErr(ctx, errors.New("neuron connection closed"))
					time.Sleep(1 * time.Second)
					continue
				} else {
					ingestErr(ctx, fmt.Errorf("neuron receiving error %v", err))
				}
			}
		})
		if err != nil {
			ctx.GetLogger().Errorf("exit neuron source subscribe for %v", err)
		}
	}()
	return nil
}

func (s *source) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("closing neuron source")
	close(ctx, s.cli)
	s.cli = nil
	return nil
}

func GetSource() api.Source {
	return &source{}
}

func extractTraceMeta(ctx api.StreamContext, data []byte) ([]byte, map[string]interface{}) {
	rawData := data
	// extract rawData
	if len(data) > NeuronTraceHeaderLen && bytes.Equal(data[:2], NeuronTraceHeader) {
		rawData = data[NeuronTraceHeaderLen:]
	}
	if !ctx.IsTraceEnabled() {
		return rawData, nil
	}
	meta := make(map[string]interface{})
	var traced bool
	var tracerCtx api.StreamContext
	var span trace.Span
	if len(data) > NeuronTraceHeaderLen && bytes.Equal(data[:2], NeuronTraceHeader) {
		traceID := data[NeuronTraceIDStartIndex:NeuronTraceIDEndIndex]
		spanID := data[NeuronTraceSpanIDStartIndex:NeuronTraceSpanIDEndIndex]
		traced, tracerCtx, span = tracenode.StartTraceByID(ctx, [16]byte(traceID), [8]byte(spanID))
	} else {
		traced, tracerCtx, span = tracenode.StartTrace(ctx, ctx.GetOpId())
	}
	if traced {
		meta["traceId"] = span.SpanContext().TraceID().String()
		meta["traceCtx"] = tracerCtx
		defer span.End()
	}
	return rawData, meta
}

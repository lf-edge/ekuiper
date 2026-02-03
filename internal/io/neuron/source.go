// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/v2/internal/topo/node/tracenode"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/nng"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
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
	conId string
	mu    syncx.RWMutex
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
	ctx.GetLogger().Infof("Connecting to neuron")
	cw, err := connection.FetchConnection(ctx, PROTOCOL+s.c.Url, "nng", s.props, sc)
	if err != nil {
		return err
	}
	s.conId = cw.ID
	cli, err := cw.Wait(ctx)
	if cli == nil {
		return fmt.Errorf("neuron client not ready: %v", err)
	}
	s.cli = cli.(*nng.Sock)
	return nil
}

func (s *source) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, ingestErr api.ErrorIngest) error {
	ctx.GetLogger().Infof("neuron source receiving loop started")
	go func() {
		err := infra.SafeRun(func() error {
			connected := true
			for {
				select {
				case <-ctx.Done():
					ctx.GetLogger().Infof("neuron source receiving loop stopped")
					return nil
				default:
					// no receiving deadline, will wait until the socket closed
					s.mu.RLock()
					cli := s.cli
					s.mu.RUnlock()
					if cli != nil {
						if msg, err := cli.Recv(); err == nil {
							connected = true
							ctx.GetLogger().Debugf("nng received message %s", string(msg))
							rawData, meta := extractTraceMeta(ctx, msg)
							ingest(ctx, rawData, meta, timex.GetNow())
						} else if err == mangos.ErrClosed {
							if connected {
								ctx.GetLogger().Infof("neuron connection closed, retry after 1 second")
								ingestErr(ctx, errors.New("neuron connection closed"))
								time.Sleep(1 * time.Second)
								connected = false
							}
							continue
						}
					}
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
	_ = connection.DetachConnection(ctx, s.conId)
	s.mu.Lock()
	defer s.mu.Unlock()
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
	meta := make(map[string]any)
	meta["sourceKind"] = "neuron"
	if len(data) > NeuronTraceHeaderLen && bytes.Equal(data[:2], NeuronTraceHeader) {
		traceID := data[NeuronTraceIDStartIndex:NeuronTraceIDEndIndex]
		spanID := data[NeuronTraceSpanIDStartIndex:NeuronTraceSpanIDEndIndex]
		// by setting traceId meta, source node knows how to construct a trace
		meta["traceId"] = tracenode.BuildTraceParentId([16]byte(traceID), [8]byte(spanID))
	}
	return rawData, meta
}

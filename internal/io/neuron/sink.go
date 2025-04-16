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
	"encoding/json"
	"fmt"
	"sort"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/topo/node/tracenode"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/nng"
)

type c struct {
	NodeName  string   `json:"nodeName"`
	GroupName string   `json:"groupName"`
	Tags      []string `json:"tags"`
	// If sent with the raw converted string or let us range over the result map
	Raw bool `json:"raw"`
}

type sink struct {
	cw    *connection.ConnWrapper
	c     *c
	cc    *nng.SockConf
	cli   *nng.Sock
	props map[string]any
}

type neuronTemplate struct {
	GroupName string      `json:"group_name"`
	NodeName  string      `json:"node_name"`
	Tags      []neuronTag `json:"tags"`
}

type neuronTag struct {
	Name  string `json:"tag_name"`
	Value any    `json:"value"`
}

func (s *sink) Provision(_ api.StreamContext, props map[string]any) error {
	props["protocol"] = PROTOCOL
	sc, err := nng.ValidateConf(props)
	if err != nil {
		return err
	}
	s.cc = sc
	cc := &c{
		Raw: false,
	}
	err = cast.MapToStruct(props, cc)
	if err != nil {
		return err
	}
	if !cc.Raw {
		if cc.NodeName == "" {
			return fmt.Errorf("node name is required if raw is not set")
		}
		if cc.GroupName == "" {
			return fmt.Errorf("group name is required if raw is not set")
		}
	}
	s.c = cc
	s.props = props
	return nil
}

func (s *sink) Connect(ctx api.StreamContext, sc api.StatusChangeHandler) error {
	ctx.GetLogger().Infof("Connecting to neuron")
	cw, err := connection.FetchConnection(ctx, PROTOCOL+s.cc.Url, "nng", s.props, sc)
	if err != nil {
		return err
	}
	s.cw = cw
	cli, err := cw.Wait(ctx)
	if cli == nil {
		return fmt.Errorf("neuron client not ready: %v", err)
	}
	s.cli = cli.(*nng.Sock)
	return nil
}

func (s *sink) Collect(ctx api.StreamContext, data api.MessageTuple) error {
	ctx.GetLogger().Debugf("receive %+v", data)
	if s.c.Raw {
		m := data.ToMap()
		r, err := json.Marshal(m)
		if err != nil {
			return err
		}
		r = extractSpanContextIntoData(ctx, data, r)
		return s.cli.Send(ctx, r)
	} else {
		return s.SendMapToNeuron(ctx, data)
	}
}

// CollectList sends all data at best effort
// It never return error, so it is not supported for cache and retry
func (s *sink) CollectList(ctx api.StreamContext, data api.MessageTupleList) error {
	data.RangeOfTuples(func(index int, tuple api.MessageTuple) bool {
		err := s.Collect(ctx, tuple)
		if err != nil {
			ctx.GetLogger().Errorf("send data %v error %v", data, err)
		}
		return true
	})
	return nil
}

func (s *sink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("closing neuron sink")
	if s.cw != nil {
		_ = connection.DetachConnection(ctx, s.cw.ID)
	}
	s.cli = nil
	return nil
}

func (s *sink) SendMapToNeuron(ctx api.StreamContext, tuple api.MessageTuple) error {
	n := s.c.NodeName
	g := s.c.GroupName
	if dp, ok := tuple.(api.HasDynamicProps); ok {
		temp, transformed := dp.DynamicProps(n)
		if transformed {
			n = temp
		}
	}
	if dp, ok := tuple.(api.HasDynamicProps); ok {
		temp, transformed := dp.DynamicProps(g)
		if transformed {
			g = temp
		}
	}
	t := &neuronTemplate{
		NodeName:  n,
		GroupName: g,
	}
	el := tuple.ToMap()
	var tags []neuronTag
	if len(s.c.Tags) == 0 {
		if conf.IsTesting {
			var keys []string
			for k := range el {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				tags = append(tags, neuronTag{k, el[k]})
			}
		} else {
			for k, v := range el {
				tags = append(tags, neuronTag{k, v})
			}
		}
	} else {
		tags = make([]neuronTag, 0, len(s.c.Tags))
		// Send as many tags as possible in order and drop the tag if it is invalid
		for _, tag := range s.c.Tags {
			n, err := ctx.ParseTemplate(tag, el)
			if err != nil {
				ctx.GetLogger().Errorf("Error parsing tag %s: %v", tag, err)
				continue
			}
			v, ok := el[n]
			if !ok {
				ctx.GetLogger().Errorf("Error get the value of tag %s: %v", n, err)
				continue
			}
			tags = append(tags, neuronTag{n, v})
		}
	}
	t.Tags = tags
	return doPublish(ctx, s.cli, tuple, t)
}

func doPublish(ctx api.StreamContext, cli *nng.Sock, tuple api.MessageTuple, t *neuronTemplate) error {
	r, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("Error marshall the tag payload %v: %v", t, err)
	}
	r = extractSpanContextIntoData(ctx, tuple, r)
	err = cli.Send(ctx, r)
	if err != nil {
		return errorx.NewIOErr(fmt.Sprintf(`Error publish the tag payload %v: %v`, t, err))
	}
	ctx.GetLogger().Debugf("Send %s", r)
	return nil
}

func extractSpanContextIntoData(ctx api.StreamContext, data any, sendBytes []byte) []byte {
	traced, _, span := tracenode.TraceInput(ctx.GetRuleId(), fmt.Sprintf("%s_emit", ctx.GetOpId()), data)
	if traced {
		defer span.End()
		traceID := span.SpanContext().TraceID()
		spanID := span.SpanContext().SpanID()
		r := NeuronTraceHeader
		r = append(r, traceID[:]...)
		r = append(r, spanID[:]...)
		r = append(r, sendBytes...)
		return r
	}
	return sendBytes
}

func GetSink() api.Sink {
	return &sink{}
}

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
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
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
	c     *c
	cc    *nng.SockConf
	cli   *nng.Sock
	props map[string]any
}

type neuronTemplate struct {
	GroupName string      `json:"group_name"`
	NodeName  string      `json:"node_name"`
	TagName   string      `json:"tag_name"`
	Value     interface{} `json:"value"`
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

func (s *sink) Ping(ctx api.StreamContext, props map[string]interface{}) error {
	props["protocol"] = PROTOCOL
	return ping(ctx, props)
}

func (s *sink) Connect(ctx api.StreamContext) error {
	cli, err := connect(ctx, s.cc.Url, s.props)
	if err != nil {
		return err
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
	close(ctx, s.cli, s.cc.Url, s.props)
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
	var (
		ok  bool
		err error
	)
	el := tuple.ToMap()
	if len(s.c.Tags) == 0 {
		if conf.IsTesting {
			var keys []string
			for k := range el {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				t.TagName = k
				t.Value = el[k]
				err := doPublish(ctx, s.cli, t)
				if err != nil {
					return err
				}
			}
		} else {
			for k, v := range el {
				t.TagName = k
				t.Value = v
				err := doPublish(ctx, s.cli, t)
				if err != nil {
					return err
				}
			}
		}
	} else {
		// Send as many tags as possible in order and drop the tag if it is invalid
		for _, tag := range s.c.Tags {
			t.TagName, err = ctx.ParseTemplate(tag, el)
			if err != nil {
				ctx.GetLogger().Errorf("Error parsing tag %s: %v", tag, err)
				continue
			}
			t.Value, ok = el[t.TagName]
			if !ok {
				ctx.GetLogger().Errorf("Error get the value of tag %s: %v", t.TagName, err)
				continue
			}
			err := doPublish(ctx, s.cli, t)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func doPublish(ctx api.StreamContext, cli *nng.Sock, t *neuronTemplate) error {
	r, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("Error marshall the tag payload %v: %v", t, err)
	}
	err = cli.Send(ctx, r)
	if err != nil {
		return errorx.NewIOErr(fmt.Sprintf(`Error publish the tag payload %s: %v`, t.TagName, err))
	}
	ctx.GetLogger().Debugf("Send %s", r)
	return nil
}

func GetSink() api.Sink {
	return &sink{}
}

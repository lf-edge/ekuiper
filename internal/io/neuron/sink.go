// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
)

type sink struct {
	c   *c
	cli *conninfo
}

type c struct {
	NodeName  string   `json:"nodeName"`
	GroupName string   `json:"groupName"`
	Tags      []string `json:"tags"`
	// If sent with the raw converted string or let us range over the result map
	Raw bool   `json:"raw"`
	Url string `json:"url"`
}

type neuronTemplate struct {
	GroupName string      `json:"group_name"`
	NodeName  string      `json:"node_name"`
	TagName   string      `json:"tag_name"`
	Value     interface{} `json:"value"`
}

func (s *sink) Configure(props map[string]interface{}) error {
	cc := &c{
		Raw: false,
		Url: DefaultNeuronUrl,
	}
	err := cast.MapToStruct(props, cc)
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
	return nil
}

func (s *sink) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("Opening neuron sink")
	cli, err := createOrGetConnection(ctx, s.c.Url)
	if err != nil {
		return err
	}
	s.cli = cli
	return nil
}

func (s *sink) Collect(ctx api.StreamContext, data interface{}) error {
	ctx.GetLogger().Debugf("receive %+v", data)
	if s.c.Raw {
		r, _, err := ctx.TransformOutput(data, true)
		if err != nil {
			return err
		}
		return publish(ctx, r, s.cli)
	} else {
		switch d := data.(type) {
		case []map[string]interface{}:
			for _, el := range d {
				err := s.SendMapToNeuron(ctx, el)
				if err != nil {
					return err
				}
			}
			return nil
		case map[string]interface{}:
			return s.SendMapToNeuron(ctx, d)
		default:
			return fmt.Errorf("unrecognized format of %s", data)
		}
	}
}

func (s *sink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("closing neuron sink")
	if s.cli != nil {
		return closeConnection(ctx, s.c.Url)
	}
	return nil
}

func (s *sink) SendMapToNeuron(ctx api.StreamContext, el map[string]interface{}) error {
	n, err := ctx.ParseTemplate(s.c.NodeName, el)
	if err != nil {
		return err
	}
	g, err := ctx.ParseTemplate(s.c.GroupName, el)
	if err != nil {
		return err
	}
	t := &neuronTemplate{
		NodeName:  n,
		GroupName: g,
	}
	var (
		ok bool
	)
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
				err := doPublish(ctx, t, s.cli)
				if err != nil {
					return err
				}
			}
		} else {
			for k, v := range el {
				t.TagName = k
				t.Value = v
				err := doPublish(ctx, t, s.cli)
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
			err := doPublish(ctx, t, s.cli)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func doPublish(ctx api.StreamContext, t *neuronTemplate, cli *conninfo) error {
	r, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("Error marshall the tag payload %v: %v", t, err)
	}
	err = publish(ctx, r, cli)
	if err != nil {
		return fmt.Errorf("%s: Error publish the tag payload %s: %v", errorx.IOErr, t.TagName, err)
	}
	ctx.GetLogger().Debugf("Publish %s", r)
	return nil
}

func GetSink() *sink {
	return &sink{}
}

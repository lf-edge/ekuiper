// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/errorx"
	"sort"
)

type sink struct {
	url       string
	c         *c
	connected bool
}

type c struct {
	NodeName  string   `json:"nodeName"`
	GroupName string   `json:"groupName"`
	Tags      []string `json:"tags"`
	// If sent with the raw converted string or let us range over the result map
	Raw bool `json:"raw"`
}

type neuronTemplate struct {
	GroupName string      `json:"group_name"`
	NodeName  string      `json:"node_name"`
	TagName   string      `json:"tag_name"`
	Value     interface{} `json:"value"`
}

func (s *sink) Configure(props map[string]interface{}) error {
	s.url = NeuronUrl
	cc := &c{
		NodeName:  "unknown",
		GroupName: "unknown",
		Raw:       false,
	}
	err := cast.MapToStruct(props, cc)
	if err != nil {
		return err
	}
	s.c = cc
	return nil
}

func (s *sink) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("Opening neuron sink")
	err := createOrGetConnection(ctx, s.url)
	if err != nil {
		return err
	}
	s.connected = true
	return nil
}

func (s *sink) Collect(ctx api.StreamContext, data interface{}) error {
	ctx.GetLogger().Debugf("receive %+v", data)
	if s.c.Raw {
		r, _, err := ctx.TransformOutput(data)
		if err != nil {
			return err
		}
		return publish(ctx, r)
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
	if s.connected {
		return closeConnection(ctx, s.url)
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
				err := doPublish(ctx, t)
				if err != nil {
					return err
				}
			}
		} else {
			for k, v := range el {
				t.TagName = k
				t.Value = v
				err := doPublish(ctx, t)
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
			err := doPublish(ctx, t)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func doPublish(ctx api.StreamContext, t *neuronTemplate) error {
	r, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("Error marshall the tag payload %v: %v", t, err)
	}
	err = publish(ctx, r)
	if err != nil {
		return fmt.Errorf("%s: Error publish the tag payload %s: %v", errorx.IOErr, t.TagName, err)
	}
	ctx.GetLogger().Debugf("Publish %s", r)
	return nil
}

func GetSink() *sink {
	return &sink{}
}

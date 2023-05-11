// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

package pubsub

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/gdexlab/go-render/render"

	"github.com/lf-edge/ekuiper/pkg/api"
)

func TestCreateAndClose(t *testing.T) {
	Reset()
	var (
		sourceTopics = []string{"h/d1/c1/s2", "h/+/+/s1", "h/d3/#", "h/d1/c1/s2", "h/+/c1/s1"}
		sinkTopics   = []string{"h/d1/c1/s1", "h/d1/c1/s2", "h/d2/c2/s1", "h/d3/c3/s1", "h/d1/c1/s1"}
		chans        []chan api.SourceTuple
	)
	for i, topic := range sinkTopics {
		CreatePub(topic)
		var (
			r   *regexp.Regexp
			err error
		)
		if strings.ContainsAny(sourceTopics[i], "+#") {
			r, err = getRegexp(sourceTopics[i])
			if err != nil {
				t.Error(err)
				return
			}
		}
		c := CreateSub(sourceTopics[i], r, fmt.Sprintf("%d", i), 100)
		chans = append(chans, c)
	}

	expPub := map[string]*pubConsumers{
		"h/d1/c1/s1": {
			count: 2,
			consumers: map[string]chan api.SourceTuple{
				"1": chans[1],
				"4": chans[4],
			},
		},
		"h/d1/c1/s2": {
			count: 1,
			consumers: map[string]chan api.SourceTuple{
				"0": chans[0],
				"3": chans[3],
			},
		},
		"h/d2/c2/s1": {
			count: 1,
			consumers: map[string]chan api.SourceTuple{
				"1": chans[1],
			},
		},
		"h/d3/c3/s1": {
			count: 1,
			consumers: map[string]chan api.SourceTuple{
				"1": chans[1],
				"2": chans[2],
			},
		},
	}
	if !reflect.DeepEqual(expPub, pubTopics) {
		t.Errorf("Error adding: Expect\n\t%v\nbut got\n\t%v", render.AsCode(expPub), render.AsCode(pubTopics))
		return
	}
	i := 0
	for i < 3 {
		CloseSourceConsumerChannel(sourceTopics[i], fmt.Sprintf("%d", i))
		RemovePub(sinkTopics[i])
		i++
	}
	expPub = map[string]*pubConsumers{
		"h/d1/c1/s1": {
			count: 1,
			consumers: map[string]chan api.SourceTuple{
				"4": chans[4],
			},
		},
		"h/d1/c1/s2": {
			count: 0,
			consumers: map[string]chan api.SourceTuple{
				"3": chans[3],
			},
		},
		"h/d3/c3/s1": {
			count:     1,
			consumers: map[string]chan api.SourceTuple{},
		},
	}
	if !reflect.DeepEqual(expPub, pubTopics) {
		t.Errorf("Error closing: Expect\n\t%v\nbut got\n\t %v", render.AsCode(expPub), render.AsCode(pubTopics))
	}
}

func getRegexp(topic string) (*regexp.Regexp, error) {
	if len(topic) == 0 {
		return nil, fmt.Errorf("invalid empty topic")
	}

	levels := strings.Split(topic, "/")
	for i, level := range levels {
		if level == "#" && i != len(levels)-1 {
			return nil, fmt.Errorf("invalid topic %s: # must at the last level", topic)
		}
	}
	regstr := strings.Replace(strings.ReplaceAll(topic, "+", "([^/]+)"), "#", ".", 1)
	return regexp.Compile(regstr)
}

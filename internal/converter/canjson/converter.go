// Copyright 2023 EMQ Technologies Co., Ltd.
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

package canjson

import (
	"encoding/hex"
	"fmt"

	"github.com/ngjaying/can"
	"github.com/ngjaying/can/pkg/descriptor"
	"github.com/valyala/fastjson"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/converter/can/dbc"
	"github.com/lf-edge/ekuiper/pkg/message"
)

// The format of the json.
// Comment out as we use fastjson
//type packedFrames struct {
//	Meta   map[string]interface{} `json:"meta,omitempty"`
//	Frames []can.Frame            `json:"frames,omitempty"`
//}

type Converter struct {
	messages map[uint32]*descriptor.Message
}

func (c *Converter) Encode(_ interface{}) ([]byte, error) {
	// TODO implement me
	panic("implement me")
}

func (c *Converter) Decode(b []byte) (interface{}, error) {
	var p fastjson.Parser
	v, err := p.ParseBytes(b)
	if err != nil {
		return nil, fmt.Errorf("invalid frame json `%s` received: %v", b, err)
	}
	// The format is staic, so we can use static struct to decode
	obj, err := v.Object()
	if err != nil {
		return nil, fmt.Errorf("invalid frame json `%s`, should be object but receive error: %v", b, err)
	}

	// decode frames
	rawFrames, err := obj.Get("frames").Array()
	if err != nil {
		return nil, fmt.Errorf("invalid frame json `%s`, should have frames array but receive error: %v", b, err)
	}
	if rawFrames == nil || len(rawFrames) == 0 {
		return nil, fmt.Errorf("invalid frame json `%s`, should have frames array but receive empty", b)
	}
	result := make(map[string]interface{})
	for _, rawFrame := range rawFrames {
		tid, err := rawFrame.Get("id").Uint()
		if err != nil {
			return nil, fmt.Errorf("invalid frame json `%s`, frame id should be uint but receive error: %v", b, err)
		}
		// Filter out invalid/unknown id frame, avoid to parse them
		desc, ok := c.messages[uint32(tid)]
		if !ok {
			conf.Log.Warnf("cannot find message %d", tid)
			continue
		}
		tdata := rawFrame.Get("data").GetStringBytes()
		if err != nil {
			return nil, fmt.Errorf("invalid frame json `%s`, frame data should be string but receive error: %v", b, err)
		}
		decodedData := make([]byte, hex.DecodedLen(len(tdata)))
		_, err = hex.Decode(decodedData, tdata)
		if err != nil {
			return nil, fmt.Errorf("invalid frame json `%s`, frame data should be hex string but receive error: %v", b, err)
		}
		signals := desc.Decode(&can.Payload{Data: decodedData})
		for _, s := range signals {
			result[s.Signal.Name] = s.Value
		}
	}
	// decode meta, ignore for now
	//metaObj, err := obj.Get("meta").Object()
	//if err != nil {
	//	return nil, fmt.Errorf("invalid frame json `%s`, should have meta object but receive error: %v", b, err)
	//}
	//if metaObj != nil {
	//	metaMap := make(map[string]interface{})
	//	metaObj.Visit(func(k []byte, v *fastjson.Value) {
	//		switch v.Type() {
	//		case fastjson.TypeNumber:
	//			metaMap[string(k)] = v.GetFloat64()
	//		case fastjson.TypeString:
	//			metaMap[string(k)] = v.String()
	//		case fastjson.TypeTrue:
	//			metaMap[string(k)] = true
	//		case fastjson.TypeFalse:
	//			metaMap[string(k)] = false
	//		default:
	//			conf.Log.Warnf("unknown type %s for meta %s", v.Type(), k)
	//		}
	//	})
	//	result["meta"] = metaMap
	//}
	return result, nil
}

func NewConverter(dbcPath string) (message.Converter, error) {
	mm, err := dbc.ParsePath(dbcPath)
	if err != nil {
		return nil, err
	}
	return &Converter{
		messages: mm,
	}, nil
}

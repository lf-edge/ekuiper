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

package can

import (
	"fmt"

	"github.com/ngjaying/can/pkg/descriptor"
	"github.com/ngjaying/can/pkg/socketcan"

	"github.com/lf-edge/ekuiper/internal/converter/can/dbc"
	"github.com/lf-edge/ekuiper/pkg/message"
)

// The converter for socketCan format
// Expects to receive a socketCan bytes array [16]byte with canId and data inside

type Converter struct {
	messages map[uint32]*descriptor.Message
}

func (c *Converter) Encode(interface{}) ([]byte, error) {
	// TODO implement me
	panic("implement me")
}

func (c *Converter) Decode(b []byte) (interface{}, error) {
	frame := socketcan.Frame{}
	frame.UnmarshalBinary(b)
	if frame.IsError() {
		return nil, fmt.Errorf("error frame received: %v", frame.DecodeErrorFrame())
	}
	canFrame := frame.DecodeFrame()
	desc, ok := c.messages[canFrame.ID]
	if !ok {
		return nil, fmt.Errorf("cannot find message %d", canFrame.ID)
	}
	if desc != nil {
		result := make(map[string]interface{})
		desc.DecodeToMap(&canFrame, result)
		return result, nil
	} else {
		return nil, fmt.Errorf("can't find message descriptor for %d", canFrame.ID)
	}
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

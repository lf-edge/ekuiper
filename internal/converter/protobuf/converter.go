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

package protobuf

import (
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"

	"github.com/lf-edge/ekuiper/internal/converter/static"
	"github.com/lf-edge/ekuiper/pkg/message"
)

type Converter struct {
	descriptor *desc.MessageDescriptor
	fc         *FieldConverter
}

var protoParser *protoparse.Parser

func init() {
	protoParser = &protoparse.Parser{}
}

func NewConverter(schemaFile string, soFile string, messageName string) (message.Converter, error) {
	if soFile != "" {
		return static.LoadStaticConverter(soFile, messageName)
	} else {
		if fds, err := protoParser.ParseFiles(schemaFile); err != nil {
			return nil, fmt.Errorf("parse schema file %s failed: %s", schemaFile, err)
		} else {
			messageDescriptor := fds[0].FindMessage(messageName)
			if messageDescriptor == nil {
				return nil, fmt.Errorf("message type %s not found in schema file %s", messageName, schemaFile)
			}
			return &Converter{
				descriptor: messageDescriptor,
				fc:         GetFieldConverter(),
			}, nil
		}
	}
}

func (c *Converter) Encode(d interface{}) ([]byte, error) {
	switch m := d.(type) {
	case map[string]interface{}:
		msg, err := c.fc.encodeMap(c.descriptor, m)
		if err != nil {
			return nil, err
		}
		return msg.Marshal()
	default:
		return nil, fmt.Errorf("unsupported type %v, must be a map", d)
	}
}

func (c *Converter) Decode(b []byte) (interface{}, error) {
	result := mf.NewDynamicMessage(c.descriptor)
	err := result.Unmarshal(b)
	if err != nil {
		return nil, err
	}
	return c.fc.DecodeMessage(result, c.descriptor), nil
}

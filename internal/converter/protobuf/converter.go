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

package protobuf

import (
	"fmt"

	"github.com/jhump/protoreflect/desc"            //nolint:staticcheck
	"github.com/jhump/protoreflect/desc/protoparse" //nolint:staticcheck
	"github.com/lf-edge/ekuiper/contract/v2/api"

	kconf "github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/converter/static"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

type Converter struct {
	descriptor *desc.MessageDescriptor
	fc         *FieldConverter
}

var protoParser *protoparse.Parser

func init() {
	etcDir, _ := kconf.GetLoc("etc/schemas/protobuf/")
	dataDir, _ := kconf.GetLoc("data/schemas/protobuf/")
	protoParser = &protoparse.Parser{ImportPaths: []string{etcDir, dataDir}}
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

func (c *Converter) Encode(ctx api.StreamContext, d any) (b []byte, err error) {
	defer func() {
		if err != nil {
			err = errorx.NewWithCode(errorx.CovnerterErr, err.Error())
		}
	}()
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

func (c *Converter) Decode(ctx api.StreamContext, b []byte) (m any, err error) {
	defer func() {
		if err != nil {
			err = errorx.NewWithCode(errorx.CovnerterErr, err.Error())
		}
	}()
	result := mf.NewDynamicMessage(c.descriptor)
	err = result.Unmarshal(b)
	if err != nil {
		return nil, err
	}
	return c.fc.DecodeMessage(result, c.descriptor), nil
}

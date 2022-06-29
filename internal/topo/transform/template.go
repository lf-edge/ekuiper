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

package transform

import (
	"bytes"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/converter"
	"github.com/lf-edge/ekuiper/pkg/message"
	"strings"
	"text/template"
)

type TransFunc func(interface{}) ([]byte, bool, error)

func GenTransform(dt string, format string, schemaId string) (TransFunc, error) {
	var (
		tp  *template.Template = nil
		c   message.Converter
		err error
	)
	switch format {
	case message.FormatProtobuf:
		r := strings.Split(schemaId, ".")
		if len(r) != 2 {
			return nil, fmt.Errorf("invalid schemaId: %s", schemaId)
		}
		c, err = converter.GetOrCreateConverter(message.FormatProtobuf, r[0], schemaId)
		if err != nil {
			return nil, err
		}
	}

	if dt != "" {
		temp, err := template.New("sink").Funcs(conf.FuncMap).Parse(dt)
		if err != nil {
			return nil, err
		}
		tp = temp
	}
	return func(d interface{}) ([]byte, bool, error) {
		var (
			bs          []byte
			transformed bool
		)
		if tp != nil {
			var output bytes.Buffer
			err := tp.Execute(&output, d)
			if err != nil {
				return nil, false, fmt.Errorf("fail to encode data %v with dataTemplate for error %v", d, err)
			}
			bs = output.Bytes()
			transformed = true
		}
		switch format {
		case message.FormatJson:
			if transformed {
				return bs, transformed, nil
			}
			j, err := message.Marshal(d)
			return j, false, err
		case message.FormatProtobuf:
			if transformed {
				m := make(map[string]interface{})
				err := message.Unmarshal(bs, &m)
				if err != nil {
					return nil, false, fmt.Errorf("fail to decode data %s after applying dataTemplate for error %v", string(bs), err)
				}
				d = m
			}
			b, err := c.Encode(d)
			return b, transformed, err
		default: // should not happen
			return nil, false, fmt.Errorf("unsupported format %v", format)
		}
	}, nil
}

func GenTp(dt string) (*template.Template, error) {
	return template.New("sink").Funcs(conf.FuncMap).Parse(dt)
}

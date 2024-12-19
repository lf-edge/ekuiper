// Copyright 2024 EMQ Technologies Co., Ltd.
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

package xml

import (
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"

	"github.com/beevik/etree"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

const xmlValue = "@value"

type XMLConverter struct{}

func (x *XMLConverter) Encode(ctx api.StreamContext, d any) ([]byte, error) {
	return covertToEncodingXml(d)
}

type EncodingMap map[string]interface{}

type EncodingSlice []interface{}

type EncodingValue map[string]interface{}

func convertToEncodingStruct(value interface{}, isValue bool) interface{} {
	switch v := value.(type) {
	case map[string]interface{}:
		if isValue {
			encodingValue := make(EncodingValue)
			for k, v := range v {
				encodingValue[k] = convertToEncodingStruct(v, false)
			}
			return encodingValue
		} else {
			encodingMap := make(EncodingMap)
			for key, val := range v {
				encodingMap[key] = convertToEncodingStruct(val, key == xmlValue)
			}
			return encodingMap
		}

	case []interface{}:
		encodingSlice := make(EncodingSlice, len(v))
		for i, val := range v {
			encodingSlice[i] = convertToEncodingStruct(val, true)
		}
		return encodingSlice
	default:
		return v
	}
}

func (e EncodingValue) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	for k, v := range e {
		space, local := splitKey(k)
		elem := xml.StartElement{
			Name: xml.Name{
				Space: space,
				Local: local,
			},
		}
		switch v := v.(type) {
		case EncodingMap:
			err := v.MarshalXML(enc, elem)
			if err != nil {
				return err
			}
		default:
			// TODO: print error
		}
	}
	return nil
}

func (e EncodingMap) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	for k, v := range e {
		if k != xmlValue {
			space, local := splitKey(k)
			start.Attr = append(start.Attr, xml.Attr{Name: xml.Name{Space: space, Local: local}, Value: fmt.Sprint(v)})
		}
	}
	if err := enc.EncodeToken(start); err != nil {
		return err
	}
	for k, v := range e {
		if k == xmlValue {
			switch v1 := v.(type) {
			case EncodingSlice:
				err := v1.MarshalXML(enc, start)
				if err != nil {
					return err
				}
			default:
				if err := enc.EncodeToken(xml.CharData(fmt.Sprintf("%v", v))); err != nil {
					return err
				}
			}
			break
		}
	}
	if err := enc.EncodeToken(start.End()); err != nil {
		return err
	}
	return nil
}

func (e EncodingSlice) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	for _, value := range e {
		switch v := value.(type) {
		case EncodingValue:
			err := v.MarshalXML(enc, start)
			if err != nil {
				return err
			}
		default:
			// TODO: print error
		}
	}
	return nil
}

func covertToEncodingXml(d any) ([]byte, error) {
	c := convertToEncodingStruct(d, true)
	xmlData, err := xml.Marshal(c)
	return xmlData, err
}

func (x *XMLConverter) Decode(ctx api.StreamContext, b []byte) (got any, err error) {
	defer func() {
		if r := recover(); r != nil {
			conf.Log.Errorf("xml decode panic, err:%v data:%v", r, string(b))
			err = fmt.Errorf("xml decode panic: %v", r)
		}
	}()
	return decodeXML(b)
}

func NewXMLConverter() *XMLConverter {
	return &XMLConverter{}
}

func decodeXML(b []byte) (any, error) {
	doc := etree.NewDocument()
	err := doc.ReadFromBytes(b)
	if err != nil {
		return nil, err
	}
	result, err := extractEleValue(&doc.Element)
	if err != nil {
		return nil, err
	}
	mm, ok := result.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid xml data:%v", string(b))
	}
	v, ok := mm[xmlValue].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid xml data:%v", string(b))
	}
	if len(v) == 1 {
		return v[0], nil
	}
	return v, nil
}

func extractEleValue(ele *etree.Element) (got any, err error) {
	defer func() {
		if err == nil && got != nil {
			m, ok := got.(map[string]interface{})
			if ok {
				for _, attr := range ele.Attr {
					key := buildAttrKey(attr)
					m[key] = attr.Value
				}
			}
		}
	}()
	if len(ele.ChildElements()) > 0 {
		curr := make([]interface{}, 0)
		for _, child := range ele.ChildElements() {
			key := buildKey(child)
			value, err := extractEleValue(child)
			if err != nil {
				return nil, err
			}
			curr = append(curr, map[string]interface{}{key: value})
		}
		outer := make(map[string]interface{})
		outer[xmlValue] = curr
		return outer, nil
	} else {
		m := make(map[string]interface{})
		if len(ele.Child) == 1 {
			v, err := extractValue(ele.Child[0])
			if err != nil {
				return nil, err
			}
			m[xmlValue] = v
		}
		return m, nil
	}
}

func extractValue(token etree.Token) (any, error) {
	got, err := func() (any, error) {
		switch v := token.(type) {
		case *etree.CharData:
			bv, err := strconv.ParseBool(v.Data)
			if err == nil {
				return bv, nil
			}
			iv, err := strconv.ParseInt(v.Data, 10, 64)
			if err == nil {
				return iv, nil
			}
			fv, err := strconv.ParseFloat(v.Data, 64)
			if err == nil {
				return fv, nil
			}
			return v.Data, nil
		default:
			return nil, fmt.Errorf("extractValue not charData")
		}
	}()
	if err != nil {
		return nil, err
	}
	return got, nil
}

func buildKey(ele *etree.Element) string {
	if ele.Space == "" {
		return ele.Tag
	}
	return fmt.Sprintf("%s:%s", ele.Space, ele.Tag)
}

func buildAttrKey(attr etree.Attr) string {
	if attr.Space == "" {
		return attr.Key
	}
	return fmt.Sprintf("%s:%s", attr.Space, attr.Key)
}

func splitKey(key string) (string, string) {
	ss := strings.Split(key, ":")
	if len(ss) == 2 {
		return ss[0], ss[1]
	}
	return "", key
}

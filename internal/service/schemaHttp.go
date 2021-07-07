// Copyright 2021 EMQ Technologies Co., Ltd.
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

package service

import (
	"fmt"
	dpb "github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"google.golang.org/protobuf/reflect/protoreflect"
	"net/http"
	"regexp"
	"strings"
)

type httpConnMeta struct {
	Method string
	Uri    string // The Uri is a relative path which must start with /
	Body   []byte
}

type httpMapping interface {
	ConvertHttpMapping(method string, params []interface{}) (*httpConnMeta, error)
}

const (
	httpAPI      = "google.api.http"
	wildcardBody = "*"
	emptyBody    = ""
)

type httpOptions struct {
	Method      string
	UriTemplate *uriTempalte // must not nil
	BodyField   string
}

type uriTempalte struct {
	Template string
	Fields   []*field
}

type field struct {
	name   string
	prefix string
}

func (d *wrappedProtoDescriptor) parseHttpOptions() error {
	optionsMap := make(map[string]*httpOptions)
	var err error
	for _, s := range d.GetServices() {
		for _, m := range s.GetMethods() {
			options := m.GetMethodOptions()
			var ho *httpOptions
			// Find http option and exit loop at once. If not found, http option is nil
			options.ProtoReflect().Range(func(d protoreflect.FieldDescriptor, v protoreflect.Value) bool {
				if d.FullName() == httpAPI {
					if d.Kind() == protoreflect.MessageKind {
						var (
							uriOpt  string
							bodyOpt string
							err     error
						)
						ho = &httpOptions{}
						v.Message().Range(func(din protoreflect.FieldDescriptor, vin protoreflect.Value) bool {
							switch din.Name() {
							case "get":
								ho.Method = http.MethodGet
								uriOpt, err = getUriOpt(din, vin)
							case "put":
								ho.Method = http.MethodPut
								uriOpt, err = getUriOpt(din, vin)
							case "delete":
								ho.Method = http.MethodDelete
								uriOpt, err = getUriOpt(din, vin)
							case "post":
								ho.Method = http.MethodPost
								uriOpt, err = getUriOpt(din, vin)
							case "patch":
								ho.Method = http.MethodPatch
								uriOpt, err = getUriOpt(din, vin)
							case "body":
								bodyOpt, err = getUriOpt(din, vin)
							default:
								err = fmt.Errorf("unsupported option %s", din.Name())
							}
							if err != nil {
								return false
							}
							return true
						})
						if err != nil {
							return false
						}
						err = ho.convertUri(m, uriOpt, bodyOpt)
						if err != nil {
							return false
						}
					} else {
						err = fmt.Errorf("invalid http option for method %s in proto", m.GetName())
					}
					return false
				}
				if err != nil {
					return false
				}
				return true
			})
			if err != nil {
				return err
			}
			if ho != nil {
				optionsMap[m.GetName()] = ho
			}
		}
	}
	d.methodOptions = optionsMap
	return err
}

func (d *wrappedProtoDescriptor) ConvertHttpMapping(method string, params []interface{}) (*httpConnMeta, error) {
	hcm := &httpConnMeta{}
	var (
		json []byte
		err  error
	)
	if ho, ok := d.methodOptions[method]; ok {
		message, err := d.ConvertParamsToMessage(method, params)
		if err != nil {
			return nil, err
		}
		if len(ho.UriTemplate.Fields) > 0 {
			args := make([]interface{}, len(ho.UriTemplate.Fields))
			for i, v := range ho.UriTemplate.Fields {
				fv, err := getMessageFieldWithDots(message, v.name)
				if err != nil {
					return nil, err
				}
				args[i], err = cast.ToString(fv, cast.CONVERT_ALL)
				if err != nil {
					return nil, fmt.Errorf("invalid field %s(%v) as http option, must be string", v.name, fv)
				}
				// Remove all params to be used in the params, the left params are for BODY
				level1Names := strings.Split(v.name, ".")
				message.ClearFieldByName(level1Names[0])
				if v.prefix != "" {
					if strings.HasPrefix(args[i].(string), v.prefix) {
						continue
					} else {
						return nil, fmt.Errorf("invalid field %s(%s) as http option, must have prefix %s", v.name, args[i], v.prefix)
					}
				}
			}
			hcm.Uri = fmt.Sprintf(ho.UriTemplate.Template, args...)
		} else {
			hcm.Uri = ho.UriTemplate.Template
		}
		hcm.Method = ho.Method
		switch ho.BodyField {
		case wildcardBody:
			json, err = message.MarshalJSON()
		case emptyBody:
			json = nil
		default:
			bodyMessage := message.GetFieldByName(ho.BodyField)
			if bm, ok := bodyMessage.(*dynamic.Message); ok {
				json, err = bm.MarshalJSON()
			} else {
				return nil, fmt.Errorf("invalid body field %s, must be a message", ho.BodyField)
			}
		}
	} else { // If options are not set, use the default setting
		hcm.Method = "POST"
		hcm.Uri = "/" + method
		json, err = d.ConvertParamsToJson(method, params)
	}
	if err != nil {
		return nil, err
	}
	hcm.Body = json
	return hcm, nil
}

func getMessageFieldWithDots(message *dynamic.Message, name string) (interface{}, error) {
	secs := strings.Split(name, ".")
	currentMessage := message
	for i, sec := range secs {
		if i == len(secs)-1 {
			return currentMessage.GetFieldByName(sec), nil
		} else {
			c := currentMessage.GetFieldByName(sec)
			if cm, ok := c.(*dynamic.Message); ok {
				currentMessage = cm
			} else {
				return nil, fmt.Errorf("fail to find field %s", name)
			}
		}
	}
	return nil, fmt.Errorf("fail to find field %s", name)
}

func getUriOpt(d protoreflect.FieldDescriptor, v protoreflect.Value) (string, error) {
	if d.Kind() != protoreflect.StringKind {
		return "", fmt.Errorf("invalid type for %s option, string required", d.Name())
	}
	return v.String(), nil
}

func (ho *httpOptions) convertUri(md *desc.MethodDescriptor, uriOpt string, bodyOpt string) error {
	fmap := make(map[string]bool) // the value represents if the key is still available (not used) so that they can be removed from *
	for _, f := range md.GetInputType().GetFields() {
		fmap[f.GetName()] = true
	}
	result := &uriTempalte{}
	re := regexp.MustCompile(`\{(.*?)\}`)
	m := re.FindAllStringSubmatch(uriOpt, -1)
	if len(m) > 0 {
		result.Template = re.ReplaceAllString(uriOpt, "%s")
		var fields []*field
		for _, e := range m {
			f := &field{}
			rr := strings.Split(e[1], "=")
			if len(rr) == 2 {
				if strings.HasSuffix(rr[1], "*") {
					f.name = rr[0]
					f.prefix = rr[1][:len(rr[1])-1]
				} else {
					return fmt.Errorf("invalid uri %s in http option", uriOpt)
				}
			} else if len(rr) == 1 {
				f.name = e[1]
			} else {
				return fmt.Errorf("invalid uri %s in http option", uriOpt)
			}
			_, ok := fmap[f.name]
			if !ok {
				return fmt.Errorf("invalid uri %s in http option, %s field not found", uriOpt, f.name)
			}
			fmap[f.name] = false
			fields = append(fields, f)
		}
		result.Fields = fields
	} else {
		result.Template = uriOpt
	}
	switch bodyOpt {
	case wildcardBody:
		ho.BodyField = bodyOpt
	default:
		if bodyOpt != emptyBody {
			if _, ok := fmap[bodyOpt]; !ok {
				return fmt.Errorf("invalid body %s, field not found", bodyOpt)
			} else {
				fmap[bodyOpt] = false
			}
		}
		ho.BodyField = bodyOpt
		paramAdded := false
		result.updateUriParams(md.GetInputType(), "", fmap, paramAdded)
	}
	ho.UriTemplate = result
	return nil
}

func (u *uriTempalte) updateUriParams(md *desc.MessageDescriptor, prefix string, fmap map[string]bool, paramAdded bool) bool {
	var jointer string
	for _, mf := range md.GetFields() {
		if fmap[mf.GetName()] || prefix != "" { // The first level field which are not consumed or the second level field
			if mf.GetType() == dpb.FieldDescriptorProto_TYPE_MESSAGE {
				paramAdded = u.updateUriParams(mf.GetMessageType(), prefix+mf.GetName()+".", fmap, paramAdded)
				continue
			}
			if !paramAdded {
				paramAdded = true
				jointer = "?"
			} else {
				jointer = "&"
			}
			u.Template = fmt.Sprintf("%s%s%s%s=%s", u.Template, jointer, prefix, mf.GetName(), "%s")
			u.Fields = append(u.Fields, &field{name: prefix + mf.GetName()})
		}
	}
	return paramAdded
}

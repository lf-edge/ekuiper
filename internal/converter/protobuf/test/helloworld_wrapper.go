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

package main

import (
	"fmt"

	"github.com/golang/protobuf/proto"
)

func (x *HelloReply) Encode(d interface{}) ([]byte, error) {
	switch r := d.(type) {
	case map[string]interface{}:
		t, ok := r["message"]
		if ok {
			if v, ok := t.(string); ok {
				x.Message = v
			} else {
				fmt.Println("message is not string")
			}
		} else {
			// if required, return error
			fmt.Println("message is not found")
		}
		return proto.Marshal(x)
	default:
		return nil, fmt.Errorf("unsupported type %v, must be a map", d)
	}
}

func (x *HelloReply) Decode(b []byte) (interface{}, error) {
	err := proto.Unmarshal(b, x)
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{}, 1)
	result["message"] = x.Message
	return result, nil
}

func GetHelloReply() interface{} {
	return &HelloReply{}
}

func (x *HelloRequest) Encode(d interface{}) ([]byte, error) {
	switch r := d.(type) {
	case map[string]interface{}:
		t, ok := r["name"]
		if ok {
			if v, ok := t.(string); ok {
				x.Name = v
			} else {
				return nil, fmt.Errorf("name is not string")
			}
		} else {
			// if required, return error
			fmt.Println("message is not found")
		}
		return proto.Marshal(x)
	default:
		return nil, fmt.Errorf("unsupported type %v, must be a map", d)
	}
}

func (x *HelloRequest) Decode(b []byte) (interface{}, error) {
	err := proto.Unmarshal(b, x)
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{}, 1)
	result["name"] = x.Name
	return result, nil
}

func GetHelloRequest() interface{} {
	return &HelloRequest{}
}

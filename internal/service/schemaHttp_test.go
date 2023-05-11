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
	"net/http"
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/internal/testx"
)

func TestBookstoreConvertHttpMapping(t *testing.T) {
	tests := []struct {
		method string
		params []interface{}
		result *httpConnMeta
		err    string
	}{
		{ // 0 create book
			method: "CreateBook",
			params: []interface{}{
				1984,
				map[string]interface{}{
					"id":     20210519,
					"author": "Conan Doyle",
					"title":  "Sherlock Holmes",
				},
			},
			// int64 will be marshaled to string!
			result: &httpConnMeta{
				Method: http.MethodPost,
				Uri:    "/v1/shelves/1984/books",
				Body:   []byte(`{"id":"20210519","author":"Conan Doyle","title":"Sherlock Holmes"}`),
			},
		}, { // 2 delete book
			method: "DeleteBook",
			params: []interface{}{
				1984,
				20210519,
			},
			result: &httpConnMeta{
				Method: http.MethodDelete,
				Uri:    "/v1/shelves/1984/books/20210519",
			},
		}, { // 3 list shelves
			method: "ListShelves",
			params: []interface{}{},
			result: &httpConnMeta{
				Method: http.MethodGet,
				Uri:    "/v1/shelves",
			},
		},
	}
	d, err := parse(PROTOBUFF, "http_bookstore.proto")
	if err != nil {
		panic(err)
	}
	for i, tt := range tests {
		r, err := d.(httpMapping).ConvertHttpMapping(tt.method, tt.params)
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d : interface error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.result, r) {
			t.Errorf("%d \n\ninterface result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.result, r)
		}
	}
}

func TestMessagingConvertHttpMapping(t *testing.T) {
	tests := []struct {
		method string
		params []interface{}
		result *httpConnMeta
		err    string
	}{
		{ // 0 get message
			method: "GetMessage",
			params: []interface{}{
				"messages/123456",
			},
			// int64 will be marshaled to string!
			result: &httpConnMeta{
				Method: http.MethodGet,
				Uri:    "/v1/messages/123456",
			},
		}, { // 1 get message prefix error
			method: "GetMessage",
			params: []interface{}{
				"message/123456",
			},
			err: "invalid field name(message/123456) as http option, must have prefix messages/",
		}, { // 2 search messages
			method: "SearchMessage",
			params: []interface{}{
				"123456",
				2,
				map[string]interface{}{
					"subfield": "foo",
				},
			},
			result: &httpConnMeta{
				Method: http.MethodGet,
				Uri:    "/v1/messages/filter/123456?revision=2&sub.subfield=foo",
			},
		}, { // 3 update message
			method: "UpdateMessage",
			params: []interface{}{
				"123456",
				map[string]interface{}{
					"text": "Hi!",
				},
			},
			result: &httpConnMeta{
				Method: http.MethodPut,
				Uri:    "/v1/messages/123456",
				Body:   []byte(`{"text":"Hi!"}`),
			},
		}, { // 4 patch message
			method: "PatchMessage",
			params: []interface{}{
				"123456",
				"Hi!",
			},
			result: &httpConnMeta{
				Method: http.MethodPatch,
				Uri:    "/v1/messages/123456",
				Body:   []byte(`{"text":"Hi!"}`),
			},
		},
	}
	d, err := parse(PROTOBUFF, "http_messaging.proto")
	if err != nil {
		panic(err)
	}
	for i, tt := range tests {
		r, err := d.(httpMapping).ConvertHttpMapping(tt.method, tt.params)
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d : interface error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.result, r) {
			t.Errorf("%d \n\ninterface result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.result, r)
		}
	}
}

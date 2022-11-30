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
	"testing"
)

func TestWrapper(t *testing.T) {
	r := HelloReply{}
	m := map[string]interface{}{"message": "hello"}
	bytes, err := r.Encode(m)
	if err != nil {
		t.Errorf("encode error: %v", err)
	}
	fmt.Printf("bytes: %X\n", bytes)
	mf, err := r.Decode(bytes)
	if err != nil {
		t.Errorf("decode error: %v", err)
	}
	fmt.Printf("mf: %v\n", mf)
}

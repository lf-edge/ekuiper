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

package main

import (
	"bufio"
	"github.com/lf-edge/ekuiper/sdk/mock"
	"os"
	"reflect"
	"testing"
)

var CACHE_FILE = "cache"

func TestFileSink(t *testing.T) {
	defer os.Remove(CACHE_FILE)
	s := &fileSink{}
	s.Configure(make(map[string]interface{}))
	exp := []string{"foo", "bar"}
	err := mock.RunSinkCollect(s, exp)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	result := getResults()
	if !reflect.DeepEqual(result, exp) {
		t.Errorf("result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", exp, result)
	}
}

func getResults() []string {
	f, err := os.Open(CACHE_FILE)
	if err != nil {
		panic(err)
	}
	result := make([]string, 0)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		result = append(result, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	_ = f.Close()
	return result
}

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

package util

import (
	"sync"
	"testing"
)

func TestDriverPool(t *testing.T) {
	url := "mock"
	testPool := newDBPool()
	testPool.isTesting = true

	expCount := 3
	wg := sync.WaitGroup{}
	wg.Add(expCount)
	for i := 0; i < expCount; i++ {
		go func() {
			defer func() {
				wg.Done()
			}()
			_, err := FetchDBToOneNode(testPool, url)
			if err != nil {
				t.Errorf("meet unexpected err:%v", err)
			}
		}()
	}
	wg.Wait()
	count := getDBConnCount(testPool, url)
	if expCount != count {
		t.Errorf("expect conn count:%v, got:%v", expCount, count)
	}

	wg.Add(expCount)
	for i := 0; i < expCount; i++ {
		go func() {
			defer func() {
				wg.Done()
			}()
			err := ReturnDBFromOneNode(testPool, url)
			if err != nil {
				t.Errorf("meet unexpected err:%v", err)
			}
		}()
	}
	wg.Wait()
	count = getDBConnCount(testPool, url)
	if count != 0 {
		t.Errorf("expect conn count:%v, got:%v", 0, count)
	}
}

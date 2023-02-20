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

package dbc

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/ngjaying/can/pkg/descriptor"
	"github.com/ngjaying/can/pkg/generate"
)

func ParsePath(dbcPath string) (map[uint32]*descriptor.Message, error) {
	dir, err := os.Stat(dbcPath)
	if err != nil {
		return nil, err
	}
	mm := make(map[uint32]*descriptor.Message)
	if dir.IsDir() {
		var (
			files []string
			max   int64
		)
		err = filepath.Walk(dbcPath, func(path string, info os.FileInfo, err error) error {
			if strings.EqualFold(filepath.Ext(path), ".dbc") {
				if info.Size() > max {
					max = info.Size()
				}
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		b1 := make([]byte, max)
		for _, file := range files {
			f, err := os.Open(file)
			if err != nil {
				return nil, err
			}
			n, err := f.Read(b1)
			if err != nil {
				return nil, err
			}
			err = addMessageDb(mm, file, b1[:n])
			if err != nil {
				return nil, err
			}
		}
	} else {
		dbc, err := os.ReadFile(dbcPath)
		if nil != err {
			return nil, err
		}
		err = addMessageDb(mm, dbcPath, dbc)
		if err != nil {
			return nil, err
		}
	}
	return mm, nil
}

func addMessageDb(mm map[uint32]*descriptor.Message, dbcPath string, dbcContent []byte) error {
	c, err := generate.Compile(dbcPath, dbcContent)
	if err != nil {
		return err
	}
	for _, m := range c.Database.Messages {
		if _, ok := mm[m.ID]; !ok {
			mm[m.ID] = m
		}
	}
	return nil
}

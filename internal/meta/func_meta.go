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

package meta

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func readFuncMetaDir() []fileContent {
	var filesByte []fileContent

	confDir, err := conf.GetConfLoc()
	if nil != err {
		return nil
	}

	dir := filepath.Join(confDir, "functions")
	files, err := os.ReadDir(dir)
	if nil != err {
		return nil
	}
	for _, file := range files {
		fname := file.Name()
		if !strings.HasSuffix(fname, ".json") {
			continue
		}
		filesByte = append(filesByte, readFuncMetaFile(filepath.Join(dir, fname)))
	}

	confDir, err = conf.GetDataLoc()
	if nil != err {
		return nil
	}

	dir = filepath.Join(confDir, "functions")
	files, err = os.ReadDir(dir)
	if nil != err {
		return nil
	}
	for _, file := range files {
		fname := file.Name()
		if !strings.HasSuffix(fname, ".json") {
			continue
		}
		filesByte = append(filesByte, readFuncMetaFile(filepath.Join(dir, fname)))
	}
	return filesByte
}

func readFuncMetaFile(filePath string) fileContent {
	fiName := filepath.Base(filePath)
	sliByte, _ := os.ReadFile(filePath)
	conf.Log.Infof("funcMeta file : %s", fiName)
	return sliByte
}

func GetFunctions() bytes.Buffer {
	files := readFuncMetaDir()
	return ConstructJsonArray(files)
}

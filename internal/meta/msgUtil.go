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
	"path"

	kconf "github.com/lf-edge/ekuiper/internal/conf"
	"gopkg.in/ini.v1"
)

var gUimsg map[string]*ini.File

func getMsg(language, section, key string) string {
	language += ".ini"
	if conf, ok := gUimsg[language]; ok {
		s := conf.Section(section)
		if s != nil {
			return s.Key(key).String()
		}
	}
	return ""
}
func ReadUiMsgDir() error {
	gUimsg = make(map[string]*ini.File)
	confDir, err := kconf.GetConfLoc()
	if nil != err {
		return err
	}

	dir := path.Join(confDir, "multilingual")
	dirEntries, err := os.ReadDir(dir)
	if nil != err {
		return err
	}

	for _, entry := range dirEntries {
		fName := entry.Name()
		kconf.Log.Infof("uiMsg file : %s", fName)
		fPath := path.Join(dir, fName)
		if conf, err := ini.Load(fPath); nil != err {
			return err
		} else {
			gUimsg[fName] = conf
		}
	}
	return nil
}

func ConstructJsonArray(jsonByteItems []fileContent) bytes.Buffer {
	var buf bytes.Buffer
	var length = len(jsonByteItems)
	if length == 0 {
		buf.Write([]byte("[]"))
		return buf
	}

	buf.Write([]byte("["))
	buf.Write(jsonByteItems[0])

	for i := 1; i < length; i++ {
		buf.Write([]byte(","))
		buf.Write(jsonByteItems[i])
	}

	buf.Write([]byte("]"))
	return buf
}

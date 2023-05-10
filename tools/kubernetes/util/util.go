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

package util

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	kconf "github.com/lf-edge/ekuiper/tools/kubernetes/conf"
)

type (
	command struct {
		Url         string      `json:"url"`
		Description string      `json:"description"`
		Method      string      `json:"method"`
		Data        interface{} `json:"data"`
		strLog      string
	}
	fileData struct {
		Commands []*command `json:"commands"`
	}
)

func (c *command) getLog() string {
	return c.strLog
}

func (c *command) call(host string) bool {
	var resp []byte
	var err error
	head := host + c.Url
	body, _ := json.Marshal(c.Data)
	switch c.Method {
	case "post", "POST":
		resp, err = kconf.Post(head, string(body))
		break
	case "get", "GET":
		resp, err = kconf.Get(head)
		break
	case "delete", "DELETE":
		resp, err = kconf.Delete(head)
		break
	case "put", "PUT":
		resp, err = kconf.Put(head, string(body))
		break
	default:
		c.strLog = fmt.Sprintf("no such method : %s", c.Method)
		return false
	}
	if nil == err {
		c.strLog = fmt.Sprintf("%s:%s resp:%s", head, c.Method, string(resp))
		return true
	}
	c.strLog = fmt.Sprintf("%s:%s resp:%s err:%v", head, c.Method, string(resp), err)
	return false
}

type (
	historyFile struct {
		Name     string `json:"name"`
		LoadTime int64  `json:"loadTime"`
	}
	server struct {
		dirCommand     string
		fileHistory    string
		mapHistoryFile map[string]*historyFile
		logs           []string
	}
)

func (f *historyFile) setName(name string) {
	f.Name = name
}

func (f *historyFile) setLoadTime(loadTime int64) {
	f.LoadTime = loadTime
}

func (s *server) getLogs() []string {
	return s.logs
}

func (s *server) printLogs() {
	for _, v := range s.logs {
		kconf.Log.Info(v)
	}
	s.logs = s.logs[:0]
}

func (s *server) loadHistoryFile() bool {
	var sli []*historyFile
	if err := kconf.LoadFileUnmarshal(s.fileHistory, &sli); nil != err {
		kconf.Log.Info(err)
		return false
	}
	for _, v := range sli {
		s.mapHistoryFile[v.Name] = v
	}
	return true
}

func (s *server) init() bool {
	s.mapHistoryFile = make(map[string]*historyFile)
	conf := kconf.GetConf()
	dirCommand := conf.GetCommandDir()
	s.dirCommand = dirCommand
	s.fileHistory = path.Join(path.Dir(dirCommand), ".history")
	if _, err := os.Stat(s.fileHistory); os.IsNotExist(err) {
		if _, err = os.Create(s.fileHistory); nil != err {
			kconf.Log.Info(err)
			return false
		}
		return true
	}
	return s.loadHistoryFile()
}

func (s *server) saveHistoryFile() bool {
	var sli []*historyFile
	for _, v := range s.mapHistoryFile {
		sli = append(sli, v)
	}
	err := kconf.SaveFileMarshal(s.fileHistory, sli)
	if nil != err {
		kconf.Log.Info(err)
		return false
	}
	return true
}

func (s *server) isUpdate(entry os.DirEntry) bool {
	v := s.mapHistoryFile[entry.Name()]
	if nil == v {
		return true
	}

	info, err := entry.Info()
	if err != nil {
		return false
	}

	if v.LoadTime < info.ModTime().Unix() {
		return true
	}
	return false
}

func (s *server) processDir() bool {
	dirEntries, err := os.ReadDir(s.dirCommand)
	if nil != err {
		s.logs = append(s.logs, fmt.Sprintf("read command dir:%v", err))
		return false
	}
	conf := kconf.GetConf()
	host := fmt.Sprintf(`http://%s:%d`, conf.GetIp(), conf.GetPort())
	for _, entry := range dirEntries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		if !s.isUpdate(entry) {
			continue
		}

		hisFile := new(historyFile)
		hisFile.setName(entry.Name())
		hisFile.setLoadTime(time.Now().Unix())
		s.mapHistoryFile[entry.Name()] = hisFile

		filePath := path.Join(s.dirCommand, entry.Name())
		file := new(fileData)
		err = kconf.LoadFileUnmarshal(filePath, file)
		if nil != err {
			s.logs = append(s.logs, fmt.Sprintf("load command file:%v", err))
			return false
		}

		for _, command := range file.Commands {
			flag := command.call(host)
			s.logs = append(s.logs, command.getLog())
			if !flag {
				break
			}
		}
	}
	s.saveHistoryFile()
	return true
}

func (s *server) watchFolders() {
	conf := kconf.GetConf()
	s.processDir()
	s.printLogs()
	chTime := time.Tick(time.Second * time.Duration(conf.GetIntervalTime()))
	for {
		select {
		case <-chTime:
			s.processDir()
			s.printLogs()
		}
	}
}

func Process() {
	if len(os.Args) != 2 {
		fmt.Println("Missing configuration file")
		return
	}

	conf := kconf.GetConf()
	if !conf.Init() {
		return
	}

	se := new(server)
	if !se.init() {
		se.printLogs()
		return
	}
	fmt.Println("Kuiper kubernetes tool is started successfully!")
	se.watchFolders()
}

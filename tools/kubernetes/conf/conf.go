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

package conf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type (
	config struct {
		Port         int    `yaml:"port"`
		Timeout      int    `yaml:"timeout"`
		IntervalTime int    `yaml:"intervalTime"`
		Ip           string `yaml:"ip"`
		ConsoleLog   bool   `yaml:"consoleLog"`
		FileLog      bool   `yaml:"fileLog"`
		LogPath      string `yaml:"logPath"`
		CommandDir   string `yaml:"commandDir"`
	}
)

var gConf config

func GetConf() *config {
	return &gConf
}

func (c *config) GetIntervalTime() int {
	return c.IntervalTime
}

func (c *config) GetIp() string {
	return c.Ip
}

func (c *config) GetPort() int {
	return c.Port
}

func (c *config) GetLogPath() string {
	return c.LogPath
}

func (c *config) GetCommandDir() string {
	return c.CommandDir
}

func processPath(path string) (string, error) {
	if abs, err := filepath.Abs(path); err != nil {
		return "", nil
	} else {
		if _, err := os.Stat(abs); os.IsNotExist(err) {
			return "", err
		}
		return abs, nil
	}
}

func (c *config) initConfig() bool {
	confPath, err := processPath(os.Args[1])
	if nil != err {
		fmt.Println("conf path err : ", err)
		return false
	}
	sliByte, err := os.ReadFile(confPath)
	if nil != err {
		fmt.Println("load conf err : ", err)
		return false
	}
	err = yaml.Unmarshal(sliByte, c)
	if nil != err {
		fmt.Println("unmashal conf err : ", err)
		return false
	}

	if c.CommandDir, err = filepath.Abs(c.CommandDir); err != nil {
		fmt.Println("command dir err : ", err)
		return false
	}
	if _, err = os.Stat(c.CommandDir); os.IsNotExist(err) {
		fmt.Println("not found dir : ", c.CommandDir)
		return false
	}

	if c.LogPath, err = filepath.Abs(c.LogPath); nil != err {
		fmt.Println("log dir err : ", err)
		return false
	}
	if _, err = os.Stat(c.LogPath); os.IsNotExist(err) {
		if err = os.MkdirAll(path.Dir(c.LogPath), 0o755); nil != err {
			fmt.Println("mak logdir err : ", err)
			return false
		}
	}
	return true
}

var (
	Log     *logrus.Logger
	gClient http.Client
)

func (c *config) initTimeout() {
	gClient.Timeout = time.Duration(c.Timeout) * time.Millisecond
}

func (c *config) initLog() bool {
	Log = logrus.New()
	Log.SetReportCaller(true)
	Log.SetFormatter(&logrus.TextFormatter{
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			filename := path.Base(f.File)
			return "", fmt.Sprintf("%s:%d", filename, f.Line)
		},
		DisableColors: true,
		FullTimestamp: true,
	})
	if c.FileLog {
		logFile, err := os.OpenFile(c.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o666)
		if err != nil {
			fmt.Println("Failed to init log file settings..." + err.Error())
			Log.Infof("Failed to log to file, using default stderr.")
			return false
		} else if c.ConsoleLog {
			mw := io.MultiWriter(os.Stdout, logFile)
			Log.SetOutput(mw)
		} else if !c.ConsoleLog {
			Log.SetOutput(logFile)
		}
	} else if c.ConsoleLog {
		Log.SetOutput(os.Stdout)
	}
	return true
}

func (c *config) Init() bool {
	if !c.initConfig() {
		return false
	}

	if !c.initLog() {
		return false
	}
	c.initTimeout()
	return true
}

func fetchContents(request *http.Request) (data []byte, err error) {
	respon, err := gClient.Do(request)
	if nil != err {
		return nil, err
	}
	defer respon.Body.Close()
	data, err = io.ReadAll(respon.Body)
	if nil != err {
		return nil, err
	}
	/*
		if respon.StatusCode < 200 || respon.StatusCode > 299 {
			return data, fmt.Errorf("http return code: %d and error message %s.", respon.StatusCode, string(data))
		}
	*/
	return data, err
}

func Get(inUrl string) (data []byte, err error) {
	request, err := http.NewRequest(http.MethodGet, inUrl, nil)
	if nil != err {
		return nil, err
	}
	return fetchContents(request)
}

func Post(inHead, inBody string) (data []byte, err error) {
	request, err := http.NewRequest(http.MethodPost, inHead, bytes.NewBuffer([]byte(inBody)))
	if nil != err {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	return fetchContents(request)
}

func Put(inHead, inBody string) (data []byte, err error) {
	request, err := http.NewRequest(http.MethodPut, inHead, bytes.NewBuffer([]byte(inBody)))
	if nil != err {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	return fetchContents(request)
}

func Delete(inUrl string) (data []byte, err error) {
	request, err := http.NewRequest(http.MethodDelete, inUrl, nil)
	if nil != err {
		return nil, err
	}
	return fetchContents(request)
}

func LoadFileUnmarshal(path string, ret interface{}) error {
	sliByte, err := os.ReadFile(path)
	if nil != err {
		return err
	}
	err = json.Unmarshal(sliByte, ret)
	if nil != err {
		return err
	}
	return nil
}

func SaveFileMarshal(path string, content interface{}) error {
	data, err := json.Marshal(content)
	if nil != err {
		return err
	}
	return os.WriteFile(path, data, 0o666)
}

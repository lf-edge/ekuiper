package common

import (
	"bytes"
	"fmt"
	"github.com/go-yaml/yaml"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"time"
)

type (
	config struct {
		Port         int    `yaml:"port"`
		Timeout      int    `yaml:"timeout"`
		IntervalTime int    `yaml:"intervalTime"`
		Ip           string `yaml:"ip"`
		LogPath      string `yaml:"logPath"`
		CommandDir   string `yaml:"commandDir"`
	}
)

var g_conf config

func GetConf() *config {
	return &g_conf
}
func (this *config) GetIntervalTime() int {
	return this.IntervalTime
}
func (this *config) GetIp() string {
	return this.Ip
}
func (this *config) GetPort() int {
	return this.Port
}
func (this *config) GetLogPath() string {
	return this.LogPath
}
func (this *config) GetCommandDir() string {
	return this.CommandDir
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

func (this *config) initConfig() bool {
	confPath, err := processPath(os.Args[1])
	if nil != err {
		fmt.Println("conf path err : ", err)
		return false
	}
	sliByte, err := ioutil.ReadFile(confPath)
	if nil != err {
		fmt.Println("load conf err : ", err)
		return false
	}
	err = yaml.Unmarshal(sliByte, this)
	if nil != err {
		fmt.Println("unmashal conf err : ", err)
		return false
	}

	if this.CommandDir, err = filepath.Abs(this.CommandDir); err != nil {
		fmt.Println("command dir err : ", err)
		return false
	}
	if _, err = os.Stat(this.CommandDir); os.IsNotExist(err) {
		return false
	}

	if this.LogPath, err = filepath.Abs(this.LogPath); nil != err {
		fmt.Println("log dir err : ", err)
		return false
	}
	if _, err = os.Stat(this.LogPath); os.IsNotExist(err) {
		if err = os.MkdirAll(path.Dir(this.LogPath), 0755); nil != err {
			fmt.Println("mak logdir err : ", err)
			return false
		}
	}
	return true
}

var (
	Log      *logrus.Logger
	g_client http.Client
)

func (this *config) initTimeout() {
	g_client.Timeout = time.Duration(this.Timeout) * time.Millisecond
}

func (this *config) initLog() bool {
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

	logFile, err := os.OpenFile(this.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		Log.SetOutput(logFile)
		return true
	} else {
		Log.Infof("Failed to log to file, using default stderr.")
		return false
	}
	return false
}
func (this *config) Init() bool {
	if !this.initConfig() {
		return false
	}

	if !this.initLog() {
		return false
	}
	this.initTimeout()
	return true
}

func fetchContents(request *http.Request) (data []byte, err error) {
	respon, err := g_client.Do(request)
	if nil != err {
		return nil, err
	}
	defer respon.Body.Close()
	data, err = ioutil.ReadAll(respon.Body)
	if nil != err {
		return nil, err
	}
	if respon.StatusCode < 200 || respon.StatusCode > 299 {
		return data, fmt.Errorf("http return code: %d and error message %s.", respon.StatusCode, string(data))
	}
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

func Delete(inUrl string) (data []byte, err error) {
	request, err := http.NewRequest(http.MethodDelete, inUrl, nil)
	if nil != err {
		return nil, err
	}
	return fetchContents(request)
}

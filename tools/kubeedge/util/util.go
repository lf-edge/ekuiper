package util

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"kubeedge/common"
	"os"
	"path"
	"strconv"
	"time"
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

func (this *command) getLog() string {
	return this.strLog
}

func (this *command) call() bool {
	var resp []byte
	var err error
	conf := common.GetConf()
	head := fmt.Sprintf(`http://%s:%d%s`, conf.GetIp(), conf.GetPort(), this.Url)
	body, _ := json.Marshal(this.Data)
	switch this.Method {
	case "post", "POST":
		resp, err = common.Post(head, string(body))
		break
	case "get", "GET":
		resp, err = common.Get(head)
		break
	case "delete", "DELETE":
		resp, err = common.Delete(head)
		break
	default:
		this.strLog = fmt.Sprintf("no such method : %s", this.Method)
		return false

	}
	if nil == err {
		this.strLog = fmt.Sprintf("%s:%s resp:%s", head, this.Method, string(resp))
		return true
	}
	this.strLog = fmt.Sprintf("%s:%s resp:%s err:%v", head, this.Method, string(resp), err)
	return false
}

type (
	server struct {
		dirCommand string
		dirHistory string
		logs       []string
	}
)

func (this *server) setDirCommand(dir string) {
	this.dirCommand = dir
}
func (this *server) setDirHistory(dir string) {
	this.dirHistory = dir
}

func (this *server) getLogs() []string {
	return this.logs
}
func (this *server) printLogs() {
	for _, v := range this.logs {
		common.Log.Info(v)
	}
	this.logs = this.logs[:0]
}

func (this *server) init() bool {
	conf := common.GetConf()
	dirCommand := conf.GetCommandDir()
	dirHistory := path.Join(path.Dir(dirCommand), ".history")
	if err := os.MkdirAll(dirHistory, 0755); nil != err {
		this.logs = append(this.logs, fmt.Sprintf("mkdir history dir:%v", err))
		return false
	}
	this.dirCommand = dirCommand
	this.dirHistory = dirHistory
	return true
}

func (this *server) processDir() bool {
	infos, err := ioutil.ReadDir(this.dirCommand)
	if nil != err {
		this.logs = append(this.logs, fmt.Sprintf("read command dir:%v", err))
		return false
	}
	for _, info := range infos {
		filePath := path.Join(this.dirCommand, info.Name())
		file := new(fileData)
		sliByte, err := ioutil.ReadFile(filePath)
		if nil != err {
			this.logs = append(this.logs, fmt.Sprintf("load command file:%v", err))
			return false
		}
		err = json.Unmarshal(sliByte, file)
		if nil != err {
			this.logs = append(this.logs, fmt.Sprintf("unmarshal command file:%v", err))
			return false
		}

		for _, command := range file.Commands {
			flag := command.call()
			this.logs = append(this.logs, command.getLog())
			if !flag {
				return false
			}
		}
		newFileName := info.Name() + "_" + strconv.FormatInt(time.Now().Unix(), 10)
		newFilePath := path.Join(this.dirHistory, newFileName)
		os.Rename(filePath, newFilePath)
	}
	return true
}

func (this *server) watchFolders() {
	conf := common.GetConf()
	this.processDir()
	this.printLogs()
	chTime := time.Tick(time.Second * time.Duration(conf.GetIntervalTime()))
	for {
		select {
		case <-chTime:
			this.processDir()
			this.printLogs()
		}
	}
}

func Process() {
	if len(os.Args) != 2 {
		common.Log.Fatal("Missing configuration file")
		return
	}

	conf := common.GetConf()
	if !conf.Init() {
		return
	}

	se := new(server)
	if !se.init() {
		se.printLogs()
		return
	}
	se.watchFolders()
}

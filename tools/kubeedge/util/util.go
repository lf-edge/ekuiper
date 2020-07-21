package util

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/tools/kubeedge/common"
	"io/ioutil"
	"os"
	"path"
	"strings"
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

func (this *command) call(host string) bool {
	var resp []byte
	var err error
	head := host + this.Url
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

func (this *historyFile) setName(name string) {
	this.Name = name
}
func (this *historyFile) setLoadTime(loadTime int64) {
	this.LoadTime = loadTime
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

func (this *server) loadHistoryFile() bool {
	var sli []*historyFile
	if err := common.LoadFileUnmarshal(this.fileHistory, &sli); nil != err {
		common.Log.Info(err)
		return false
	}
	for _, v := range sli {
		this.mapHistoryFile[v.Name] = v
	}
	return true
}

func (this *server) init() bool {
	this.mapHistoryFile = make(map[string]*historyFile)
	conf := common.GetConf()
	dirCommand := conf.GetCommandDir()
	this.dirCommand = dirCommand
	this.fileHistory = path.Join(path.Dir(dirCommand), ".history")
	if _, err := os.Stat(this.fileHistory); os.IsNotExist(err) {
		if _, err = os.Create(this.fileHistory); nil != err {
			common.Log.Info(err)
			return false
		}
		return true
	}
	return this.loadHistoryFile()
}

func (this *server) saveHistoryFile() bool {
	var sli []*historyFile
	for _, v := range this.mapHistoryFile {
		sli = append(sli, v)
	}
	err := common.SaveFileMarshal(this.fileHistory, sli)
	if nil != err {
		common.Log.Info(err)
		return false
	}
	return true
}

func (this *server) isUpdate(info os.FileInfo) bool {
	v := this.mapHistoryFile[info.Name()]
	if nil == v {
		return true
	}

	if v.LoadTime < info.ModTime().Unix() {
		return true
	}
	return false
}

func (this *server) processDir() bool {
	infos, err := ioutil.ReadDir(this.dirCommand)
	if nil != err {
		this.logs = append(this.logs, fmt.Sprintf("read command dir:%v", err))
		return false
	}
	conf := common.GetConf()
	host := fmt.Sprintf(`http://%s:%d`, conf.GetIp(), conf.GetPort())
	for _, info := range infos {
		if !strings.HasSuffix(info.Name(), ".json") {
			continue
		}
		if !this.isUpdate(info) {
			continue
		}

		hisFile := new(historyFile)
		hisFile.setName(info.Name())
		hisFile.setLoadTime(time.Now().Unix())
		this.mapHistoryFile[info.Name()] = hisFile

		filePath := path.Join(this.dirCommand, info.Name())
		file := new(fileData)
		err = common.LoadFileUnmarshal(filePath, file)
		if nil != err {
			this.logs = append(this.logs, fmt.Sprintf("load command file:%v", err))
			return false
		}

		for _, command := range file.Commands {
			flag := command.call(host)
			this.logs = append(this.logs, command.getLog())
			if !flag {
				break
			}
		}
	}
	this.saveHistoryFile()
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
	fmt.Println("Kuiper kubeedge tool is started successfully!")
	se.watchFolders()
}

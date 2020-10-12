package plugins

import (
	"github.com/emqx/kuiper/common"
	"io/ioutil"
	"path"
	"strconv"
	"strings"
)

var g_uiMsg map[string]map[int]string

func getUiMsg(language string, code int) string {
	if msgs, ok := g_uiMsg[language]; ok {
		if msg, ok := msgs[code]; ok {
			return msg
		}
	}
	return ""
}
func (m *Manager) readUiMsgFile(fPath string) (map[int]string, error) {
	fData, err := ioutil.ReadFile(fPath)
	if nil != err {
		return nil, err
	}
	rows := strings.Split(string(fData), "\n")

	msgs := make(map[int]string)
	for i := 0; i < len(rows); i++ {
		row := strings.Split(rows[i], "=")
		if 2 != len(row) {
			common.Log.Infof("uiMsg format error : %s", rows[i])
			continue
		}
		code, err := strconv.Atoi(row[0])
		if nil != err {
			common.Log.Infof("uiMsg data error : %s", rows[i])
			continue
		}
		msgs[code] = row[1]
	}
	return msgs, err
}

func (m *Manager) readUiMsgDir() error {
	g_uiMsg = make(map[string]map[int]string)
	confDir, err := common.GetConfLoc()
	if nil != err {
		return err
	}

	dir := path.Join(confDir, "multilingual")
	infos, err := ioutil.ReadDir(dir)
	if nil != err {
		return err
	}

	for _, info := range infos {
		fName := info.Name()
		common.Log.Infof("uiMsg file : %s", fName)
		fPath := path.Join(dir, fName)
		if msgs, err := m.readUiMsgFile(fPath); nil != err {
			return err
		} else {
			g_uiMsg[fName] = msgs
		}
	}
	return nil
}

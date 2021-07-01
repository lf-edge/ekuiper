package plugin

import (
	kconf "github.com/emqx/kuiper/internal/conf"
	"gopkg.in/ini.v1"
	"io/ioutil"
	"path"
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
func (m *Manager) readUiMsgDir() error {
	gUimsg = make(map[string]*ini.File)
	confDir, err := kconf.GetConfLoc()
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

package plugins

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"io/ioutil"
	"path"
	"strings"
)

type (
	fileFunc struct {
		Name    string        `json:"name"`
		Example string        `json:"example"`
		Hint    *fileLanguage `json:"hint"`
	}
	fileFuncs struct {
		About   *fileAbout  `json:"about"`
		FiFuncs []*fileFunc `json:"functions"`
	}
	uiFunc struct {
		Name    string    `json:"name"`
		Example string    `json:"example"`
		Hint    *language `json:"hint"`
	}
	uiFuncs struct {
		About   *about    `json:"about"`
		UiFuncs []*uiFunc `json:"functions"`
	}
)

func newUiFuncs(fi *fileFuncs) *uiFuncs {
	if nil == fi {
		return nil
	}
	uis := new(uiFuncs)
	uis.About = newAbout(fi.About)
	for _, v := range fi.FiFuncs {
		ui := new(uiFunc)
		ui.Name = v.Name
		ui.Example = v.Example
		ui.Hint = newLanguage(v.Hint)
		uis.UiFuncs = append(uis.UiFuncs, ui)
	}
	return uis
}

var g_funcMetadata []*uiFuncs

func (m *Manager) readFuncMetaDir() error {
	confDir, err := common.GetConfLoc()
	if nil != err {
		return err
	}

	dir := path.Join(confDir, "functions")
	files, err := ioutil.ReadDir(dir)
	if nil != err {
		return err
	}
	for _, file := range files {
		fname := file.Name()
		if !strings.HasSuffix(fname, ".json") {
			continue
		}

		filePath := path.Join(dir, fname)
		fis := new(fileFuncs)
		err = common.ReadJsonUnmarshal(filePath, fis)
		if nil != err {
			return fmt.Errorf("fname:%s err:%v", fname, err)
		}
		if nil == fis.About {
			return fmt.Errorf("not found about of %s", filePath)
		} else {
			_, fis.About.Installed = m.registry.Get(FUNCTION, strings.TrimSuffix(fname, `.json`))
		}
		common.Log.Infof("funcMeta file : %s", fname)
		g_funcMetadata = append(g_funcMetadata, newUiFuncs(fis))
	}
	return nil
}

func (m *Manager) readFuncMetaFile(filePath string) error {
	fiName := path.Base(filePath)
	fis := new(fileFuncs)
	err := common.ReadJsonUnmarshal(filePath, fis)
	if nil != err {
		return fmt.Errorf("filePath:%s err:%v", filePath, err)
	}
	if nil == fis.About {
		return fmt.Errorf("not found about of %s", filePath)
	} else {
		_, fis.About.Installed = m.registry.Get(FUNCTION, strings.TrimSuffix(fiName, `.json`))
	}
	g_funcMetadata = append(g_funcMetadata, newUiFuncs(fis))
	common.Log.Infof("funcMeta file : %s", fiName)
	return nil
}
func GetFunctions() []*uiFuncs {
	return g_funcMetadata
}

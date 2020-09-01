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
		Control string        `json:"control"`
		Example string        `json:"example"`
		Hint    *fileLanguage `json:"hint"`
	}
	uiFunc struct {
		Name    string    `json:"name"`
		Control string    `json:"control"`
		Example string    `json:"example"`
		Hint    *language `json:"hint"`
	}
)

func newUiFunc(fi *fileFunc) *uiFunc {
	if nil == fi {
		return nil
	}
	ui := new(uiFunc)
	ui.Name = fi.Name
	ui.Control = fi.Control
	ui.Example = fi.Example
	ui.Hint = newLanguage(fi.Hint)
	return ui
}

var g_funcMetadata []*uiFunc

func readfuncMetaDir() error {
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
		var fis []*fileFunc
		err = common.ReadJsonUnmarshal(filePath, &fis)
		if nil != err {
			return fmt.Errorf("fname:%s err:%v", fname, err)
		}
		common.Log.Infof("funcMeta file : %s", fname)
		for _, fi := range fis {
			g_funcMetadata = append(g_funcMetadata, newUiFunc(fi))
		}
	}
	return nil
}

func readFuncMetaFile(filePath string) error {
	var fis []*fileFunc
	err := common.ReadJsonUnmarshal(filePath, &fis)
	if nil != err {
		return fmt.Errorf("filePath:%s err:%v", filePath, err)
	}
	for _, fi := range fis {
		g_funcMetadata = append(g_funcMetadata, newUiFunc(fi))
	}

	common.Log.Infof("funcMeta file : %s", path.Base(filePath))
	return nil
}
func GetFunctions() []*uiFunc {
	return g_funcMetadata
}

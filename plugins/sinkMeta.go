package plugins

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"io/ioutil"
	"path"
	"strings"
)

const (
	sink   = `sink`
	source = `source`
)

type (
	author struct {
		Name    string `json:"name"`
		Email   string `json:"email"`
		Company string `json:"company"`
		Website string `json:"website"`
	}
	fileLanguage struct {
		English string `json:"en_US"`
		Chinese string `json:"zh_CN"`
	}
	fileField struct {
		Name     string        `json:"name"`
		Default  interface{}   `json:"default"`
		Control  string        `json:"control"`
		Optional bool          `json:"optional"`
		Type     string        `json:"type"`
		Hint     *fileLanguage `json:"hint"`
		Label    *fileLanguage `json:"label"`
		Values   interface{}   `json:"values"`
	}
	fileAbout struct {
		Trial       bool          `json:"trial"`
		Installed   bool          `json:"installed"`
		Author      *author       `json:"author"`
		HelpUrl     *fileLanguage `json:"helpUrl"`
		Description *fileLanguage `json:"description"`
	}
	fileSink struct {
		About  *fileAbout   `json:"about"`
		Libs   []string     `json:"libs"`
		Fields []*fileField `json:"properties"`
	}
	language struct {
		English string `json:"en"`
		Chinese string `json:"zh"`
	}
	about struct {
		Trial       bool      `json:"trial"`
		Installed   bool      `json:"installed"`
		Author      *author   `json:"author"`
		HelpUrl     *language `json:"helpUrl"`
		Description *language `json:"description"`
	}
	field struct {
		Exist    bool        `json:"exist"`
		Name     string      `json:"name"`
		Default  interface{} `json:"default"`
		Type     string      `json:"type"`
		Control  string      `json:"control"`
		Optional bool        `json:"optional"`
		Values   interface{} `json:"values"`
		Hint     *language   `json:"hint"`
		Label    *language   `json:"label"`
	}

	uiSink struct {
		About  *about   `json:"about"`
		Libs   []string `json:"libs"`
		Fields []field  `json:"properties"`
	}
	uiSinks struct {
		CustomProperty map[string]*uiSink `json:"customProperty"`
		language       string
	}
)

func isInternalSink(fiName string) bool {
	internal := []string{`edgex.json`, `log.json`, `mqtt.json`, `nop.json`, `rest.json`}
	for _, v := range internal {
		if v == fiName {
			return true
		}
	}
	return false
}
func newLanguage(fi *fileLanguage) *language {
	if nil == fi {
		return nil
	}
	ui := new(language)
	ui.English = fi.English
	ui.Chinese = fi.Chinese
	return ui
}
func newField(fis []*fileField) (uis []field, err error) {
	for _, fi := range fis {
		if nil == fi {
			continue
		}
		ui := field{
			Name:     fi.Name,
			Type:     fi.Type,
			Control:  fi.Control,
			Optional: fi.Optional,
			Values:   fi.Values,
			Hint:     newLanguage(fi.Hint),
			Label:    newLanguage(fi.Label),
		}
		uis = append(uis, ui)
		switch t := fi.Default.(type) {
		case []map[string]interface{}:
			var auxFi []*fileField
			if err = common.MapToStruct(t, &auxFi); nil != err {
				return nil, err
			}
			if ui.Default, err = newField(auxFi); nil != err {
				return nil, err
			}
		default:
			ui.Default = fi.Default
		}
	}
	return uis, err
}
func newAbout(fi *fileAbout) *about {
	if nil == fi {
		return nil
	}
	ui := new(about)
	ui.Trial = fi.Trial
	ui.Installed = fi.Installed
	ui.Author = fi.Author
	ui.HelpUrl = newLanguage(fi.HelpUrl)
	ui.Description = newLanguage(fi.Description)
	return ui
}
func newUiSink(fi *fileSink) (*uiSink, error) {
	if nil == fi {
		return nil, nil
	}
	var err error
	ui := new(uiSink)
	ui.Libs = fi.Libs
	ui.About = newAbout(fi.About)
	ui.Fields, err = newField(fi.Fields)
	return ui, err
}

var g_sinkMetadata map[string]*uiSink //immutable
func (m *Manager) readSinkMetaDir() error {
	g_sinkMetadata = make(map[string]*uiSink)
	confDir, err := common.GetConfLoc()
	if nil != err {
		return err
	}

	dir := path.Join(confDir, "sinks")
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
		if err := m.readSinkMetaFile(filePath); nil != err {
			return err
		}
	}
	return nil
}

func (m *Manager) uninstalSink(name string) {
	if ui, ok := g_sinkMetadata[name+".json"]; ok {
		if nil != ui.About {
			ui.About.Installed = false
		}
	}
}
func (m *Manager) readSinkMetaFile(filePath string) error {
	finame := path.Base(filePath)
	pluginName := strings.TrimSuffix(finame, `.json`)
	metadata := new(fileSink)
	err := common.ReadJsonUnmarshal(filePath, metadata)
	if nil != err {
		return fmt.Errorf("filePath:%s err:%v", filePath, err)
	}
	if nil == metadata.About {
		return fmt.Errorf("not found about of %s", finame)
	} else if isInternalSink(finame) {
		metadata.About.Installed = true
	} else {
		_, metadata.About.Installed = m.registry.Get(SINK, pluginName)
	}
	g_sinkMetadata[finame], err = newUiSink(metadata)
	if nil != err {
		return err
	}
	common.Log.Infof("Loading metadata file for sink: %s", finame)
	return nil
}

func (us *uiSinks) setCustomProperty(pluginName string) error {
	fileName := pluginName + `.json`
	sinkMetadata := g_sinkMetadata
	data, ok := sinkMetadata[fileName]
	if !ok {
		return fmt.Errorf(`%s%s`, getMsg(us.language, sink, "not_found_plugin"), pluginName)
	}
	if 0 == len(us.CustomProperty) {
		us.CustomProperty = make(map[string]*uiSink)
	}
	us.CustomProperty[pluginName] = data
	return nil
}

func (us *uiSinks) hintWhenNewSink(pluginName string) (err error) {
	return us.setCustomProperty(pluginName)
}

func GetSinkMeta(pluginName, language string) (ptrSinkProperty *uiSinks, err error) {
	ptrSinkProperty = new(uiSinks)
	ptrSinkProperty.language = language
	err = ptrSinkProperty.hintWhenNewSink(pluginName)
	return ptrSinkProperty, err
}

type pluginfo struct {
	Name  string `json:"name"`
	About *about `json:"about"`
}

func GetSinks() (sinks []*pluginfo) {
	sinkMeta := g_sinkMetadata
	for fileName, v := range sinkMeta {
		node := new(pluginfo)
		node.Name = strings.TrimSuffix(fileName, `.json`)
		node.About = v.About
		i := 0
		for ; i < len(sinks); i++ {
			if node.Name <= sinks[i].Name {
				sinks = append(sinks, node)
				copy(sinks[i+1:], sinks[i:])
				sinks[i] = node
				break
			}
		}
		if len(sinks) == i {
			sinks = append(sinks, node)
		}
	}
	return sinks
}

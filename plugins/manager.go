package plugins

import (
	"archive/zip"
	"errors"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/api"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"plugin"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"
)

type Plugin struct {
	Name string `json:"name"`
	File string `json:"file"`
}

type PluginType int

const (
	SOURCE PluginType = iota
	SINK
	FUNCTION
)

const DELETED = "$deleted"

var (
	PluginTypes = []string{"sources", "sinks", "functions"}
	once        sync.Once
	singleton   *Manager
)

//Registry is append only because plugin cannot delete or reload. To delete a plugin, restart the server to reindex
type Registry struct {
	sync.RWMutex
	internal []map[string]string
}

func (rr *Registry) Store(t PluginType, name string, version string) {
	rr.Lock()
	rr.internal[t][name] = version
	rr.Unlock()
}

func (rr *Registry) List(t PluginType) []string {
	rr.RLock()
	result := rr.internal[t]
	rr.RUnlock()
	keys := make([]string, 0, len(result))
	for k := range result {
		keys = append(keys, k)
	}
	return keys
}

func (rr *Registry) Get(t PluginType, name string) (string, bool) {
	rr.RLock()
	result := rr.internal[t]
	rr.RUnlock()
	r, ok := result[name]
	return r, ok
}

//func (rr *Registry) Delete(t PluginType, value string) {
//	rr.Lock()
//	s := rr.internal[t]
//	for i, f := range s{
//		if f == value{
//			s[len(s)-1], s[i] = s[i], s[len(s)-1]
//			rr.internal[t] = s
//			break
//		}
//	}
//	rr.Unlock()
//}

var symbolRegistry = make(map[string]plugin.Symbol)
var mu sync.RWMutex

func getPlugin(t string, pt PluginType) (plugin.Symbol, error) {
	ut := ucFirst(t)
	ptype := PluginTypes[pt]
	key := ptype + "/" + t
	mu.Lock()
	defer mu.Unlock()
	var nf plugin.Symbol
	nf, ok := symbolRegistry[key]
	if !ok {
		m, err := NewPluginManager()
		if err != nil {
			return nil, fmt.Errorf("fail to initialize the plugin manager")
		}
		mod, err := getSoFilePath(m, pt, t)
		if err != nil {
			return nil, fmt.Errorf("cannot get the plugin file path: %v", err)
		}
		common.Log.Debugf("Opening plugin %s", mod)
		plug, err := plugin.Open(mod)
		if err != nil {
			return nil, fmt.Errorf("cannot open %s: %v", mod, err)
		}
		common.Log.Debugf("Successfully open plugin %s", mod)
		nf, err = plug.Lookup(ut)
		if err != nil {
			return nil, fmt.Errorf("cannot find symbol %s, please check if it is exported", t)
		}
		common.Log.Debugf("Successfully look-up plugin %s", mod)
		symbolRegistry[key] = nf
	}
	return nf, nil
}

func GetSource(t string) (api.Source, error) {
	nf, err := getPlugin(t, SOURCE)
	if err != nil {
		return nil, err
	}
	var s api.Source
	switch t := nf.(type) {
	case api.Source:
		s = t
	case func() api.Source:
		s = t()
	default:
		return nil, fmt.Errorf("exported symbol %s is not type of api.Source or function that return api.Source", t)
	}
	return s, nil
}

func GetSink(t string) (api.Sink, error) {
	nf, err := getPlugin(t, SINK)
	if err != nil {
		return nil, err
	}
	var s api.Sink
	switch t := nf.(type) {
	case api.Sink:
		s = t
	case func() api.Sink:
		s = t()
	default:
		return nil, fmt.Errorf("exported symbol %s is not type of api.Sink or function that return api.Sink", t)
	}
	return s, nil
}

func GetFunction(t string) (api.Function, error) {
	nf, err := getPlugin(t, FUNCTION)
	if err != nil {
		return nil, err
	}
	var s api.Function
	switch t := nf.(type) {
	case api.Function:
		s = t
	case func() api.Function:
		s = t()
	default:
		return nil, fmt.Errorf("exported symbol %s is not type of api.Function or function that return api.Function", t)
	}
	return s, nil
}

type Manager struct {
	pluginDir string
	etcDir    string
	registry  *Registry
}

func NewPluginManager() (*Manager, error) {
	var outerErr error
	once.Do(func() {
		dir, err := common.GetLoc("/plugins")
		if err != nil {
			outerErr = fmt.Errorf("cannot find plugins folder: %s", err)
			return
		}
		etcDir, err := common.GetLoc("/etc")
		if err != nil {
			outerErr = fmt.Errorf("cannot find etc folder: %s", err)
			return
		}

		plugins := make([]map[string]string, 3)
		for i := 0; i < 3; i++ {
			names, err := findAll(PluginType(i), dir)
			if err != nil {
				outerErr = fmt.Errorf("fail to find existing plugins: %s", err)
				return
			}
			plugins[i] = names
		}
		registry := &Registry{internal: plugins}

		singleton = &Manager{
			pluginDir: dir,
			etcDir:    etcDir,
			registry:  registry,
		}
		if err := singleton.readSourceMetaDir(); nil != err {
			common.Log.Errorf("readSourceMetaDir:%v", err)
		}
		if err := singleton.readSinkMetaDir(); nil != err {
			common.Log.Errorf("readSinkMetaDir:%v", err)
		}
		if err := singleton.readFuncMetaDir(); nil != err {
			common.Log.Errorf("readFuncMetaDir:%v", err)
		}
	})
	return singleton, outerErr
}

func findAll(t PluginType, pluginDir string) (result map[string]string, err error) {
	result = make(map[string]string)
	dir := path.Join(pluginDir, PluginTypes[t])
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return
	}

	for _, file := range files {
		baseName := filepath.Base(file.Name())
		if strings.HasSuffix(baseName, ".so") {
			n, v := parseName(baseName)
			result[n] = v
		}
	}
	return
}

func (m *Manager) List(t PluginType) (result []string, err error) {
	return m.registry.List(t), nil
}

func (m *Manager) Register(t PluginType, j *Plugin) error {
	name, uri := j.Name, j.File
	//Validation
	name = strings.Trim(name, " ")
	if name == "" {
		return fmt.Errorf("invalid name %s: should not be empty", name)
	}
	if !isValidUrl(uri) || !strings.HasSuffix(uri, ".zip") {
		return fmt.Errorf("invalid uri %s", uri)
	}

	if v, ok := m.registry.Get(t, name); ok {
		if v == DELETED {
			return fmt.Errorf("invalid name %s: the plugin is marked as deleted but Kuiper is not restarted for the change to take effect yet", name)
		} else {
			return fmt.Errorf("invalid name %s: duplicate", name)
		}
	}

	zipPath := path.Join(m.pluginDir, name+".zip")
	var unzipFiles []string
	//clean up: delete zip file and unzip files in error
	defer os.Remove(zipPath)
	//download
	err := downloadFile(zipPath, uri)
	if err != nil {
		return fmt.Errorf("fail to download file %s: %s", uri, err)
	}
	//unzip and copy to destination
	unzipFiles, version, err := m.install(t, name, zipPath)
	if err != nil {
		if t == SOURCE && len(unzipFiles) == 1 { //source that only copy so file
			os.Remove(unzipFiles[0])
		}
		return fmt.Errorf("fail to unzip file %s: %s", uri, err)
	}
	m.registry.Store(t, name, version)

	switch t {
	case SINK:
		if err := m.readSinkMetaFile(path.Join(m.etcDir, PluginTypes[t], name+`.json`)); nil != err {
			common.Log.Errorf("readSinkFile:%v", err)
		}
	case SOURCE:
		if err := m.readSourceMetaFile(path.Join(m.etcDir, PluginTypes[t], name+`.json`)); nil != err {
			common.Log.Errorf("readSourceFile:%v", err)
		}
	case FUNCTION:
		if err := m.readFuncMetaFile(path.Join(m.etcDir, PluginTypes[t], name+`.json`)); nil != err {
			common.Log.Errorf("readFuncFile:%v", err)
		}
	}
	return nil
}

func (m *Manager) Delete(t PluginType, name string, stop bool) error {
	name = strings.Trim(name, " ")
	if name == "" {
		return fmt.Errorf("invalid name %s: should not be empty", name)
	}
	soPath, err := getSoFilePath(m, t, name)
	if err != nil {
		return err
	}
	var results []string
	paths := []string{
		soPath,
	}
	switch t {
	case SOURCE:
		paths = append(paths, path.Join(m.etcDir, PluginTypes[t], name+".yaml"))
		m.uninstalSource(name)
	case SINK:
		m.uninstalSink(name)
	case FUNCTION:
		m.uninstalFunc(name)
	}

	for _, p := range paths {
		_, err := os.Stat(p)
		if err == nil {
			err = os.Remove(p)
			if err != nil {
				results = append(results, err.Error())
			}
		} else {
			results = append(results, fmt.Sprintf("can't find %s", p))
		}
	}

	if len(results) > 0 {
		return errors.New(strings.Join(results, "\n"))
	} else {
		m.registry.Store(t, name, DELETED)
		if stop {
			go func() {
				time.Sleep(1 * time.Second)
				os.Exit(100)
			}()
		}
		return nil
	}
}
func (m *Manager) Get(t PluginType, name string) (map[string]string, bool) {
	v, ok := m.registry.Get(t, name)
	if strings.HasPrefix(v, "v") {
		v = v[1:]
	}
	if ok {
		m := map[string]string{
			"name":    name,
			"version": v,
		}
		return m, ok
	}
	return nil, false
}

// Return the lowercase version of so name. It may be upper case in path.
func getSoFilePath(m *Manager, t PluginType, name string) (string, error) {
	v, ok := m.registry.Get(t, name)
	if !ok {
		return "", common.NewErrorWithCode(common.NOT_FOUND, fmt.Sprintf("invalid name %s: not exist", name))
	}

	soFile := name + ".so"
	if v != "" {
		soFile = fmt.Sprintf("%s@%s.so", name, v)
	}
	p := path.Join(m.pluginDir, PluginTypes[t], soFile)
	if _, err := os.Stat(p); err != nil {
		p = path.Join(m.pluginDir, PluginTypes[t], ucFirst(soFile))
	}
	if _, err := os.Stat(p); err != nil {
		return "", common.NewErrorWithCode(common.NOT_FOUND, fmt.Sprintf("cannot find .so file for plugin %s", name))
	}
	return p, nil
}

func (m *Manager) install(t PluginType, name string, src string) ([]string, string, error) {
	var filenames []string
	var tempPath = path.Join(m.pluginDir, "temp", PluginTypes[t], name)
	defer os.RemoveAll(tempPath)
	r, err := zip.OpenReader(src)
	if err != nil {
		return filenames, "", err
	}
	defer r.Close()

	soPrefix := regexp.MustCompile(fmt.Sprintf(`^((%s)|(%s))(@.*)?\.so$`, name, ucFirst(name)))
	var yamlFile, yamlPath, version string
	expFiles := 1
	if t == SOURCE {
		yamlFile = name + ".yaml"
		yamlPath = path.Join(m.etcDir, PluginTypes[t], yamlFile)
		expFiles = 2
	}
	needInstall := false
	for _, file := range r.File {
		fileName := file.Name
		if yamlFile == fileName {
			err = unzipTo(file, yamlPath)
			if err != nil {
				return filenames, "", err
			}
			filenames = append(filenames, yamlPath)
		} else if fileName == name+".json" {
			if err := unzipTo(file, path.Join(m.etcDir, PluginTypes[t], fileName)); nil != err {
				common.Log.Errorf("Failed to decompress the metadata %s file", fileName)
			}
		} else if soPrefix.Match([]byte(fileName)) {
			soPath := path.Join(m.pluginDir, PluginTypes[t], fileName)
			err = unzipTo(file, soPath)
			if err != nil {
				return filenames, "", err
			}
			filenames = append(filenames, soPath)
			_, version = parseName(fileName)
		} else { //unzip other files
			err = unzipTo(file, path.Join(tempPath, fileName))
			if err != nil {
				return filenames, "", err
			}
			if fileName == "install.sh" {
				needInstall = true
			}
		}
	}
	if len(filenames) != expFiles {
		return filenames, version, fmt.Errorf("invalid zip file: so file or conf file is missing")
	} else if needInstall {
		//run install script if there is
		spath := path.Join(tempPath, "install.sh")
		out, err := exec.Command("/bin/sh", spath).Output()
		if err != nil {
			return filenames, "", err
		} else {
			common.Log.Infof("install %s plugin %s log: %s", PluginTypes[t], name, out)
		}
	}
	return filenames, version, nil
}

func parseName(n string) (string, string) {
	result := strings.Split(n, ".so")
	result = strings.Split(result[0], "@")
	name := lcFirst(result[0])
	if len(result) > 1 {
		return name, result[1]
	}
	return name, ""
}

func unzipTo(f *zip.File, fpath string) error {
	_, err := os.Stat(fpath)
	if err == nil || !os.IsNotExist(err) {
		if err = os.Remove(fpath); err != nil {
			return fmt.Errorf("failed to delete file %s", fpath)
		}
	}

	if f.FileInfo().IsDir() {
		return fmt.Errorf("%s: not a file, but a directory", fpath)
	}

	if err := os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
		return err
	}

	outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
	if err != nil {
		return err
	}

	rc, err := f.Open()
	if err != nil {
		return err
	}

	_, err = io.Copy(outFile, rc)

	outFile.Close()
	rc.Close()
	return err
}

func isValidUrl(uri string) bool {
	pu, err := url.ParseRequestURI(uri)
	if err != nil {
		return false
	}

	switch pu.Scheme {
	case "http", "https":
		u, err := url.Parse(uri)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return false
		}
	case "file":
		if pu.Host != "" || pu.Path == "" {
			return false
		}
	default:
		return false
	}
	return true
}

func downloadFile(filepath string, uri string) error {
	common.Log.Infof("Start to download file %s\n", uri)
	u, err := url.ParseRequestURI(uri)
	if err != nil {
		return err
	}
	var src io.Reader
	switch u.Scheme {
	case "file":
		// deal with windows path
		if strings.Index(u.Path, ":") == 2 {
			u.Path = u.Path[1:]
		}
		common.Log.Debugf(u.Path)
		sourceFileStat, err := os.Stat(u.Path)
		if err != nil {
			return err
		}

		if !sourceFileStat.Mode().IsRegular() {
			return fmt.Errorf("%s is not a regular file", u.Path)
		}
		srcFile, err := os.Open(u.Path)
		if err != nil {
			return err
		}
		defer srcFile.Close()
		src = srcFile
	case "http", "https":
		// Get the data
		resp, err := http.Get(uri)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("cannot download the file with status: %s", resp.Status)
		}
		defer resp.Body.Close()
		src = resp.Body
	default:
		return fmt.Errorf("unsupported url scheme %s", u.Scheme)
	}
	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, src)
	return err
}

func ucFirst(str string) string {
	for i, v := range str {
		return string(unicode.ToUpper(v)) + str[i+1:]
	}
	return ""
}

func lcFirst(str string) string {
	for i, v := range str {
		return string(unicode.ToLower(v)) + str[i+1:]
	}
	return ""
}

package plugins

import (
	"archive/zip"
	"errors"
	"fmt"
	"github.com/emqx/kuiper/common"
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

func GetPlugin(t string, pt PluginType) (plugin.Symbol, error) {
	ut := ucFirst(t)
	ptype := PluginTypes[pt]
	key := ptype + "/" + t
	var nf plugin.Symbol
	nf, ok := symbolRegistry[key]
	if !ok {
		loc, err := common.GetLoc("/plugins/")
		if err != nil {
			return nil, fmt.Errorf("cannot find the plugins folder")
		}
		m, err := NewPluginManager()
		if err != nil {
			return nil, fmt.Errorf("fail to initialize the plugin manager")
		}
		soFile, err := getSoFileName(m, pt, t)
		if err != nil {
			return nil, fmt.Errorf("cannot get the plugin file name: %v", err)
		}
		mod := path.Join(loc, ptype, soFile)
		plug, err := plugin.Open(mod)
		if err != nil {
			return nil, fmt.Errorf("cannot open %s: %v", mod, err)
		}
		nf, err = plug.Lookup(ut)
		if err != nil {
			return nil, fmt.Errorf("cannot find symbol %s, please check if it is exported", t)
		}
		symbolRegistry[key] = nf
	}
	return nf, nil
}

type Manager struct {
	pluginDir string
	etcDir    string
	registry  *Registry
}

func NewPluginManager() (*Manager, error) {
	var err error
	once.Do(func() {
		dir, err := common.GetLoc("/plugins")
		if err != nil {
			err = fmt.Errorf("cannot find plugins folder: %s", err)
			return
		}
		etcDir, err := common.GetLoc("/etc")
		if err != nil {
			err = fmt.Errorf("cannot find etc folder: %s", err)
			return
		}

		plugins := make([]map[string]string, 3)
		for i := 0; i < 3; i++ {
			names, err := findAll(PluginType(i), dir)
			if err != nil {
				err = fmt.Errorf("fail to find existing plugins: %s", err)
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
	})
	return singleton, err
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
	return nil
}

func (m *Manager) Delete(t PluginType, name string, stop bool) error {
	name = strings.Trim(name, " ")
	if name == "" {
		return fmt.Errorf("invalid name %s: should not be empty", name)
	}
	soFile, err := getSoFileName(m, t, name)
	if err != nil {
		return err
	}
	var results []string
	paths := []string{
		path.Join(m.pluginDir, PluginTypes[t], soFile),
	}
	if t == SOURCE {
		paths = append(paths, path.Join(m.etcDir, PluginTypes[t], name+".yaml"))
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
	if ok {
		m := map[string]string{
			"name":    name,
			"version": v,
		}
		return m, ok
	}
	return nil, false
}

func getSoFileName(m *Manager, t PluginType, name string) (string, error) {
	v, ok := m.registry.Get(t, name)
	if !ok {
		return "", fmt.Errorf("invalid name %s: not exist", name)
	}

	soFile := ucFirst(name) + ".so"
	if v != "" {
		soFile = fmt.Sprintf("%s@v%s.so", ucFirst(name), v)
	}
	return soFile, nil
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

	soPrefix := regexp.MustCompile(fmt.Sprintf(`^%s(@v.*)?\.so$`, ucFirst(name)))
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
	result = strings.Split(result[0], "@v")
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
	_, err := url.ParseRequestURI(uri)
	if err != nil {
		return false
	}

	u, err := url.Parse(uri)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false
	}

	return true
}

func downloadFile(filepath string, url string) error {
	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("cannot download the file with status: %s", resp.Status)
	}
	defer resp.Body.Close()

	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
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

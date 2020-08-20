package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/benbjohnson/clock"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/go-yaml/yaml"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
)

const (
	logFileName   = "stream.log"
	etc_dir       = "/etc/"
	data_dir      = "/data/"
	log_dir       = "/log/"
	StreamConf    = "kuiper.yaml"
	KuiperBaseKey = "KuiperBaseKey"
	MetaKey       = "__meta"
)

var (
	Log          *logrus.Logger
	Config       *KuiperConf
	IsTesting    bool
	Clock        clock.Clock
	logFile      *os.File
	LoadFileType = "relative"
)

func LoadConf(confName string) ([]byte, error) {
	confDir, err := GetConfLoc()
	if err != nil {
		return nil, err
	}

	file := path.Join(confDir, confName)
	b, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	return b, nil
}

type tlsConf struct {
	Certfile string `yaml:"certfile"`
	Keyfile  string `yaml:"keyfile"`
}

type KuiperConf struct {
	Basic struct {
		Debug          bool     `yaml:"debug"`
		ConsoleLog     bool     `yaml:"consoleLog"`
		FileLog        bool     `yaml:"fileLog"`
		Port           int      `yaml:"port"`
		RestPort       int      `yaml:"restPort"`
		RestTls        *tlsConf `yaml:"restTls"`
		Prometheus     bool     `yaml:"prometheus"`
		PrometheusPort int      `yaml:"prometheusPort"`
	}
	Rule api.RuleOption
	Sink struct {
		CacheThreshold    int `yaml:"cacheThreshold"`
		CacheTriggerCount int `yaml:"cacheTriggerCount"`
	}
}

func init() {
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
	Log.Debugf("init with args %s", os.Args)
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "-test.") {
			IsTesting = true
			break
		}
	}
	if IsTesting {
		Log.Debugf("running in testing mode")
		Clock = clock.NewMock()
	} else {
		Clock = clock.New()
	}
}

func InitConf() {
	b, err := LoadConf(StreamConf)
	if err != nil {
		Log.Fatal(err)
	}

	kc := KuiperConf{
		Rule: api.RuleOption{
			LateTol:            1000,
			Concurrency:        1,
			BufferLength:       1024,
			CheckpointInterval: 300000, //5 minutes
		},
	}
	if err := yaml.Unmarshal(b, &kc); err != nil {
		Log.Fatal(err)
	} else {
		Config = &kc
	}

	if Config.Basic.Debug {
		Log.SetLevel(logrus.DebugLevel)
	}

	logDir, err := GetLoc(log_dir)
	if err != nil {
		Log.Fatal(err)
	}
	file := logDir + logFileName
	logFile, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		if Config.Basic.ConsoleLog {
			if Config.Basic.FileLog {
				mw := io.MultiWriter(os.Stdout, logFile)
				Log.SetOutput(mw)
			}
		} else {
			if Config.Basic.FileLog {
				Log.SetOutput(logFile)
			}
		}
	} else {
		fmt.Println("Failed to init log file settings...")
		Log.Infof("Failed to log to file, using default stderr.")
	}
}

func PrintMap(m map[string]string, buff *bytes.Buffer) {
	si := make([]string, 0, len(m))
	for s := range m {
		si = append(si, s)
	}
	sort.Strings(si)
	for _, s := range si {
		buff.WriteString(fmt.Sprintf("%s: %s\n", s, m[s]))
	}
}

func CloseLogger() {
	if logFile != nil {
		logFile.Close()
	}
}

func GetConfLoc() (string, error) {
	return GetLoc(etc_dir)
}

func GetDataLoc() (string, error) {
	if IsTesting {
		dataDir, err := GetLoc(data_dir)
		if err != nil {
			return "", err
		}
		d := path.Join(path.Dir(dataDir), "test")
		if _, err := os.Stat(d); os.IsNotExist(err) {
			err = os.MkdirAll(d, 0755)
			if err != nil {
				return "", err
			}
		}
		return d, nil
	}
	return GetLoc(data_dir)
}

func absolutePath(subdir string) (dir string, err error) {
	subdir = strings.TrimLeft(subdir, `/`)
	subdir = strings.TrimRight(subdir, `/`)
	switch subdir {
	case "etc":
		dir = "/etc/kuiper/"
		break
	case "data":
		dir = "/var/lib/kuiper/data/"
		break
	case "log":
		dir = "/var/log/kuiper/"
		break
	case "plugins":
		dir = "/var/lib/kuiper/plugins/"
		break
	}
	if 0 == len(dir) {
		return "", fmt.Errorf("no find such file : %s", subdir)
	}
	return dir, nil
}

/*
func GetLoc(subdir string) (string, error) {
	if base := os.Getenv(KuiperBaseKey); base != "" {
		Log.Infof("Specified Kuiper base folder at location %s.\n", base)
		dir = base
	} else {
		dir, err = filepath.Abs(filepath.Dir(os.Args[0]))
		if err != nil {
			return "", err
		}
		dir = filepath.Dir(dir)
	}

	dir = path.Join(dir, subdir)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return "", fmt.Errorf("conf dir not found : %s", dir)
	}
	return dir, nil
}
*/
func GetLoc(subdir string) (string, error) {
	if "relative" == LoadFileType {
		return relativePath(subdir)
	}

	if "absolute" == LoadFileType {
		return absolutePath(subdir)
	}
	return "", fmt.Errorf("Unrecognized loading method.")
}

func relativePath(subdir string) (dir string, err error) {
	dir, err = os.Getwd()
	if err != nil {
		return "", err
	}

	if base := os.Getenv(KuiperBaseKey); base != "" {
		Log.Infof("Specified Kuiper base folder at location %s.\n", base)
		dir = base
	}
	confDir := dir + subdir
	if _, err := os.Stat(confDir); os.IsNotExist(err) {
		lastdir := dir
		for len(dir) > 0 {
			dir = filepath.Dir(dir)
			if lastdir == dir {
				break
			}
			confDir = dir + subdir
			if _, err := os.Stat(confDir); os.IsNotExist(err) {
				lastdir = dir
				continue
			} else {
				//Log.Printf("Trying to load file from %s", confDir)
				return confDir, nil
			}
		}
	} else {
		//Log.Printf("Trying to load file from %s", confDir)
		return confDir, nil
	}

	return "", fmt.Errorf("conf dir not found, please set KuiperBaseKey program environment variable correctly.")
}

func ProcessPath(p string) (string, error) {
	if abs, err := filepath.Abs(p); err != nil {
		return "", nil
	} else {
		if _, err := os.Stat(abs); os.IsNotExist(err) {
			return "", err
		}
		return abs, nil
	}
}

/*********** Type Cast Utilities *****/
//TODO datetime type
func ToString(input interface{}) string {
	return fmt.Sprintf("%v", input)
}
func ToInt(input interface{}) (int, error) {
	switch t := input.(type) {
	case float64:
		return int(t), nil
	case int64:
		return int(t), nil
	case int:
		return t, nil
	default:
		return 0, fmt.Errorf("unsupported type %T of %[1]v", input)
	}
}

/*
*   Convert a map into a struct. The output parameter must be a pointer to a struct
*   The struct can have the json meta data
 */
func MapToStruct(input map[string]interface{}, output interface{}) error {
	// convert map to json
	jsonString, err := json.Marshal(input)
	if err != nil {
		return err
	}

	// convert json to struct
	return json.Unmarshal(jsonString, output)
}

func ConvertMap(s map[interface{}]interface{}) map[string]interface{} {
	r := make(map[string]interface{})
	for k, v := range s {
		switch t := v.(type) {
		case map[interface{}]interface{}:
			v = ConvertMap(t)
		case []interface{}:
			v = ConvertArray(t)
		}
		r[fmt.Sprintf("%v", k)] = v
	}
	return r
}

func ConvertArray(s []interface{}) []interface{} {
	r := make([]interface{}, len(s))
	for i, e := range s {
		switch t := e.(type) {
		case map[interface{}]interface{}:
			e = ConvertMap(t)
		case []interface{}:
			e = ConvertArray(t)
		}
		r[i] = e
	}
	return r
}

func SyncMapToMap(sm *sync.Map) map[string]interface{} {
	m := make(map[string]interface{})
	sm.Range(func(k interface{}, v interface{}) bool {
		m[fmt.Sprintf("%v", k)] = v
		return true
	})
	return m
}

func MapToSyncMap(m map[string]interface{}) *sync.Map {
	sm := new(sync.Map)
	for k, v := range m {
		sm.Store(k, v)
	}
	return sm
}

func ReadJsonUnmarshal(path string, ret interface{}) error {
	sliByte, err := ioutil.ReadFile(path)
	if nil != err {
		return err
	}
	err = json.Unmarshal(sliByte, ret)
	if nil != err {
		return err
	}
	return nil
}
func ReadYamlUnmarshal(path string, ret interface{}) error {
	sliByte, err := ioutil.ReadFile(path)
	if nil != err {
		return err
	}
	err = yaml.Unmarshal(sliByte, ret)
	if nil != err {
		return err
	}
	return nil
}
func WriteYamlMarshal(path string, data interface{}) error {
	y, err := yaml.Marshal(data)
	if nil != err {
		return err
	}
	return ioutil.WriteFile(path, y, 0666)
}

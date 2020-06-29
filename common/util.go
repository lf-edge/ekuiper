package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/benbjohnson/clock"
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
)

const (
	logFileName   = "stream.log"
	etc_dir       = "/etc/"
	data_dir      = "/data/"
	log_dir       = "/log/"
  abs_etc_dir    = "/etc/kuiper/"
  abs_data_dir   = "/var/lib/kuiper/data/"
  abs_log_dir    = "/var/log/kuiper/"
  abs_plugin_dir = "/var/lib/kuiper/plugins/"
	StreamConf    = "kuiper.yaml"
	KuiperBaseKey = "KuiperBaseKey"
	MetaKey       = "__meta"
)

var (
	Log       *logrus.Logger
	Config    *XStreamConf
	IsTesting bool
	Clock     clock.Clock
	logFile   *os.File
)

func LoadConf(confName string) ([]byte, error) {
	confDir, err := GetConfLoc()
	if err != nil {
		return nil, err
	}

	file := confDir + confName
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

type XStreamConf struct {
	Debug          bool     `yaml:"debug"`
	ConsoleLog     bool     `yaml:"consoleLog"`
	FileLog        bool     `yaml:"fileLog"`
	Port           int      `yaml:"port"`
	RestPort       int      `yaml:"restPort"`
	RestTls        *tlsConf `yaml:"restTls"`
	Prometheus     bool     `yaml:"prometheus"`
	PrometheusPort int      `yaml:"prometheusPort"`
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
	var cfg map[string]XStreamConf
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		Log.Fatal(err)
	}

	if c, ok := cfg["basic"]; !ok {
		Log.Fatal("No basic config in kuiper.yaml")
	} else {
		Config = &c
	}

	if Config.Debug {
		Log.SetLevel(logrus.DebugLevel)
	}

	logDir, err := GetLoc(log_dir)
	if err != nil {
		Log.Fatal(err)
	}
	file := logDir + logFileName
	logFile, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		if Config.ConsoleLog {
			if Config.FileLog {
				mw := io.MultiWriter(os.Stdout, logFile)
				Log.SetOutput(mw)
			}
		} else {
			if Config.FileLog {
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
	return GetLoc(data_dir)
}

func getAbsPath(subdir string) (dir string, err error) {
  switch true {
  case strings.Contains(subdir, "etc"):
    dir = abs_etc_dir
    break
  case strings.Contains(subdir, "data"):
    dir = abs_data_dir
    break
  case strings.Contains(subdir, "log"):
    dir = abs_log_dir
    break
  case strings.Contains(subdir, "plugins"):
    dir = abs_plugin_dir
    break
  }
  if _, err = os.Stat(dir); os.IsExist(err) {
    return dir, nil
  }
    return "", err
}

func GetLoc(subdir string) (string, error) {
  dir,err := getAbsPath(subdir)
  if err == nil{
    return dir,err
  }
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

func GetAndCreateDataLoc(dir string) (string, error) {
	dataDir, err := GetDataLoc()
	if err != nil {
		return "", err
	}
	d := path.Join(path.Dir(dataDir), dir)
	if _, err := os.Stat(d); os.IsNotExist(err) {
		err = os.MkdirAll(d, 0755)
		if err != nil {
			return "", err
		}
	}
	return d, nil
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

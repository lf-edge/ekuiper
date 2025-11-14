// Copyright 2021-2025 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package conf

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pingcap/failpoint"
)

func init() {
	PathConfig.LoadFileType = "relative"
	PathConfig.Dirs = AbsoluteMapping
}

type PathConfigure struct {
	LoadFileType string
	Dirs         map[string]string
}

const (
	etcDir        = "etc"
	dataDir       = "data"
	logDir        = "log"
	pluginsDir    = "plugins"
	metricsDir    = "metrics"
	KuiperBaseKey = "KuiperBaseKey"
)

var (
	PathConfig      PathConfigure
	AbsoluteMapping = map[string]string{
		etcDir:     "/etc/kuiper",
		dataDir:    "/var/lib/kuiper/data",
		logDir:     "/var/log/kuiper",
		pluginsDir: "/var/lib/kuiper/plugins",
	}
)

func GetConfLoc() (s string, err error) {
	defer func() {
		failpoint.Inject("GetConfLocErr", func() {
			err = errors.New("GetConfLocErr")
		})
	}()
	return GetLoc(etcDir)
}

func GetLogLoc() (string, error) {
	return GetLoc(logDir)
}

func GetMetricsLoc() (string, error) {
	logPath, err := GetLogLoc()
	if err != nil {
		return "", err
	}
	return filepath.Join(logPath, metricsDir), nil
}

func GetDataLoc() (s string, err error) {
	defer func() {
		failpoint.Inject("GetDataLocErr", func() {
			err = errors.New("GetDataLocErr")
		})
	}()
	if IsTesting {
		dataDir, err := GetLoc(dataDir)
		if err != nil {
			return "", err
		}
		dir := "test"
		if TestId != "" {
			dir = TestId
		}
		d := path.Join(dataDir, dir)
		if _, err := os.Stat(d); os.IsNotExist(err) {
			err = os.MkdirAll(d, 0o755)
			if err != nil {
				return "", err
			}
		}
		return d, nil
	}
	return GetLoc(dataDir)
}

func GetPluginsLoc() (s string, err error) {
	defer func() {
		failpoint.Inject("GetPluginsLocErr", func() {
			err = errors.New("GetPluginsLocErr")
		})
	}()
	return GetLoc(pluginsDir)
}

func absolutePath(loc string) (dir string, err error) {
	for relDir, absoluteDir := range PathConfig.Dirs {
		if strings.HasPrefix(loc, relDir) {
			dir = strings.Replace(loc, relDir, absoluteDir, 1)
			break
		}
	}
	if len(dir) == 0 {
		return "", fmt.Errorf("location %s is not allowed for absolute mode", loc)
	}
	return dir, nil
}

// GetLoc subdir must be a relative path
func GetLoc(subdir string) (string, error) {
	if subdir == "" {
		return os.Getenv(KuiperBaseKey), nil
	}
	switch PathConfig.LoadFileType {
	case "relative":
		return relativePath(subdir)
	case "absolute":
		return absolutePath(subdir)
	default:
		return "", fmt.Errorf("unrecognized loading method")
	}
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
	confDir := path.Join(dir, subdir)
	if _, err := os.Stat(confDir); os.IsNotExist(err) {
		lastdir := dir
		for len(dir) > 0 {
			dir = filepath.Dir(dir)
			if lastdir == dir {
				break
			}
			confDir = path.Join(dir, subdir)
			if _, err := os.Stat(confDir); os.IsNotExist(err) {
				lastdir = dir
				continue
			}
			// Log.Printf("Trying to load file from %s", confDir)
			return confDir, nil
		}
	} else {
		// Log.Printf("Trying to load file from %s", confDir)
		return confDir, nil
	}

	return "", fmt.Errorf("dir %s not found, please make sure it is created.", confDir)
}

func InitMetricsFolder() error {
	mPath, err := GetMetricsLoc()
	if err != nil {
		return err
	}
	if _, err = os.Stat(mPath); os.IsNotExist(err) {
		err := os.Mkdir(mPath, 0o755)
		if err != nil {
			return err
		}
	}
	return nil
}

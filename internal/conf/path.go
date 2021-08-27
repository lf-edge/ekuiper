// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const (
	etcDir          = "etc"
	dataDir         = "data"
	logDir          = "log"
	pluginsDir      = "plugins"
	KuiperBaseKey   = "KuiperBaseKey"
	KuiperSyslogKey = "KuiperSyslogKey"
)

var LoadFileType = "relative"
var AbsoluteMapping = map[string]string{
	etcDir:     "/etc/kuiper",
	dataDir:    "/var/lib/kuiper/data",
	logDir:     "/var/log/kuiper",
	pluginsDir: "/var/lib/kuiper/plugins",
}

func GetConfLoc() (string, error) {
	return GetLoc(etcDir)
}

func GetDataLoc() (string, error) {
	if IsTesting {
		dataDir, err := GetLoc(dataDir)
		if err != nil {
			return "", err
		}
		d := path.Join(dataDir, "test")
		if _, err := os.Stat(d); os.IsNotExist(err) {
			err = os.MkdirAll(d, 0755)
			if err != nil {
				return "", err
			}
		}
		return d, nil
	}
	return GetLoc(dataDir)
}

func GetPluginsLoc() (string, error) {
	return GetLoc(pluginsDir)
}

func absolutePath(loc string) (dir string, err error) {
	for relDir, absoluteDir := range AbsoluteMapping {
		if strings.HasPrefix(loc, relDir) {
			dir = strings.Replace(loc, relDir, absoluteDir, 1)
			break
		}
	}
	if 0 == len(dir) {
		return "", fmt.Errorf("location %s is not allowed for absolue mode", loc)
	}
	return dir, nil
}

// GetLoc subdir must be a relative path
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
			} else {
				//Log.Printf("Trying to load file from %s", confDir)
				return confDir, nil
			}
		}
	} else {
		//Log.Printf("Trying to load file from %s", confDir)
		return confDir, nil
	}

	return "", fmt.Errorf("dir %s not found, please make sure it is created.", confDir)
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

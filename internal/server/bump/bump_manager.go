// Copyright 2024 EMQ Technologies Co., Ltd.
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

package bump

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/filex"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

const (
	currentVersion = 4
	bumpTable      = "eKuiperMeta_bump_version"
)

var GlobalBumpManager *BumpManager

type BumpManager struct {
	Version int
	store   kv.KeyValue
}

func InitBumpManager() error {
	s, err := store.GetKV(bumpTable)
	failpoint.Inject("initManagerError", func() {
		err = errors.New("initManagerError")
	})
	if err != nil {
		return fmt.Errorf("init bump manager failed, err:%v", err)
	}
	b := &BumpManager{
		store: s,
	}
	GlobalBumpManager = b
	GlobalBumpManager.Version, err = loadVersionFromStorage()
	if err == nil {
		conf.Log.Infof("start bump version: %v", GlobalBumpManager.Version)
	}
	return err
}

func loadVersionFromStorage() (int, error) {
	var ver int
	got, err := GlobalBumpManager.store.Get("version", &ver)
	failpoint.Inject("loadVersionError", func() {
		err = errors.New("loadVersionError")
	})
	if err != nil {
		return 0, fmt.Errorf("init bump manager failed, err:%v", err)
	}
	if got {
		return ver, nil
	}
	return 0, nil
}

func BumpToCurrentVersion(dataDir string) error {
	for i := GlobalBumpManager.Version; i <= currentVersion; i++ {
		switch i {
		case 0:
			if err := bumpFrom0To1(dataDir); err != nil {
				return err
			}
			if err := storeGlobalVersion(1); err != nil {
				return err
			}
			GlobalBumpManager.Version = 1
		case 1:
			if err := bumpFrom1TO2(); err != nil {
				return err
			}
			if err := storeGlobalVersion(2); err != nil {
				return err
			}
			GlobalBumpManager.Version = 2
		case 2:
			if err := bumpFrom2TO3(); err != nil {
				return err
			}
			if err := storeGlobalVersion(3); err != nil {
				return err
			}
			GlobalBumpManager.Version = 3
		case 3:
			if err := bumpFrom3TO4(); err != nil {
				return err
			}
			if err := storeGlobalVersion(4); err != nil {
				return err
			}
			GlobalBumpManager.Version = 4
		}
	}
	conf.Log.Infof("success bump version: %v", currentVersion)
	return nil
}

func bumpFrom0To1(dir string) error {
	if err := migrateDataIntoStorage(dir, "sources"); err != nil {
		return err
	}
	if err := migrateDataIntoStorage(dir, "connections"); err != nil {
		return err
	}
	if err := migrateDataIntoStorage(dir, "sinks"); err != nil {
		return err
	}
	return nil
}

func migrateDataIntoStorage(dataDir, confType string) error {
	dir := path.Join(dataDir, confType)
	files, err := os.ReadDir(dir)
	failpoint.Inject("migrateReadError", func(val failpoint.Value) {
		x := ""
		switch val.(int) {
		case 1:
			x = "sources"
		case 2:
			x = "sinks"
		case 3:
			x = "connections"
		}
		if x == confType {
			err = errors.New("migrateReadError")
		}
	})
	if err != nil {
		return err
	}
	for _, file := range files {
		fname := file.Name()
		if !strings.HasSuffix(fname, ".yaml") {
			continue
		}
		pluginTyp := strings.TrimSuffix(fname, ".yaml")
		filePath := filepath.Join(dir, fname)
		confKeys := make(map[string]map[string]interface{})
		err = filex.ReadYamlUnmarshal(filePath, &confKeys)
		failpoint.Inject("migrateUnmarshalErr", func() {
			err = errors.New("migrateUnmarshalErr")
		})
		if err != nil {
			return err
		}
		for confKey, confData := range confKeys {
			err = conf.WriteCfgIntoKVStorage(confType, pluginTyp, confKey, confData)
			failpoint.Inject("migrateWriteErr", func() {
				err = errors.New("migrateWriteErr")
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func storeGlobalVersion(ver int) error {
	err := GlobalBumpManager.store.Set("version", ver)
	return err
}

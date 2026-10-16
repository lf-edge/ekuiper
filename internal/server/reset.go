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

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

var (
	isResetRequested atomic.Bool
	onResetStop      = stopEKuiper
)

func RequestReset() bool {
	return isResetRequested.CompareAndSwap(false, true)
}

func IsResetting() bool {
	return isResetRequested.Load()
}

func resetHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	if conf.Config != nil {
		if conf.Config.Store.Type != "" && conf.Config.Store.Type != "sqlite" {
			handleError(w, fmt.Errorf("reset is not supported for store type %q, only sqlite is currently supported", conf.Config.Store.Type), "Reset rejected", logger)
			return
		}
		if conf.Config.Store.ExtStateType != "" && conf.Config.Store.ExtStateType != "sqlite" {
			handleError(w, fmt.Errorf("reset is not supported for extStateType %q, only sqlite is currently supported", conf.Config.Store.ExtStateType), "Reset rejected", logger)
			return
		}
	}

	force := r.URL.Query().Get("force") == "true"
	if !isDegraded && !force {
		handleError(w, errors.New("reset in normal mode requires force=true"), "Reset rejected", logger)
		return
	}

	dataDir, err := conf.GetDataLoc()
	if err != nil || !isSafeDataDir(dataDir) {
		handleError(w, fmt.Errorf("invalid data directory path: %v", err), "Reset rejected", logger)
		return
	}

	if !RequestReset() {
		handleErrorWithStatus(w, errors.New("reset is already in progress"), "Reset rejected", http.StatusConflict, logger)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"message": "reset process initiated, eKuiper is resetting and restarting...",
	})

	go func() {
		time.Sleep(100 * time.Millisecond)
		conf.Log.Info("eKuiper stopping for factory reset restart")
		onResetStop()
	}()
}

func resolveRealPath(dir string) (string, error) {
	if dir == "" {
		return "", errors.New("empty directory")
	}
	clean := filepath.Clean(dir)
	if clean == "." || clean == ".." {
		return "", errors.New("invalid directory")
	}
	abs, err := filepath.Abs(clean)
	if err != nil {
		return "", err
	}
	realPath, err := filepath.EvalSymlinks(abs)
	if err != nil {
		if os.IsNotExist(err) {
			parent := filepath.Dir(abs)
			if realParent, pErr := filepath.EvalSymlinks(parent); pErr == nil {
				return filepath.Join(realParent, filepath.Base(abs)), nil
			}
			return abs, nil
		}
		return "", err
	}
	return realPath, nil
}

func pathsOverlap(p1, p2 string) bool {
	if p1 == p2 {
		return true
	}
	sep := string(filepath.Separator)
	return strings.HasPrefix(p1, p2+sep) || strings.HasPrefix(p2, p1+sep)
}

func isSafeDataDir(dir string, extraConfDirs ...string) bool {
	realData, err := resolveRealPath(dir)
	if err != nil {
		return false
	}
	systemDirs := []string{"/", "/etc", "/bin", "/sbin", "/usr", "/var", "/root", "/home", "/dev", "/proc", "/sys"}
	for _, s := range systemDirs {
		if realData == s {
			return false
		}
	}
	cwd, err := os.Getwd()
	if err == nil {
		realCwd, _ := filepath.EvalSymlinks(cwd)
		if realData == realCwd || realData == filepath.Dir(realCwd) {
			return false
		}
	}
	confDir, _ := conf.GetConfLoc()
	if confDir != "" {
		if realConf, err := resolveRealPath(confDir); err == nil {
			if pathsOverlap(realData, realConf) {
				return false
			}
		}
	}
	pluginsDir, _ := conf.GetPluginsLoc()
	if pluginsDir != "" {
		if realPlugins, err := resolveRealPath(pluginsDir); err == nil {
			if pathsOverlap(realData, realPlugins) {
				return false
			}
		}
	}
	for _, d := range extraConfDirs {
		if d != "" {
			if realExtra, err := resolveRealPath(d); err == nil {
				if pathsOverlap(realData, realExtra) {
					return false
				}
			}
		}
	}
	return true
}

func cleanRuntimeData() error {
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		return err
	}
	confDir, _ := conf.GetConfLoc()
	return cleanRuntimeDataLoc(dataDir, confDir)
}

func cleanRuntimeDataLoc(dataDir, confDir string) error {
	if !isSafeDataDir(dataDir, confDir) {
		return fmt.Errorf("unsafe data directory: %s", dataDir)
	}

	realDataDir, err := resolveRealPath(dataDir)
	if err != nil {
		return fmt.Errorf("failed to resolve absolute data directory %s: %w", dataDir, err)
	}

	var realConfDir string
	if confDir != "" {
		if p, err := resolveRealPath(confDir); err == nil {
			realConfDir = p
		}
	}

	var errs []error

	// 1. Remove all files and directories in realDataDir EXCEPT init.json
	entries, err := os.ReadDir(realDataDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to read data directory %s: %w", realDataDir, err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "init.json" {
			continue
		}
		target := filepath.Join(realDataDir, name)
		if err := os.RemoveAll(target); err != nil {
			errs = append(errs, fmt.Errorf("failed to remove %s: %w", target, err))
		}
	}

	// 2. Remove only initialized* marker files in realConfDir if realConfDir != realDataDir (keep all other etc configs intact)
	if realConfDir != "" && realConfDir != realDataDir {
		entries, err := os.ReadDir(realConfDir)
		if err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("failed to read etc directory %s: %w", realConfDir, err))
		} else {
			for _, entry := range entries {
				if !entry.IsDir() && strings.HasPrefix(entry.Name(), "initialized") {
					target := filepath.Join(realConfDir, entry.Name())
					if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
						errs = append(errs, fmt.Errorf("failed to remove %s: %w", target, err))
					}
				}
			}
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

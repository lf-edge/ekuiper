// Copyright 2023 EMQ Technologies Co., Ltd.
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

package cgroup

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/lf-edge/ekuiper/internal/conf"
)

// GetMemoryLimit attempts to retrieve the cgroup memory limit for the current
// process.
func GetMemoryLimit() (limit uint64, err error) {
	return getCgroupMemLimit("/")
}

// See http://man7.org/linux/man-pages/man5/proc.5.html for `mountinfo` format.
func getCgroupDetails(mountInfoPath string, cRoot string, controller string) (string, int, error) {
	//nolint:gosec
	info, err := os.Open(mountInfoPath)
	if err != nil {
		return "", 0, err
	}
	defer func() {
		err := info.Close()
		if err != nil {
			conf.Log.Errorf("close mountInfoPath, err:%v", err)
		}
	}()

	scanner := bufio.NewScanner(info)
	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())
		if len(fields) < 10 {
			continue
		}

		ver, ok := detectCgroupVersion(fields, controller)
		if ok {
			mountPoint := string(fields[4])
			if ver == 2 {
				return mountPoint, ver, nil
			}
			// It is possible that the controller mount and the cgroup path are not the same (both are relative to the NS root).
			// So start with the mount and construct the relative path of the cgroup.
			// To test:
			//  1、start a docker to run unit test or tidb-server
			//   > docker run -it --cpus=8 --memory=8g --name test --rm ubuntu:18.04 bash
			//
			//  2、change the limit when the container is running
			//	docker update --cpus=8 <containers>
			nsRelativePath := string(fields[3])
			if !strings.Contains(nsRelativePath, "..") {
				// We don't expect to see err here ever but in case that it happens
				// the best action is to ignore the line and hope that the rest of the lines
				// will allow us to extract a valid path.
				if relPath, err := filepath.Rel(nsRelativePath, cRoot); err == nil {
					return filepath.Join(mountPoint, relPath), ver, nil
				}
			}
		}
	}

	return "", 0, fmt.Errorf("failed to detect cgroup root mount and version")
}

// root is always "/" in the production. It will be changed for testing.
func getCgroupMemLimit(root string) (limit uint64, err error) {
	path, err := detectControlPath(filepath.Join(root, procPathCGroup), "memory")
	if err != nil {
		return 0, err
	}

	if path == "" {
		conf.Log.Warn("no cgroup memory controller detected")
		return 0, nil
	}

	mount, ver, err := getCgroupDetails(filepath.Join(root, procPathMountInfo), path, "memory")
	if err != nil {
		return 0, err
	}

	switch ver {
	case 1:
		// cgroupv1
		limit, err = detectMemLimitInV1(filepath.Join(root, mount))
	case 2:
		// cgroupv2
		limit, err = detectMemLimitInV2(filepath.Join(root, mount, path))
	default:
		limit, err = 0, fmt.Errorf("detected unknown cgroup version index: %d", ver)
	}

	return limit, err
}

func detectMemLimitInV1(cRoot string) (limit uint64, err error) {
	return detectMemStatValue(cRoot, cgroupV1MemStat, cgroupV1MemLimitStatKey, 1)
}

func detectMemLimitInV2(cRoot string) (limit uint64, err error) {
	return readInt64Value(cRoot, cgroupV2MemLimit, 2)
}

func detectMemStatValue(cRoot, filename, key string, cgVersion int) (value uint64, err error) {
	statFilePath := filepath.Join(cRoot, filename)
	//nolint:gosec
	stat, err := os.Open(statFilePath)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = stat.Close()
	}()

	scanner := bufio.NewScanner(stat)
	for scanner.Scan() {
		fields := bytes.Fields(scanner.Bytes())
		if len(fields) != 2 || string(fields[0]) != key {
			continue
		}

		trimmed := string(bytes.TrimSpace(fields[1]))
		value, err = strconv.ParseUint(trimmed, 10, 64)
		if err != nil {
			return 0, err
		}

		return value, nil
	}
	return 0, fmt.Errorf("failed to find expected memory stat %q for cgroup v%d in %s", key, cgVersion, filename)
}

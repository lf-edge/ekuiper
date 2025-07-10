// Copyright 2025 EMQ Technologies Co., Ltd.
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
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func bumpTo5() error {
	etcDir, err := conf.GetConfLoc()
	if err != nil {
		return err
	}
	dataDir, err := conf.GetDataLoc()
	if err != nil {
		return err
	}
	targetFolders := []string{"sources", "sinks", "functions"}
	for _, tf := range targetFolders {
		if err := copyFiles(filepath.Join(etcDir, tf), filepath.Join(dataDir, tf)); err != nil {
			return fmt.Errorf("bump 5 failed, err:%v", err.Error())
		}
	}
	return nil
}

func copyFiles(srcDir, dstDir string) error {
	srcFiles, err := os.ReadDir(srcDir)
	if err != nil {
		return fmt.Errorf("failed to read source directory: %v", err)
	}
	for _, file := range srcFiles {
		if file.IsDir() {
			continue
		}
		srcPath := filepath.Join(srcDir, file.Name())
		dstPath := filepath.Join(dstDir, file.Name())
		if _, err := os.Stat(dstPath); err == nil {
			fmt.Printf("Skipping existing file: %s\n", file.Name())
			continue
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("failed to check destination file: %v", err)
		}

		srcFile, err := os.Open(srcPath)
		if err != nil {
			return fmt.Errorf("failed to open source file: %v", err)
		}
		defer srcFile.Close()

		dstFile, err := os.Create(dstPath)
		if err != nil {
			return fmt.Errorf("failed to create destination file: %v", err)
		}
		defer dstFile.Close()
		if _, err := io.Copy(dstFile, srcFile); err != nil {
			return fmt.Errorf("failed to copy file content: %v", err)
		}
	}
	return nil
}

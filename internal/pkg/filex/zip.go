// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

package filex

import (
	"archive/zip"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/pingcap/failpoint"
)

func UnzipTo(f *zip.File, folder, name string) (err error) {
	defer func() {
		failpoint.Inject("UnzipToErr", func() {
			err = errors.New("UnzipToErr")
		})
	}()

	// Ensure destination folder exists (restore previous behavior)
	if err := os.MkdirAll(folder, os.ModePerm); err != nil {
		return err
	}

	// Open the folder as a sandboxed root first to prevent path traversal
	root, err := os.OpenRoot(folder)
	if err != nil {
		return err
	}
	defer root.Close()

	if f.FileInfo().IsDir() {
		// Make Folder using sandboxed root
		if err := root.Mkdir(name, os.ModePerm); err != nil && !os.IsExist(err) {
			return err
		}
		return nil
	}

	// For files, create parent directory if needed
	dir := filepath.Dir(name)
	if dir != "." && dir != "" {
		if err := root.Mkdir(dir, os.ModePerm); err != nil && !os.IsExist(err) {
			return err
		}
	}

	// Remove existing file if present
	_ = root.Remove(name)

	outFile, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
	if err != nil {
		return err
	}
	defer outFile.Close()

	rc, err := f.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	_, err = io.Copy(outFile, rc)
	return err
}

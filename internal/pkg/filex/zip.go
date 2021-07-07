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

package filex

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func UnzipTo(f *zip.File, fpath string) error {
	_, err := os.Stat(fpath)

	if f.FileInfo().IsDir() {
		// Make Folder
		if _, err := os.Stat(fpath); os.IsNotExist(err) {
			if err := os.MkdirAll(fpath, os.ModePerm); err != nil {
				return err
			}
		}
		return nil
	}

	if err == nil || !os.IsNotExist(err) {
		if err = os.RemoveAll(fpath); err != nil {
			return fmt.Errorf("failed to delete file %s", fpath)
		}
	}
	if _, err := os.Stat(filepath.Dir(fpath)); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return err
		}
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

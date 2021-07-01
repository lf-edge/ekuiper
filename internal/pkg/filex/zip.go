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

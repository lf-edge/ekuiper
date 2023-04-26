// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

package file

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/io/mock"
	"github.com/lf-edge/ekuiper/pkg/api"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestJsonFile(t *testing.T) {
	path, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	meta := map[string]interface{}{
		"file": filepath.Join(path, "test", "test.json"),
	}
	exp := []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"id": float64(1), "name": "John Doe"}, meta, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"id": float64(2), "name": "Jane Doe"}, meta, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"id": float64(3), "name": "John Smith"}, meta, time.Now()),
	}
	p := map[string]interface{}{
		"path": filepath.Join(path, "test"),
	}
	r := &FileSource{}
	err = r.Configure("test.json", p)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	mock.TestSourceOpen(r, exp, t)
}

func TestJsonFolder(t *testing.T) {
	path, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	moveToFolder := filepath.Join(path, "test", "moveTo")
	exp := []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"id": float64(1), "name": "John Doe", "height": 1.82}, map[string]interface{}{"file": filepath.Join(path, "test", "json", "f1.json")}, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"id": float64(2), "name": "Jane Doe", "height": 1.65}, map[string]interface{}{"file": filepath.Join(path, "test", "json", "f1.json")}, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"id": float64(3), "name": "Will Doe", "height": 1.76}, map[string]interface{}{"file": filepath.Join(path, "test", "json", "f2.json")}, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"id": float64(4), "name": "Dude Doe", "height": 1.92}, map[string]interface{}{"file": filepath.Join(path, "test", "json", "f3.json")}, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"id": float64(5), "name": "Jane Doe", "height": 1.72}, map[string]interface{}{"file": filepath.Join(path, "test", "json", "f3.json")}, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"id": float64(6), "name": "John Smith", "height": 2.22}, map[string]interface{}{"file": filepath.Join(path, "test", "json", "f3.json")}, time.Now()),
	}
	p := map[string]interface{}{
		"path":            filepath.Join(path, "test"),
		"actionAfterRead": 2,
		"moveTo":          moveToFolder,
	}
	r := &FileSource{}
	err = r.Configure("json", p)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	mock.TestSourceOpen(r, exp, t)
	// wait for the move to finish
	time.Sleep(100 * time.Millisecond)
	files, err := os.ReadDir(moveToFolder)
	if err != nil {
		t.Error(err)
	}
	if len(files) != 3 {
		t.Errorf("expect 3 files in moveTo folder, but got %d", len(files))
	}
	for _, f := range files {
		os.Rename(filepath.Join(moveToFolder, f.Name()), filepath.Join(path, "test", "json", f.Name()))
	}
}

func TestCSVFolder(t *testing.T) {
	// Move test files to temp folder
	path, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	testFolder := filepath.Join(path, "test", "csvTemp")
	err = os.MkdirAll(testFolder, 0755)
	if err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(filepath.Join(path, "test", "csv"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		err = copy(filepath.Join(path, "test", "csv", f.Name()), filepath.Join(testFolder, f.Name()))
		if err != nil {
			t.Fatal(err)
		}
	}
	// Start testing
	exp := []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"@": "#", "id": "1", "ts": "1670170500", "value": "161.927872"}, map[string]interface{}{"file": filepath.Join(path, "test", "csvTemp", "a.csv")}, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"@": "#", "id": "2", "ts": "1670170900", "value": "176"}, map[string]interface{}{"file": filepath.Join(path, "test", "csvTemp", "a.csv")}, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"id": "33", "ts": "1670270500", "humidity": "89"}, map[string]interface{}{"file": filepath.Join(path, "test", "csvTemp", "b.csv")}, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"id": "44", "ts": "1670270900", "humidity": "76"}, map[string]interface{}{"file": filepath.Join(path, "test", "csvTemp", "b.csv")}, time.Now()),
	}
	p := map[string]interface{}{
		"fileType":         "csv",
		"path":             filepath.Join(path, "test"),
		"actionAfterRead":  1,
		"hasHeader":        true,
		"delimiter":        "\t",
		"ignoreStartLines": 3,
		"ignoreEndLines":   1,
	}
	r := &FileSource{}
	err = r.Configure("csvTemp", p)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	mock.TestSourceOpen(r, exp, t)
	// wait for file deleted takes effect
	time.Sleep(100 * time.Millisecond)
	files, err = os.ReadDir(testFolder)
	if err != nil {
		t.Error(err)
	}
	if len(files) != 0 {
		t.Errorf("expect 0 files in csvTemp folder, but got %d", len(files))
	}
}

func copy(src, dst string) error {
	sourceFileStat, err := os.Stat(src)
	if err != nil {
		return err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}

	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destination.Close()
	_, err = io.Copy(destination, source)
	return err
}

func TestCSVFile(t *testing.T) {
	path, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	exp := []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"ns": "@", "id": "id", "ts": "ts", "number": "value"}, map[string]interface{}{"file": filepath.Join(path, "test", "csv", "a.csv")}, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"ns": "#", "id": "1", "ts": "1670170500", "number": "161.927872"}, map[string]interface{}{"file": filepath.Join(path, "test", "csv", "a.csv")}, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"ns": "#", "id": "2", "ts": "1670170900", "number": "176"}, map[string]interface{}{"file": filepath.Join(path, "test", "csv", "a.csv")}, time.Now()),
	}
	p := map[string]interface{}{
		"fileType":         "csv",
		"path":             filepath.Join(path, "test", "csv"),
		"delimiter":        "\t",
		"ignoreStartLines": 3,
		"ignoreEndLines":   1,
		"columns":          []string{"ns", "id", "ts", "number"},
	}
	r := &FileSource{}
	err = r.Configure("a.csv", p)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	mock.TestSourceOpen(r, exp, t)
}

func TestJsonLines(t *testing.T) {
	path, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	meta := map[string]interface{}{
		"file": filepath.Join(path, "test", "test.lines"),
	}
	exp := []api.SourceTuple{
		api.NewDefaultSourceTuple(map[string]interface{}{"id": float64(1), "name": "John Doe"}, meta, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"id": float64(2), "name": "Jane Doe"}, meta, time.Now()),
		api.NewDefaultSourceTuple(map[string]interface{}{"id": float64(3), "name": "John Smith"}, meta, time.Now()),
	}
	p := map[string]interface{}{
		"path":     filepath.Join(path, "test"),
		"fileType": "lines",
	}
	r := &FileSource{}
	err = r.Configure("test.lines", p)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	mock.TestSourceOpen(r, exp, t)
}

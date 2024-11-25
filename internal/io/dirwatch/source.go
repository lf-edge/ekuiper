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

package dirwatch

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type FileDirSource struct {
	config        *FileDirSourceConfig
	taskCh        chan *FileSourceTask
	fileContentCh chan []byte

	watcher    *fsnotify.Watcher
	rewindMeta *FileDirSourceRewindMeta
	wg         *sync.WaitGroup
}

func (f *FileDirSource) Subscribe(ctx api.StreamContext, ingest api.TupleIngest, ingestError api.ErrorIngest) error {
	f.wg.Add(2)
	go f.startHandleTask(ctx, ingest, ingestError)
	go f.handleFileDirNotify(ctx)
	return f.readDirFile()
}

type FileDirSourceConfig struct {
	Path             string   `json:"path"`
	AllowedExtension []string `json:"allowedExtension"`
}

func (f *FileDirSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	c := &FileDirSourceConfig{}
	if err := cast.MapToStruct(configs, c); err != nil {
		return err
	}
	f.config = c
	f.taskCh = make(chan *FileSourceTask, 1024)
	f.fileContentCh = make(chan []byte, 1024)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	f.watcher = watcher
	f.rewindMeta = &FileDirSourceRewindMeta{}
	if err := f.watcher.Add(f.config.Path); err != nil {
		return err
	}
	conf.Log.Infof("start to watch %v, rule:%v", f.config.Path, ctx.GetRuleId())
	f.wg = &sync.WaitGroup{}
	return nil
}

func (f *FileDirSource) Close(ctx api.StreamContext) error {
	f.watcher.Close()
	f.wg.Wait()
	return nil
}

func (f *FileDirSource) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	return nil
}

func (f *FileDirSource) handleFileDirNotify(ctx api.StreamContext) {
	defer f.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-f.watcher.Events:
			if !ok {
				return
			}
			switch {
			case event.Has(fsnotify.Write):
				f.taskCh <- &FileSourceTask{
					name:     event.Name,
					taskType: WriteFile,
				}
			case event.Has(fsnotify.Create):
				f.taskCh <- &FileSourceTask{
					name:     event.Name,
					taskType: CreateFile,
				}
			}
		case err, ok := <-f.watcher.Errors:
			if !ok {
				return
			}
			ctx.GetLogger().Errorf("dirwatch err:%v", err.Error())
		}
	}
}

func (f *FileDirSource) startHandleTask(ctx api.StreamContext, ingest api.TupleIngest, ingestError api.ErrorIngest) {
	defer f.wg.Done()
	for {
		select {
		case task := <-f.taskCh:
			switch task.taskType {
			case WriteFile, CreateFile:
				f.ingestFileContent(ctx, task.name, ingest, ingestError)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (f *FileDirSource) ingestFileContent(ctx api.StreamContext, fileName string, ingest api.TupleIngest, ingestError api.ErrorIngest) {
	if !checkFileExtension(fileName, f.config.AllowedExtension) {
		return
	}
	willRead, modifyTime, err := f.checkFileRead(fileName)
	if err != nil {
		ingestError(ctx, err)
		return
	}
	if willRead {
		c, err := os.ReadFile(fileName)
		if err != nil {
			ingestError(ctx, fmt.Errorf("read file %s err: %v", fileName, err))
			return
		}
		message := make(map[string]interface{})
		message["filename"] = filepath.Base(fileName)
		message["modifyTime"] = modifyTime.Unix()
		message["content"] = c
		f.updateRewindMeta(fileName, modifyTime)
		ingest(ctx, message, nil, time.Now())
	}
}

func (f *FileDirSource) checkFileRead(fileName string) (bool, time.Time, error) {
	fInfo, err := os.Stat(fileName)
	if err != nil {
		return false, time.Time{}, err
	}
	if fInfo.IsDir() {
		return false, time.Time{}, fmt.Errorf("%s is a directory", fileName)
	}
	fTime := fInfo.ModTime()
	if fTime.After(f.rewindMeta.LastModifyTime) {
		return true, fTime, nil
	}
	return false, time.Time{}, nil
}

func (f *FileDirSource) updateRewindMeta(_ string, modifyTime time.Time) {
	if modifyTime.After(f.rewindMeta.LastModifyTime) {
		f.rewindMeta.LastModifyTime = modifyTime
	}
}

func (f *FileDirSource) GetOffset() (any, error) {
	c, err := json.Marshal(f.rewindMeta)
	return string(c), err
}

func (f *FileDirSource) Rewind(offset any) error {
	c, ok := offset.(string)
	if !ok {
		return fmt.Errorf("fileDirSource rewind failed")
	}
	f.rewindMeta = &FileDirSourceRewindMeta{}
	if err := json.Unmarshal([]byte(c), f.rewindMeta); err != nil {
		return err
	}
	return nil
}

func (f *FileDirSource) ResetOffset(input map[string]any) error {
	return fmt.Errorf("FileDirSource ResetOffset not supported")
}

func (f *FileDirSource) readDirFile() error {
	entries, err := os.ReadDir(f.config.Path)
	if err != nil {
		return err
	}
	files := make(FileWithTimeSlice, 0)
	for _, entry := range entries {
		if !entry.IsDir() {
			fileName := entry.Name()
			info, err := entry.Info()
			if err != nil {
				return err
			}
			files = append(files, FileWithTime{name: fileName, modifyTime: info.ModTime()})
		}
	}
	sort.Sort(files)
	for _, file := range files {
		f.taskCh <- &FileSourceTask{name: filepath.Join(f.config.Path, file.name), taskType: CreateFile}
	}
	return nil
}

type FileWithTime struct {
	name       string
	modifyTime time.Time
}

type FileWithTimeSlice []FileWithTime

func (f FileWithTimeSlice) Len() int {
	return len(f)
}

func (f FileWithTimeSlice) Less(i, j int) bool {
	return f[i].modifyTime.Before(f[j].modifyTime)
}

func (f FileWithTimeSlice) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

type FileSourceTask struct {
	name         string
	previousName string
	taskType     FileTaskType
}

type FileTaskType int

const (
	CreateFile FileTaskType = iota
	WriteFile
)

type FileDirSourceRewindMeta struct {
	LastModifyTime time.Time `json:"lastModifyTime"`
}

func checkFileExtension(name string, allowedExtension []string) bool {
	if len(allowedExtension) < 1 {
		return true
	}
	fileExt := strings.TrimPrefix(filepath.Ext(name), ".")
	for _, ext := range allowedExtension {
		if fileExt == ext {
			return true
		}
	}
	return false
}

func GetSource() api.Source {
	return &FileDirSource{}
}

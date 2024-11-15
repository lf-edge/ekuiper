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

package fileDir

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/lf-edge/ekuiper/contract/v2/api"

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

type FileDirSourceConfig struct {
	Path string `json:"path"`
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
	f.rewindMeta = &FileDirSourceRewindMeta{
		SentFile: map[string]time.Time{},
	}
	if err := f.watcher.Add(f.config.Path); err != nil {
		return err
	}
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

func (f *FileDirSource) Subscribe(ctx api.StreamContext, ingest api.BytesIngest, ingestError api.ErrorIngest) error {
	go f.startHandleTask(ctx, ingest, ingestError)
	go f.handleFileDirNotify()
	return nil
}

func (f *FileDirSource) handleFileDirNotify() {
	f.wg.Add(1)
	defer f.wg.Done()
	for {
		select {
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
			case event.Has(fsnotify.Remove):
				f.taskCh <- &FileSourceTask{
					name:     event.Name,
					taskType: RemoveFile,
				}
			}
		case err, ok := <-f.watcher.Errors:
			if !ok {
				return
			}
			log.Println("error:", err)
		}
	}
}

func (f *FileDirSource) startHandleTask(ctx api.StreamContext, ingest api.BytesIngest, ingestError api.ErrorIngest) {
	f.wg.Add(1)
	defer f.wg.Done()
	for {
		select {
		case task := <-f.taskCh:
			switch task.taskType {
			case WriteFile:
				f.ingestFileContent(ctx, task.name, ingest, ingestError)
			case CreateFile:
				f.ingestFileContent(ctx, task.name, ingest, ingestError)
			case RemoveFile:
				f.handleRemove(task.name)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (f *FileDirSource) ingestFileContent(ctx api.StreamContext, fileName string, ingest api.BytesIngest, ingestError api.ErrorIngest) {
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
		ingest(ctx, c, nil, time.Now())
		f.updateRewindMeta(fileName, modifyTime)
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
	modifyTime, ok := f.rewindMeta.SentFile[fileName]
	if !ok {
		return true, fTime, nil
	}
	if fInfo.ModTime().After(modifyTime) {
		return true, fTime, nil
	}
	return false, time.Time{}, nil
}

func (f *FileDirSource) handleRemove(name string) {
	delete(f.rewindMeta.SentFile, name)
}

func (f *FileDirSource) updateRewindMeta(fileName string, modifyTime time.Time) {
	f.rewindMeta.SentFile[fileName] = modifyTime
}

func (f *FileDirSource) GetOffset() (any, error) {
	c, err := json.Marshal(f.rewindMeta)
	return c, err
}

func (f *FileDirSource) Rewind(offset any) error {
	c, ok := offset.([]byte)
	if !ok {
		return nil
	}
	f.rewindMeta = &FileDirSourceRewindMeta{
		SentFile: map[string]time.Time{},
	}
	if err := json.Unmarshal(c, f.rewindMeta); err != nil {
		return nil
	}
	return nil
}

func (f *FileDirSource) ResetOffset(input map[string]any) error {
	//TODO implement me
	panic("implement me")
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
	RemoveFile
)

type FileDirSourceRewindMeta struct {
	// filename -> modify time
	SentFile map[string]time.Time
}

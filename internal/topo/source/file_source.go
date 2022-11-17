// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package source

import (
	"errors"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/filex"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"os"
	"path"
	"path/filepath"
	"time"
)

type FileType string

const (
	JSON_TYPE FileType = "json"
)

var fileTypes = map[FileType]bool{
	JSON_TYPE: true,
}

type FileSourceConfig struct {
	FileType   FileType `json:"fileType"`
	Path       string   `json:"path"`
	Interval   int      `json:"interval"`
	RetainSize int      `json:"$retainSize"`
}

// The BATCH to load data from file at once
type FileSource struct {
	file   string
	config *FileSourceConfig
}

func (fs *FileSource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Close file source")
	// do nothing
	return nil
}

func (fs *FileSource) Configure(fileName string, props map[string]interface{}) error {
	cfg := &FileSourceConfig{}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.FileType == "" {
		return errors.New("missing or invalid property fileType, must be 'json'")
	}
	if _, ok := fileTypes[cfg.FileType]; !ok {
		return fmt.Errorf("invalid property fileType: %s", cfg.FileType)
	}
	if cfg.Path == "" {
		return errors.New("missing property Path")
	}
	if fileName == "" {
		return errors.New("file name must be specified")
	}
	if !filepath.IsAbs(cfg.Path) {
		cfg.Path, err = conf.GetLoc(cfg.Path)
		if err != nil {
			return fmt.Errorf("invalid path %s", cfg.Path)
		}
	}
	if fileName != "/$$TEST_CONNECTION$$" {
		fs.file = path.Join(cfg.Path, fileName)

		if fi, err := os.Stat(fs.file); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("file %s not exist", fs.file)
			} else if !fi.Mode().IsRegular() {
				return fmt.Errorf("file %s is not a regular file", fs.file)
			}
		}
	}
	fs.config = cfg
	return nil
}

func (fs *FileSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	err := fs.Load(ctx, consumer)
	if err != nil {
		errCh <- err
		return
	}
	if fs.config.Interval > 0 {
		ticker := time.NewTicker(time.Millisecond * time.Duration(fs.config.Interval))
		logger := ctx.GetLogger()
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				logger.Debugf("Load file source again at %v", conf.GetNowInMilli())
				err := fs.Load(ctx, consumer)
				if err != nil {
					errCh <- err
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}
}

func (fs *FileSource) Load(ctx api.StreamContext, consumer chan<- api.SourceTuple) error {
	switch fs.config.FileType {
	case JSON_TYPE:
		ctx.GetLogger().Debugf("Start to load from file %s", fs.file)
		resultMap := make([]map[string]interface{}, 0)
		err := filex.ReadJsonUnmarshal(fs.file, &resultMap)
		if err != nil {
			return fmt.Errorf("loaded %s, check error %s", fs.file, err)
		}
		ctx.GetLogger().Debug("Sending tuples")
		if fs.config.RetainSize > 0 && fs.config.RetainSize < len(resultMap) {
			resultMap = resultMap[(len(resultMap) - fs.config.RetainSize):]
			ctx.GetLogger().Debug("Sending tuples for retain size %d", fs.config.RetainSize)
		}
		for _, m := range resultMap {
			select {
			case consumer <- api.NewDefaultSourceTuple(m, nil):
				// do nothing
			case <-ctx.Done():
				return nil
			}
		}
		// Send EOF if retain size not set
		if fs.config.RetainSize == 0 {
			select {
			case consumer <- api.NewDefaultSourceTuple(nil, nil):
				// do nothing
			case <-ctx.Done():
				return nil
			}
		}
		ctx.GetLogger().Debug("All tuples sent")
		return nil
	}
	return fmt.Errorf("invalid file type %s", fs.config.FileType)
}

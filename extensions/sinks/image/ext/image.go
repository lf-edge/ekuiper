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

package image

import (
	"bytes"
	"context"
	"fmt"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type c struct {
	Path        string `json:"path"`
	ImageFormat string `json:"imageFormat"`
	MaxAge      int    `json:"maxAge"`
	MaxCount    int    `json:"maxCount"`
}

type imageSink struct {
	c      *c
	cancel context.CancelFunc
}

func (m *imageSink) Configure(props map[string]interface{}) error {
	conf := &c{
		MaxAge:   72,
		MaxCount: 1000,
	}
	err := cast.MapToStruct(props, conf)
	if err != nil {
		return err
	}
	if conf.Path == "" {
		return fmt.Errorf("path is required")
	}
	if conf.ImageFormat != "png" && conf.ImageFormat != "jpeg" {
		return fmt.Errorf("%s image type is not currently supported", conf.ImageFormat)
	}
	m.c = conf
	return nil
}

func (m *imageSink) Open(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Debug("Opening image sink")

	if _, err := os.Stat(m.c.Path); os.IsNotExist(err) {
		if err := os.MkdirAll(m.c.Path, os.ModePerm); nil != err {
			return fmt.Errorf("fail to open image sink for %v", err)
		}
	}

	t := time.NewTicker(time.Duration(3) * time.Minute)
	exeCtx, cancel := ctx.WithCancel()
	m.cancel = cancel
	go func() {
		defer t.Stop()
		for {
			select {
			case <-t.C:
				m.delFile(logger)
			case <-exeCtx.Done():
				logger.Info("image sink done")
				return
			}
		}
	}()
	return nil
}

func (m *imageSink) delFile(logger api.Logger) error {
	logger.Debugf("deleting images")
	dirEntries, err := os.ReadDir(m.c.Path)
	if nil != err || 0 == len(dirEntries) {
		logger.Error("read dir fail")
		return err
	}

	files := make([]os.FileInfo, 0, len(dirEntries))
	for _, entry := range dirEntries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		files = append(files, info)
	}

	pos := m.c.MaxCount
	delTime := time.Now().Add(time.Duration(0-m.c.MaxAge) * time.Hour)
	for i := 0; i < len(files); i++ {
		for j := i + 1; j < len(files); j++ {
			if files[i].ModTime().Before(files[j].ModTime()) {
				files[i], files[j] = files[j], files[i]
			}
		}
		if files[i].ModTime().Before(delTime) && i < pos {
			pos = i
			break
		}
	}
	logger.Debugf("pos is %d, and file len is %d", pos, len(files))
	for i := pos; i < len(files); i++ {
		fname := files[i].Name()
		logger.Debugf("try to delete %s", fname)
		if strings.HasSuffix(fname, m.c.ImageFormat) {
			fpath := filepath.Join(m.c.Path, fname)
			os.Remove(fpath)
		}
	}
	return nil
}

func (m *imageSink) getSuffix() string {
	now := time.Now()
	year, month, day := now.Date()
	hour, minute, second := now.Clock()
	nsecond := now.Nanosecond()
	return fmt.Sprintf(`%d-%d-%d_%d-%d-%d-%d`, year, month, day, hour, minute, second, nsecond)
}

func (m *imageSink) saveFile(b []byte, fpath string) error {
	reader := bytes.NewReader(b)
	switch m.c.ImageFormat {
	case "png":
		img, err := png.Decode(reader)
		if err != nil {
			return err
		}
		fp, err := os.Create(fpath)
		if nil != err {
			return err
		}
		defer fp.Close()
		err = png.Encode(fp, img)
		if err != nil {
			os.Remove(fpath)
			return err
		}
	case "jpeg":
		img, err := jpeg.Decode(reader)
		if err != nil {
			return err
		}
		fp, err := os.Create(fpath)
		if nil != err {
			return err
		}
		defer fp.Close()
		err = jpeg.Encode(fp, img, nil)
		if err != nil {
			os.Remove(fpath)
			return err
		}
	default:
		return fmt.Errorf("unsupported format %s", m.c.ImageFormat)
	}
	return nil
}

func (m *imageSink) saveFiles(images map[string]interface{}) error {
	for k, v := range images {
		image, ok := v.([]byte)
		if !ok {
			return fmt.Errorf("found none bytes data %v for path %s", image, k)
		}
		suffix := m.getSuffix()
		fname := fmt.Sprintf(`%s%s.%s`, k, suffix, m.c.ImageFormat)
		fpath := filepath.Join(m.c.Path, fname)
		err := m.saveFile(image, fpath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *imageSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	switch v := item.(type) {
	case []map[string]interface{}:
		var outer error
		for _, vm := range v {
			err := m.saveFiles(vm)
			if err != nil {
				outer = err
				logger.Error(err)
			}
		}
		return outer
	case map[string]interface{}:
		return m.saveFiles(v)
	default:
		return fmt.Errorf("image sink receive invalid data %v", item)
	}
}

func (m *imageSink) Close(ctx api.StreamContext) error {
	if m.cancel != nil {
		m.cancel()
	}
	return m.delFile(ctx.GetLogger())
}

func GetSink() api.Sink {
	return &imageSink{}
}

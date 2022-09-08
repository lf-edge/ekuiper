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

package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
	"image/jpeg"
	"image/png"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type imageSink struct {
	path     string
	format   string
	maxAge   int
	maxCount int
	cancel   context.CancelFunc
}

func (m *imageSink) Configure(props map[string]interface{}) error {
	if i, ok := props["imageFormat"]; ok {
		if i, ok := i.(string); ok {
			if "png" != i && "jpeg" != i {
				return fmt.Errorf("%s image type is not currently supported", i)
			}
			m.format = i
		}
	} else {
		return fmt.Errorf("Field not found format.")
	}

	if i, ok := props["path"]; ok {
		if i, ok := i.(string); ok {
			m.path = i
		} else {
			return fmt.Errorf("%s image type is not supported", i)
		}
	} else {
		return fmt.Errorf("Field not found path.")
	}

	m.maxAge = 72
	if i, ok := props["maxAge"]; ok {
		if i, ok := i.(int); ok {
			m.maxAge = i
		}
	}
	m.maxCount = 1000
	if i, ok := props["maxCount"]; ok {
		if i, ok := i.(int); ok {
			m.maxCount = i
		}
	}
	return nil
}

func (m *imageSink) Open(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Debug("Opening image sink")

	if _, err := os.Stat(m.path); os.IsNotExist(err) {
		if err := os.MkdirAll(m.path, os.ModePerm); nil != err {
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
	files, err := ioutil.ReadDir(m.path)
	if nil != err || 0 == len(files) {
		return err
	}

	pos := m.maxCount
	delTime := time.Now().Add(time.Duration(0-m.maxAge) * time.Hour)
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

	for i := pos; i < len(files); i++ {
		fname := files[i].Name()
		if strings.HasSuffix(fname, m.format) {
			fpath := filepath.Join(m.path, fname)
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
	fp, err := os.Create(fpath)
	if nil != err {
		return err
	}
	defer fp.Close()
	if "png" == m.format {
		if img, err := png.Decode(reader); nil != err {
			return err
		} else if err = png.Encode(fp, img); nil != err {
			return err
		}
	} else if "jpeg" == m.format {
		if img, err := jpeg.Decode(reader); nil != err {
			return err
		} else if err = jpeg.Encode(fp, img, nil); nil != err {
			return err
		}
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
		fname := fmt.Sprintf(`%s%s.%s`, k, suffix, m.format)
		fpath := filepath.Join(m.path, fname)
		m.saveFile(image, fpath)
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
		fmt.Errorf("image sink receive invalid data %v", item)
	}
	return nil
}

func (m *imageSink) Close(ctx api.StreamContext) error {
	if m.cancel != nil {
		m.cancel()
	}
	return m.delFile(ctx.GetLogger())
}

func Image() api.Sink {
	return &imageSink{}
}

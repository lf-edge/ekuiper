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
	"errors"
	"fmt"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
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

func (m *imageSink) Provision(_ api.StreamContext, configs map[string]any) error {
	conf := &c{
		MaxAge:   72,
		MaxCount: 1000,
	}
	err := cast.MapToStruct(configs, conf)
	if err != nil {
		return err
	}
	if conf.Path == "" {
		return errors.New("path is required")
	}
	if conf.ImageFormat != "png" && conf.ImageFormat != "jpeg" {
		return fmt.Errorf("invalid image format: %s", conf.ImageFormat)
	}
	if conf.MaxAge < 0 {
		return fmt.Errorf("invalid max age: %d", conf.MaxAge)
	}
	if conf.MaxCount < 0 {
		return fmt.Errorf("invalid max count: %d", conf.MaxCount)
	}
	m.c = conf
	return nil
}

func (m *imageSink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	if _, err := os.Stat(m.c.Path); os.IsNotExist(err) {
		if err := os.MkdirAll(m.c.Path, os.ModePerm); nil != err {
			sch(api.ConnectionDisconnected, err.Error())
			return fmt.Errorf("fail to open image sink for %v", err)
		}
	}

	t := timex.GetTicker(time.Duration(3) * time.Minute)
	exeCtx, cancel := ctx.WithCancel()
	m.cancel = cancel
	go func() {
		defer t.Stop()
		for {
			select {
			case <-t.C:
				m.delFile(ctx.GetLogger())
			case <-exeCtx.Done():
				ctx.GetLogger().Info("image sink done")
				return
			}
		}
	}()
	sch(api.ConnectionConnected, "")
	return nil
}

func (m *imageSink) delFile(logger api.Logger) error {
	logger.Debugf("deleting images")
	dirEntries, err := os.ReadDir(m.c.Path)
	if nil != err || len(dirEntries) == 0 {
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

func (m *imageSink) Collect(ctx api.StreamContext, item api.MessageTuple) error {
	return m.saveFiles(item.ToMap())
}

func (m *imageSink) CollectList(ctx api.StreamContext, items api.MessageTupleList) error {
	// TODO handle partial errors
	items.RangeOfTuples(func(_ int, tuple api.MessageTuple) bool {
		err := m.saveFiles(tuple.ToMap())
		if err != nil {
			ctx.GetLogger().Error(err)
		}
		return true
	})
	return nil
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

var _ api.TupleCollector = &imageSink{}

// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
	"bufio"
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"io"
	"os"
	"sync"
	"time"
)

type sinkConf struct {
	Interval int    `json:"interval"`
	Path     string `json:"path"`
}

type fileSink struct {
	c *sinkConf

	mux    sync.Mutex
	file   *os.File
	writer io.Writer
}

func (m *fileSink) Configure(props map[string]interface{}) error {
	c := &sinkConf{
		Interval: 1000,
		Path:     "cache",
	}
	if err := cast.MapToStruct(props, c); err != nil {
		return err
	}
	if c.Interval <= 0 {
		return fmt.Errorf("interval must be positive")
	}
	if c.Path == "" {
		return fmt.Errorf("path must be set")
	}
	m.c = c
	return nil
}

func (m *fileSink) Open(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Debug("Opening file sink")
	var (
		f   *os.File
		err error
	)
	if _, err = os.Stat(m.c.Path); os.IsNotExist(err) {
		_, err = os.Create(m.c.Path)
	}
	f, err = os.OpenFile(m.c.Path, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return fmt.Errorf("fail to open file sink for %v", err)
	}
	m.file = f
	if m.c.Interval > 0 {
		m.writer = bufio.NewWriter(f)
		t := time.NewTicker(time.Duration(m.c.Interval) * time.Millisecond)
		go func() {
			defer t.Stop()
			for {
				select {
				case <-t.C:
					m.mux.Lock()
					err := m.writer.(*bufio.Writer).Flush()
					if err != nil {
						logger.Errorf("file sink fails to flush with error %s.", err)
					}
					m.mux.Unlock()
				case <-ctx.Done():
					logger.Info("file sink done")
					return
				}
			}
		}()
	} else {
		m.writer = f
	}

	return nil
}

func (m *fileSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	logger.Debugf("file sink receive %s", item)
	if v, _, err := ctx.TransformOutput(item); err == nil {
		logger.Debugf("file sink transform data %s", v)
		m.mux.Lock()
		m.writer.Write(v)
		m.writer.Write([]byte("\n"))
		m.mux.Unlock()
	} else {
		return fmt.Errorf("file sink transform data error: %v", err)
	}
	return nil
}

func (m *fileSink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing file sink")
	if m.file != nil {
		ctx.GetLogger().Infof("File sync before close")
		if m.c.Interval > 0 {
			ctx.GetLogger().Infof("flush at close")
			m.writer.(*bufio.Writer).Flush()
		}
		m.file.Sync()
		return m.file.Close()
	}
	return nil
}

func File() api.Sink {
	return &fileSink{}
}

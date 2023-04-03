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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

type sinkConf struct {
	Interval  int      `json:"interval"`
	Path      string   `json:"path"`
	FileType  FileType `json:"fileType"`
	HasHeader bool     `json:"hasHeader"`
	Delimiter string   `json:"delimiter"`
	Format    string   `json:"format"` // only use for validation; transformation is done in sink_node
}

type fileSink struct {
	c *sinkConf
	// If firstLine is true, it means the file is newly created and the first line is not written yet.
	// Do not write line feed for the first line.
	firstLine bool
	mux       sync.Mutex
	file      *os.File
	writer    io.Writer
	hook      writerHooks
}

func (m *fileSink) Configure(props map[string]interface{}) error {
	c := &sinkConf{
		Interval: 1000,
		Path:     "cache",
		FileType: LINES_TYPE,
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
	if c.FileType != JSON_TYPE && c.FileType != CSV_TYPE && c.FileType != LINES_TYPE {
		return fmt.Errorf("fileType must be one of json, csv or lines")
	}
	if c.FileType == CSV_TYPE {
		if c.Format != message.FormatDelimited {
			return fmt.Errorf("format must be delimited when fileType is csv")
		}
		if c.Delimiter == "" {
			conf.Log.Warnf("delimiter is not set, use default ','")
			c.Delimiter = ","
		}
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
	m.firstLine = true
	switch m.c.FileType {
	case JSON_TYPE:
		m.hook = &jsonWriterHooks{}
	case CSV_TYPE:
		m.hook = &csvWriterHooks{}
	case LINES_TYPE:
		m.hook = &linesWriterHooks{}
	}
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
	// extract header for csv
	if m.c.FileType == CSV_TYPE && m.c.HasHeader && m.hook.Header() == nil {
		var header []string
		switch v := item.(type) {
		case map[string]interface{}:
			header = make([]string, len(v))
			for k := range item.(map[string]interface{}) {
				header = append(header, k)
			}
		case []map[string]interface{}:
			if len(v) > 0 {
				header = make([]string, len(v[0]))
				for k := range v[0] {
					header = append(header, k)
				}
			}
		}
		m.hook.(*csvWriterHooks).SetHeader(strings.Join(header, m.c.Delimiter))
	}
	if v, _, err := ctx.TransformOutput(item); err == nil {
		logger.Debugf("file sink transform data %s", v)
		m.mux.Lock()
		defer m.mux.Unlock()
		if !m.firstLine {
			_, e := m.writer.Write(m.hook.Line())
			if e != nil {
				return err
			}
		} else {
			n, err := m.writer.Write(m.hook.Header())
			if err != nil {
				return err
			}
			if n > 0 {
				_, e := m.writer.Write(m.hook.Line())
				if e != nil {
					return err
				}
			}
			m.firstLine = false
		}
		_, e := m.writer.Write(v)
		if e != nil {
			return err
		}
	} else {
		return fmt.Errorf("file sink transform data error: %v", err)
	}
	return nil
}

func (m *fileSink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing file sink")
	if m.file != nil {
		ctx.GetLogger().Infof("File sync before close")
		_, e := m.writer.Write(m.hook.Footer())
		if e != nil {
			ctx.GetLogger().Errorf("file sink fails to write footer with error %s.", e)
		}
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

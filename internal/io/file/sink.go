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

package file

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type sinkConf struct {
	RollingInterval    cast.DurationConf `json:"rollingInterval"`
	RollingCount       int               `json:"rollingCount"`
	RollingNamePattern string            `json:"rollingNamePattern"` // where to add the timestamp to the file name
	RollingHook        string            `json:"rollingHook"`
	RollingHookProps   map[string]any    `json:"rollingHookProps"`
	RollingSize        int64             `json:"rollingSize"`
	CheckInterval      cast.DurationConf `json:"checkInterval"`
	Path               string            `json:"path"` // support dynamic property, when rolling, make sure the path is updated
	FileType           FileType          `json:"fileType"`
	HasHeader          bool              `json:"hasHeader"`
	Delimiter          string            `json:"delimiter"`
	Format             string            `json:"format"` // only use for validation; transformation is done in sink_node
	Compression        string            `json:"compression"`
	Encryption         string            `json:"encryption"`
	Fields             []string          `json:"fields"` // only use for extracting header for csv; transformation is done in sink_node
}

type fileSink struct {
	c *sinkConf

	mux      syncx.Mutex
	fws      map[string]*fileWriter
	rollHook modules.RollHook
	headers  string
}

func (m *fileSink) Provision(ctx api.StreamContext, props map[string]interface{}) error {
	c := &sinkConf{
		RollingCount:  1000000,
		Path:          "cache",
		FileType:      LINES_TYPE,
		CheckInterval: cast.DurationConf(5 * time.Minute),
	}
	if err := cast.MapToStruct(props, c); err != nil {
		return err
	}
	if c.RollingInterval < 0 {
		return fmt.Errorf("rollingInterval must be positive")
	}
	if c.RollingCount < 0 {
		return fmt.Errorf("rollingCount must be positive")
	}

	if c.CheckInterval < 0 {
		return fmt.Errorf("checkInterval must be positive")
	}
	if c.RollingInterval == 0 && c.RollingCount == 0 && c.RollingSize == 0 {
		return fmt.Errorf("one of rollingInterval, rollingCount, or rollingSize must be set")
	}
	if c.RollingInterval > 0 && c.RollingInterval < c.CheckInterval {
		c.CheckInterval = c.RollingInterval
		ctx.GetLogger().Infof("set checkInterval to %v", c.CheckInterval)
	}
	if c.RollingNamePattern != "" && c.RollingNamePattern != "prefix" && c.RollingNamePattern != "suffix" && c.RollingNamePattern != "none" {
		return fmt.Errorf("rollingNamePattern must be one of prefix, suffix or none")
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
			ctx.GetLogger().Warnf("delimiter is not set, use default ','")
			c.Delimiter = ","
		}
	}

	if _, ok := compressionTypes[c.Compression]; !ok && c.Compression != "" {
		return fmt.Errorf("compression must be one of gzip, zstd")
	}
	if c.RollingHook != "" {
		h, ok := modules.GetFileRollHook(c.RollingHook)
		if !ok {
			return fmt.Errorf("rolling hook %s is not registered", c.RollingHook)
		}
		err := h.Provision(ctx, c.RollingHookProps)
		if err != nil {
			return err
		}
		m.rollHook = h
	}
	m.c = c
	m.fws = make(map[string]*fileWriter)
	return nil
}

func (m *fileSink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	ctx.GetLogger().Debug("Opening file sink")
	// Check if the files have opened longer than the rolling interval, if so close it and create a new one
	if m.c.CheckInterval > 0 {
		t := timex.GetTicker(time.Duration(m.c.CheckInterval))
		go func() { // this will never panic
			defer t.Stop()
			for {
				select {
				case now := <-t.C:
					e := infra.SafeRun(func() error {
						m.mux.Lock()
						defer m.mux.Unlock()
						for k, v := range m.fws {
							if now.Sub(v.Start) > time.Duration(m.c.RollingInterval) {
								err := m.roll(ctx, k, v)
								// TODO how to deal with this error
								if err != nil {
									return fmt.Errorf("file sink fails to close file %s with error %s.", k, err)
								}
							}
						}
						return nil
					})
					if e != nil {
						ctx.GetLogger().Error(e)
					}
				case <-ctx.Done():
					ctx.GetLogger().Info("file sink done")
					return
				}
			}
		}()
	}
	sch(api.ConnectionConnected, "")
	return nil
}

func (m *fileSink) Collect(ctx api.StreamContext, tuple api.RawTuple) error {
	item := tuple.Raw()
	ctx.GetLogger().Debugf("file sink receive %s", item)
	fn := m.c.Path
	if dp, ok := tuple.(api.HasDynamicProps); ok {
		t, transformed := dp.DynamicProps(fn)
		if transformed {
			fn = t
		}
	}
	ctx.GetLogger().Debugf("writing to file path %s", fn)
	fw, item, err := m.GetFws(ctx, fn, item)
	if err != nil {
		return err
	}

	m.mux.Lock()
	defer m.mux.Unlock()
	if fw.Written {
		lineBytes := fw.Hook.Line()
		_, e := fw.Writer.Write(lineBytes)
		if e != nil {
			return e
		}
		if m.c.RollingSize > 0 {
			fw.Size += int64(len(lineBytes))
		}
	} else {
		fw.Written = true
	}
	_, e := fw.Writer.Write(item)
	if e != nil {
		return e
	}
	if m.c.RollingCount > 0 {
		fw.Count++
		if fw.Count >= m.c.RollingCount {
			return m.roll(ctx, fn, fw)
		}
	}
	if m.c.RollingSize > 0 {
		fw.Size += int64(len(item))
		if fw.Size >= m.c.RollingSize {
			return m.roll(ctx, fn, fw)
		}
	}
	return nil
}

func (m *fileSink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing file sink")
	var errs []error
	for k, v := range m.fws {
		e := m.roll(ctx, k, v)
		if e != nil {
			ctx.GetLogger().Errorf("failed to close file %s: %v", k, e)
			errs = append(errs, e)
		}
	}
	if m.rollHook != nil {
		e := m.rollHook.Close(ctx)
		if e != nil {
			errs = append(errs, e)
		}
	}
	return errors.Join(errs...)
}

func (m *fileSink) roll(ctx api.StreamContext, k string, v *fileWriter) error {
	ctx.GetLogger().Infof("rolling file %s", k)
	err := v.Close(ctx)
	if err != nil {
		return err
	}
	if m.rollHook != nil {
		if rollErr := m.rollHook.RollDone(ctx, v.File.Name()); rollErr != nil {
			ctx.GetLogger().Errorf("%v roll done file:%v failed, err:%v", ctx.GetRuleId(), v.File.Name(), rollErr)
		}
	}
	delete(m.fws, k)
	// The file will be created when the next item comes
	v.Written = false
	return nil
}

// GetFws returns the file writer for the given file name, if the file writer does not exist, it will create one
// The item is used to get the csv header if needed
func (m *fileSink) GetFws(ctx api.StreamContext, fn string, item []byte) (*fileWriter, []byte, error) {
	m.mux.Lock()
	defer m.mux.Unlock()
	if m.c.FileType == CSV_TYPE && m.c.HasHeader && m.headers == "" {
		if len(m.c.Fields) > 0 {
			m.headers = strings.Join(m.c.Fields, m.c.Delimiter)
		} else {
			db := []byte(m.c.Delimiter)
			if bytes.HasPrefix(item, db) {
				cursor := len(db)
				nextCursor := cursor + 4
				hl := binary.BigEndian.Uint32(item[cursor:nextCursor])
				cursor = nextCursor
				nextCursor = cursor + int(hl)
				hb := item[cursor:nextCursor]
				m.headers = string(hb)
				ctx.GetLogger().Debugf("csv header %s", hb)
				cursor = nextCursor
				item = item[nextCursor:]
			}
		}
	}
	fws, ok := m.fws[fn]
	if !ok {
		var e error
		nfn := fn
		if m.c.RollingNamePattern != "" {
			newFile := ""
			fileDir := filepath.Dir(fn)
			fileName := filepath.Base(fn)
			switch m.c.RollingNamePattern {
			case "prefix":
				newFile = fmt.Sprintf("%d-%s", timex.GetNowInMilli(), fileName)
			case "suffix":
				ext := filepath.Ext(fn)
				newFile = fmt.Sprintf("%s-%d%s", strings.TrimSuffix(fileName, ext), timex.GetNowInMilli(), ext)
			default:
				newFile = fileName
			}
			nfn = filepath.Join(fileDir, newFile)
		}

		fws, e = m.createFileWriter(ctx, nfn, m.c.FileType, m.headers, m.c.Compression, m.c.Encryption)
		if e != nil {
			return nil, item, e
		}
		m.fws[fn] = fws
	}
	return fws, item, nil
}

func GetSink() api.Sink {
	return &fileSink{}
}

var (
	_ api.BytesCollector = &fileSink{}
	_ model.StreamWriter = &fileSink{}
)

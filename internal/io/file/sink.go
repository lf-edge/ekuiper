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
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type sinkConf struct {
	RollingInterval    int64          `json:"rollingInterval"`
	RollingCount       int            `json:"rollingCount"`
	RollingNamePattern string         `json:"rollingNamePattern"` // where to add the timestamp to the file name
	RollingHook        string         `json:"rollingHook"`
	RollingHookProps   map[string]any `json:"rollingHookProps"`
	CheckInterval      int64          `json:"checkInterval"`
	Path               string         `json:"path"` // support dynamic property, when rolling, make sure the path is updated
	FileType           FileType       `json:"fileType"`
	HasHeader          bool           `json:"hasHeader"`
	Delimiter          string         `json:"delimiter"`
	Format             string         `json:"format"` // only use for validation; transformation is done in sink_node
	Compression        string         `json:"compression"`
	Encryption         string         `json:"encryption"`
	Fields             []string       `json:"fields"` // only use for extracting header for csv; transformation is done in sink_node
}

type fileSink struct {
	c *sinkConf

	mux      sync.Mutex
	fws      map[string]*fileWriter
	rollHook modules.RollHook
}

func (m *fileSink) Provision(ctx api.StreamContext, props map[string]interface{}) error {
	c := &sinkConf{
		RollingCount:  1000000,
		Path:          "cache",
		FileType:      LINES_TYPE,
		CheckInterval: (5 * time.Minute).Milliseconds(),
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
	if c.RollingInterval == 0 && c.RollingCount == 0 {
		return fmt.Errorf("one of rollingInterval and rollingCount must be set")
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

func (m *fileSink) Connect(ctx api.StreamContext) error {
	ctx.GetLogger().Debug("Opening file sink")
	// Check if the files have opened longer than the rolling interval, if so close it and create a new one
	if m.c.CheckInterval > 0 {
		t := timex.GetTicker(m.c.CheckInterval)
		go func() {
			defer t.Stop()
			for {
				select {
				case now := <-t.C:
					m.mux.Lock()
					for k, v := range m.fws {
						if now.Sub(v.Start) > time.Duration(m.c.RollingInterval)*time.Millisecond {
							err := m.roll(ctx, k, v)
							// TODO how to deal with this error
							if err != nil {
								ctx.GetLogger().Errorf("file sink fails to close file %s with error %s.", k, err)
							}
						}
					}
					m.mux.Unlock()
				case <-ctx.Done():
					ctx.GetLogger().Info("file sink done")
					return
				}
			}
		}()
	}
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
	fw, err := m.GetFws(ctx, fn, item)
	if err != nil {
		return err
	}

	m.mux.Lock()
	defer m.mux.Unlock()
	if fw.Written {
		_, e := fw.Writer.Write(fw.Hook.Line())
		if e != nil {
			return e
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
	ctx.GetLogger().Debugf("rolling file %s", k)
	err := v.Close(ctx)
	if err != nil {
		return err
	} else {
		if m.rollHook != nil {
			err = m.rollHook.RollDone(ctx, v.File.Name())
			if err != nil {
				return err
			}
		}
	}
	delete(m.fws, k)
	// The file will be created when the next item comes
	v.Written = false
	return nil
}

// GetFws returns the file writer for the given file name, if the file writer does not exist, it will create one
// The item is used to get the csv header if needed
func (m *fileSink) GetFws(ctx api.StreamContext, fn string, item interface{}) (*fileWriter, error) {
	m.mux.Lock()
	defer m.mux.Unlock()
	fws, ok := m.fws[fn]
	if !ok {
		var e error
		// extract header for csv
		var headers string
		if m.c.FileType == CSV_TYPE && m.c.HasHeader {
			var header []string
			if len(m.c.Fields) > 0 {
				header = m.c.Fields
			} else {
				switch v := item.(type) {
				case map[string]interface{}:
					header = make([]string, len(v))
					i := 0
					for k := range item.(map[string]interface{}) {
						header[i] = k
						i++
					}
				case []map[string]interface{}:
					if len(v) > 0 {
						header = make([]string, len(v[0]))
						i := 0
						for k := range v[0] {
							header[i] = k
							i++
						}
					}
				}
				sort.Strings(header)
			}
			headers = strings.Join(header, m.c.Delimiter)
		}
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

		fws, e = m.createFileWriter(ctx, nfn, m.c.FileType, headers, m.c.Compression, m.c.Encryption)
		if e != nil {
			return nil, e
		}
		m.fws[fn] = fws
	}
	return fws, nil
}

func GetSink() api.Sink {
	return &fileSink{}
}

var (
	_ api.BytesCollector = &fileSink{}
	_ model.StreamWriter = &fileSink{}
)

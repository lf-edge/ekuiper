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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type sinkConf struct {
	Interval           *int     `json:"interval"` // deprecated, will remove in the next release
	RollingInterval    int64    `json:"rollingInterval"`
	RollingCount       int      `json:"rollingCount"`
	RollingNamePattern string   `json:"rollingNamePattern"` // where to add the timestamp to the file name
	CheckInterval      *int64   `json:"checkInterval"`      // Once interval removed, this will be NOT nullable
	Path               string   `json:"path"`               // support dynamic property, when rolling, make sure the path is updated
	FileType           FileType `json:"fileType"`
	HasHeader          bool     `json:"hasHeader"`
	Delimiter          string   `json:"delimiter"`
	Format             string   `json:"format"` // only use for validation; transformation is done in sink_node
	Compression string `json:"compression"`
}

type fileSink struct {
	c *sinkConf

	mux sync.Mutex
	fws map[string]*fileWriter
}

func (m *fileSink) Configure(props map[string]interface{}) error {
	c := &sinkConf{
		RollingCount: 1000000,
		Path:         "cache",
		FileType:     LINES_TYPE,
	}
	if err := cast.MapToStruct(props, c); err != nil {
		return err
	}
	if c.Interval != nil {
		if *c.Interval < 0 {
			return fmt.Errorf("interval must be positive")
		} else if c.CheckInterval == nil {
			conf.Log.Warnf("interval is deprecated, use checkInterval instead. automatically set checkInterval to %d", c.Interval)
			t := int64(*c.Interval)
			c.CheckInterval = &t
		} else {
			conf.Log.Warnf("interval is deprecated and ignored, use checkInterval instead.")
		}
	} else if c.CheckInterval == nil { // set checkInterval default value if both interval and checkInerval are not set
		t := (5 * time.Minute).Milliseconds()
		c.CheckInterval = &t
	}
	if c.RollingInterval < 0 {
		return fmt.Errorf("rollingInterval must be positive")
	}
	if c.RollingCount < 0 {
		return fmt.Errorf("rollingCount must be positive")
	}

	if *c.CheckInterval < 0 {
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
			conf.Log.Warnf("delimiter is not set, use default ','")
			c.Delimiter = ","
		}
	}

	if _, ok := compressionTypes[c.Compression]; !ok && c.Compression!="" {
		return fmt.Errorf("compression must be one of none, zlib, gzip or flate")
	}

	m.c = c
	m.fws = make(map[string]*fileWriter)
	return nil
}

func (m *fileSink) Open(ctx api.StreamContext) error {
	ctx.GetLogger().Debug("Opening file sink")
	// Check if the files have opened longer than the rolling interval, if so close it and create a new one
	if *m.c.CheckInterval > 0 {
		t := conf.GetTicker(int(*m.c.CheckInterval))
		go func() {
			defer t.Stop()
			for {
				select {
				case now := <-t.C:
					m.mux.Lock()
					for k, v := range m.fws {
						if now.Sub(v.Start) > time.Duration(m.c.RollingInterval)*time.Millisecond {
							ctx.GetLogger().Debugf("rolling file %s", k)
							err := v.Close(ctx)
							// TODO how to inform this error to the rule
							if err != nil {
								ctx.GetLogger().Errorf("file sink fails to close file %s with error %s.", k, err)
							}
							delete(m.fws, k)
							// The file will be created when the next item comes
							v.Written = false
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

func (m *fileSink) Collect(ctx api.StreamContext, item interface{}) error {
	ctx.GetLogger().Debugf("file sink receive %s", item)
	fn, err := ctx.ParseTemplate(m.c.Path, item)
	if err != nil {
		return err
	}
	fw, err := m.GetFws(ctx, fn, item)
	if err != nil {
		return err
	}
	if v, _, err := ctx.TransformOutput(item); err == nil {
		ctx.GetLogger().Debugf("file sink transform data %s", v)
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
		_, e := fw.Writer.Write(v)
		if e != nil {
			return e
		}
		if m.c.RollingCount > 0 {
			fw.Count++
			if fw.Count >= m.c.RollingCount {
				e = fw.Close(ctx)
				if e != nil {
					return e
				}
				delete(m.fws, fn)
				fw.Count = 0
				fw.Written = false
			}
		}
	} else {
		return fmt.Errorf("file sink transform data error: %v", err)
	}
	return nil
}

func (m *fileSink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing file sink")
	var errs []error
	for k, v := range m.fws {
		if e := v.Close(ctx); e != nil {
			ctx.GetLogger().Errorf("failed to close file %s: %v", k, e)
			errs = append(errs, e)
		}
	}
	return errors.Join(errs...)
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
			headers = strings.Join(header, m.c.Delimiter)
		}
		nfn := fn
		if m.c.RollingNamePattern != "" {
			switch m.c.RollingNamePattern {
			case "prefix":
				nfn = fmt.Sprintf("%d-%s", conf.GetNowInMilli(), fn)
			case "suffix":
				ext := filepath.Ext(fn)
				nfn = fmt.Sprintf("%s-%d%s", strings.TrimSuffix(fn, ext), conf.GetNowInMilli(), ext)
			}
		}
		fws, e = createFileWriter(ctx, nfn, m.c.FileType, headers, m.c.Compression)
		if e != nil {
			return nil, e
		}
		m.fws[fn] = fws
	}
	return fws, nil
}

func File() api.Sink {
	return &fileSink{}
}

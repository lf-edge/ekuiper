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
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zlib"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type FileSourceConfig struct {
	FileType          FileType `json:"fileType"`
	Path              string   `json:"path"`
	Interval          int      `json:"interval"`
	IsTable           bool     `json:"isTable"`
	SendInterval      int      `json:"sendInterval"`
	ActionAfterRead   int      `json:"actionAfterRead"`
	MoveTo            string   `json:"moveTo"`
	HasHeader         bool     `json:"hasHeader"`
	Columns           []string `json:"columns"`
	IgnoreStartLines  int      `json:"ignoreStartLines"`
	IgnoreEndLines    int      `json:"ignoreEndLines"`
	Delimiter string `json:"delimiter"`
	Compress  string `json:"compress"`
}

// FileSource The BATCH to load data from file at once
type FileSource struct {
	file   string
	isDir  bool
	config *FileSourceConfig
}

func (fs *FileSource) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Close file source")
	// do nothing
	return nil
}

func (fs *FileSource) Configure(fileName string, props map[string]interface{}) error {
	cfg := &FileSourceConfig{
		FileType: JSON_TYPE,
	}
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
	if !filepath.IsAbs(cfg.Path) {
		cfg.Path, err = conf.GetLoc(cfg.Path)
		if err != nil {
			return fmt.Errorf("invalid path %s", cfg.Path)
		}
	}
	if fileName != "/$$TEST_CONNECTION$$" {
		fs.file = filepath.Join(cfg.Path, fileName)
		fi, err := os.Stat(fs.file)
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("file %s not exist", fs.file)
			}
		}
		if fi.IsDir() {
			fs.isDir = true
		}
	}
	if cfg.IgnoreStartLines < 0 {
		cfg.IgnoreStartLines = 0
	}
	if cfg.IgnoreEndLines < 0 {
		cfg.IgnoreEndLines = 0
	}
	if cfg.ActionAfterRead < 0 || cfg.ActionAfterRead > 2 {
		return fmt.Errorf("invalid actionAfterRead: %d", cfg.ActionAfterRead)
	}
	if cfg.ActionAfterRead == 2 {
		if cfg.MoveTo == "" {
			return fmt.Errorf("missing moveTo when actionAfterRead is 2")
		} else {
			if !filepath.IsAbs(cfg.MoveTo) {
				cfg.MoveTo, err = conf.GetLoc(cfg.MoveTo)
				if err != nil {
					return fmt.Errorf("invalid moveTo %s: %v", cfg.MoveTo, err)
				}
			}
			fileInfo, err := os.Stat(cfg.MoveTo)
			if err != nil {
				err := os.MkdirAll(cfg.MoveTo, os.ModePerm)
				if err != nil {
					return fmt.Errorf("fail to create dir for moveTo %s: %v", cfg.MoveTo, err)
				}
			} else if !fileInfo.IsDir() {
				return fmt.Errorf("moveTo %s is not a directory", cfg.MoveTo)
			}
		}
	}
	if cfg.Delimiter == "" {
		cfg.Delimiter = ","
	}

	if cfg.Compress != ZLIB && cfg.Compress != GZIP && cfg.Compress != FLATE && cfg.Compress != NONE_COMPRESS && cfg.Compress != "" {
		return fmt.Errorf("compressAlgorithm must be one of none, zlib, gzip or flate")
	}
	fs.config = cfg
	return nil
}

func (fs *FileSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	err := fs.Load(ctx, consumer)
	if err != nil {
		select {
		case consumer <- &xsql.ErrorSourceTuple{Error: err}:
			ctx.GetLogger().Errorf("find error when loading file %s with err %v", fs.file, err)
		case <-ctx.Done():
			return
		}
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
	if fs.isDir {
		ctx.GetLogger().Debugf("Monitor dir %s", fs.file)
		entries, err := os.ReadDir(fs.file)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			file := filepath.Join(fs.file, entry.Name())
			err := fs.parseFile(ctx, file, consumer)
			if err != nil {
				ctx.GetLogger().Errorf("parse file %s fail with error: %v", file, err)
				continue
			}
		}
	} else {
		err := fs.parseFile(ctx, fs.file, consumer)
		if err != nil {
			return err
		}
	}
	// Send EOF if retain size not set if used in table
	if fs.config.IsTable {
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

func (fs *FileSource) parseFile(ctx api.StreamContext, file string, consumer chan<- api.SourceTuple) (result error) {
	r, err := fs.prepareFile(ctx, file)
	if err != nil {
		ctx.GetLogger().Debugf("prepare file %s error: %v", file, err)
		return err
	}
	meta := map[string]interface{}{
		"file": file,
	}
	defer func() {
		ctx.GetLogger().Debugf("Finish loading from file %s", file)
		if closer, ok := r.(io.Closer); ok {
			ctx.GetLogger().Debugf("Close reader")
			closer.Close()
		}
		if result == nil {
			switch fs.config.ActionAfterRead {
			case 1:
				if err := os.Remove(file); err != nil {
					result = err
				}
				ctx.GetLogger().Debugf("Remove file %s", file)
			case 2:
				targetFile := filepath.Join(fs.config.MoveTo, filepath.Base(file))
				if err := os.Rename(file, targetFile); err != nil {
					result = err
				}
				ctx.GetLogger().Debugf("Move file %s to %s", file, targetFile)
			}
		}
	}()
	return fs.publish(ctx, r, consumer, meta)
}

func (fs *FileSource) publish(ctx api.StreamContext, file io.Reader, consumer chan<- api.SourceTuple, meta map[string]interface{}) error {
	ctx.GetLogger().Debug("Start to load")
	switch fs.config.FileType {
	case JSON_TYPE:
		r := json.NewDecoder(file)
		resultMap := make([]map[string]interface{}, 0)
		err := r.Decode(&resultMap)
		if err != nil {
			return fmt.Errorf("loaded %s, check error %s", fs.file, err)
		}
		ctx.GetLogger().Debug("Sending tuples")
		for _, m := range resultMap {
			select {
			case consumer <- api.NewDefaultSourceTuple(m, meta):
			case <-ctx.Done():
				return nil
			}
			if fs.config.SendInterval > 0 {
				time.Sleep(time.Millisecond * time.Duration(fs.config.SendInterval))
			}
		}
		return nil
	case CSV_TYPE:
		r := csv.NewReader(file)
		r.Comma = rune(fs.config.Delimiter[0])
		r.TrimLeadingSpace = true
		r.FieldsPerRecord = -1
		cols := fs.config.Columns
		if fs.config.HasHeader {
			var err error
			ctx.GetLogger().Debug("Has header")
			cols, err = r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				ctx.GetLogger().Warnf("Read file %s encounter error: %v", fs.file, err)
				return err
			}
			ctx.GetLogger().Debugf("Got header %v", cols)
		}
		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				ctx.GetLogger().Warnf("Read file %s encounter error: %v", fs.file, err)
				continue
			}
			ctx.GetLogger().Debugf("Read" + strings.Join(record, ","))
			var m map[string]interface{}
			if cols == nil {
				m = make(map[string]interface{}, len(record))
				for i, v := range record {
					m["cols"+strconv.Itoa(i)] = v
				}
			} else {
				m = make(map[string]interface{}, len(cols))
				for i, v := range cols {
					m[v] = record[i]
				}
			}
			select {
			case consumer <- api.NewDefaultSourceTuple(m, meta):
			case <-ctx.Done():
				return nil
			}
			if fs.config.SendInterval > 0 {
				time.Sleep(time.Millisecond * time.Duration(fs.config.SendInterval))
			}
		}
	case LINES_TYPE:
		scanner := bufio.NewScanner(file)
		scanner.Split(bufio.ScanLines)
		for scanner.Scan() {
			var tuple api.SourceTuple
			m, err := ctx.Decode(scanner.Bytes())
			if err != nil {
				tuple = &xsql.ErrorSourceTuple{
					Error: fmt.Errorf("Invalid data format, cannot decode %s with error %s", scanner.Text(), err),
				}
			} else {
				tuple = api.NewDefaultSourceTuple(m, meta)
			}
			select {
			case consumer <- tuple:
			case <-ctx.Done():
				return nil
			}
			if fs.config.SendInterval > 0 {
				time.Sleep(time.Millisecond * time.Duration(fs.config.SendInterval))
			}
		}
	default:
		return fmt.Errorf("invalid file type %s", fs.config.FileType)
	}
	return nil
}

// prepareFile prepare file by deleting ignore lines
func (fs *FileSource) prepareFile(ctx api.StreamContext, file string) (io.Reader, error) {
	f, err := os.Open(file)
	if err != nil {
		ctx.GetLogger().Error(err)
		return nil, err
	}
	var reader io.ReadCloser

	switch fs.config.Compress {
	case "flate":
		reader = flate.NewReader(f)
	case "gzip":
		newReader, err := gzip.NewReader(f)
		if err != nil {
			return nil, err
		}
		reader = newReader
	case "zlib":
		r, err := zlib.NewReader(f)
		if err != nil {
			return nil, err
		}
		reader = r
	default:
		reader = f
	}

	if fs.config.IgnoreStartLines > 0 || fs.config.IgnoreEndLines > 0 {
		r, w := io.Pipe()
		go func() {
			defer func() {
				ctx.GetLogger().Debugf("Close pipe files %s", file)
				w.Close()
				reader.Close()
			}()
			scanner := bufio.NewScanner(reader)
			scanner.Split(bufio.ScanLines)

			ln := 0
			// This is a queue to store the lines that should be ignored
			tempLines := make([]string, 0, fs.config.IgnoreEndLines)
			for scanner.Scan() {
				if ln >= fs.config.IgnoreStartLines {
					if fs.config.IgnoreEndLines > 0 { // the last n line are left in the tempLines
						slot := (ln - fs.config.IgnoreStartLines) % fs.config.IgnoreEndLines
						if len(tempLines) <= slot { // first round
							tempLines = append(tempLines, scanner.Text())
						} else {
							_, err := w.Write([]byte(tempLines[slot]))
							if err != nil {
								ctx.GetLogger().Error(err)
								break
							}
							_, err = w.Write([]byte{'\n'})
							if err != nil {
								ctx.GetLogger().Error(err)
								break
							}
							tempLines[slot] = scanner.Text()
						}
					} else {
						_, err = w.Write(scanner.Bytes())
						if err != nil {
							ctx.GetLogger().Error(err)
							break
						}
						_, err = w.Write([]byte{'\n'})
						if err != nil {
							ctx.GetLogger().Error(err)
							break
						}
					}
				}
				ln++
			}
		}()
		return r, nil
	}
	return reader, nil
}

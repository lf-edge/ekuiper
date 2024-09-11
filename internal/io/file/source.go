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
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	_ "github.com/lf-edge/ekuiper/v2/internal/io/file/reader"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type SourceConfig struct {
	FileName         string        `json:"datasource"`
	FileType         string        `json:"fileType"`
	Path             string        `json:"path"`
	Interval         time.Duration `json:"interval"`
	IsTable          bool          `json:"isTable"`
	Parallel         bool          `json:"parallel"`
	SendInterval     time.Duration `json:"sendInterval"`
	ActionAfterRead  int           `json:"actionAfterRead"`
	MoveTo           string        `json:"moveTo"`
	IgnoreStartLines int           `json:"ignoreStartLines"`
	IgnoreEndLines   int           `json:"ignoreEndLines"`
	// Only use for planning
	Decompression string `json:"decompression"`
}

// Source load data from file system.
// Depending on file types, it may read line by line like lines, csv.
// Otherwise, it reads the file as a whole and send to company reader node to read and split.
// The planner need to plan according to the file type.
type Source struct {
	file   string
	isDir  bool
	config *SourceConfig
	reader modules.FileStreamReader
	eof    api.EOFIngest
}

func (fs *Source) Provision(ctx api.StreamContext, props map[string]any) error {
	cfg := &SourceConfig{
		FileType: "json",
	}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.FileType == "" {
		return errors.New("missing or invalid property fileType, must be 'json'")
	}
	reader, ok := modules.GetFileStreamReader(ctx, cfg.FileType)
	if ok {
		// TODO support later. If decompression is set, we need to read in the whole file
		if cfg.Decompression != "" {
			return fmt.Errorf("decompression is not supported for %s file type", cfg.FileType)
		}
		err = reader.Provision(ctx, props)
		if err != nil {
			return err
		}
		fs.reader = reader
	} else {
		ctx.GetLogger().Warnf("file type %s is not stream reader, will send out the whole file", cfg.FileType)
	}

	if cfg.Path == "" {
		return errors.New("missing property Path")
	}
	if !filepath.IsAbs(cfg.Path) {
		p, err := conf.GetLoc(cfg.Path)
		if err != nil {
			return fmt.Errorf("invalid path %s", cfg.Path)
		}
		cfg.Path = p
	}
	fs.file = filepath.Join(cfg.Path, cfg.FileName)
	fi, err := os.Stat(fs.file)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("file %s not exist", fs.file)
		}
	}
	if fi.IsDir() {
		fs.isDir = true
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
	fs.config = cfg
	return nil
}

func (fs *Source) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	sch(api.ConnectionConnected, "")
	return nil
}

// Pull file source may ingest bytes or tuple
// For stream source, it ingest one line
// For batch source, it ingest the whole file, thus it need a reader node to coordinate and read the content into lines/array
func (fs *Source) Pull(ctx api.StreamContext, _ time.Time, ingest api.TupleIngest, ingestError api.ErrorIngest) {
	fs.Load(ctx, ingest, ingestError)
}

func (fs *Source) SetEofIngest(eof api.EOFIngest) {
	fs.eof = eof
}

func (fs *Source) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Close file source")
	// do nothing
	return nil
}

func (fs *Source) Load(ctx api.StreamContext, ingest api.TupleIngest, ingestError api.ErrorIngest) {
	if fs.isDir {
		ctx.GetLogger().Debugf("Monitor dir %s", fs.file)
		entries, err := os.ReadDir(fs.file)
		// may be just forget to put in the file
		if err != nil {
			ingestError(ctx, err)
		}
		if fs.config.Parallel {
			var wg sync.WaitGroup
			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				wg.Add(1)
				go func(file string) {
					e := infra.SafeRun(func() error {
						defer wg.Done()
						fs.parseFile(ctx, file, ingest, ingestError)
						return nil
					})
					if e != nil {
						ingestError(ctx, e)
					}
				}(filepath.Join(fs.file, entry.Name()))
			}
			wg.Wait()
		} else {
			for _, entry := range entries {
				if entry.IsDir() {
					continue
				}
				file := filepath.Join(fs.file, entry.Name())
				fs.parseFile(ctx, file, ingest, ingestError)
			}
		}
	} else {
		fs.parseFile(ctx, fs.file, ingest, ingestError)
	}
	if fs.config.Interval == 0 && fs.eof != nil {
		fs.eof(ctx)
		ctx.GetLogger().Debug("All tuples sent")
	}
}

func (fs *Source) parseFile(ctx api.StreamContext, file string, ingest api.TupleIngest, ingestError api.ErrorIngest) {
	var (
		err error
		r   io.Reader
	)
	f, err := os.Open(file)
	if err != nil {
		ctx.GetLogger().Debugf("prepare file %s error: %v", file, err)
		ingestError(ctx, err)
	}
	r = f
	// This is the buffer size, 1MB by default
	maxSize := 1 << 20
	info, err := f.Stat()
	if err != nil {
		ctx.GetLogger().Debugf("get file info for %s error: %v", file, err)
	} else {
		maxSize = int(info.Size())
	}
	if fs.config.IgnoreStartLines > 0 || fs.config.IgnoreEndLines > 0 {
		r = ignoreLines(ctx, r, fs.config.IgnoreStartLines, fs.config.IgnoreEndLines)
	}
	if closer, ok := r.(io.Closer); ok {
		defer func() {
			ctx.GetLogger().Debugf("Close reader")
			closer.Close()
		}()
	}
	meta := map[string]any{"file": file}
	// Read line or read all
	if fs.reader != nil {
		err = fs.reader.Bind(ctx, r, maxSize)
		if err != nil {
			ingestError(ctx, err)
			return
		}
		for {
			line, err := fs.reader.Read(ctx)
			if err != nil {
				if err != io.EOF {
					ctx.GetLogger().Errorf("read file %s error: %v", file, err)
				}
				break
			}
			rcvTime := timex.GetNow()
			ingest(ctx, line, meta, rcvTime)
			if fs.config.SendInterval > 0 {
				time.Sleep(fs.config.SendInterval)
			}
		}
		_ = fs.reader.Close(ctx)
	} else {
		rcvTime := timex.GetNow()
		content, err := os.ReadFile(file)
		if err != nil {
			ingestError(ctx, err)
			// have error, do not need to do action after read
			return
		} else {
			ingest(ctx, content, meta, rcvTime)
		}
	}

	ctx.GetLogger().Debugf("Finish loading from file %s", file)
	switch fs.config.ActionAfterRead {
	case 1:
		if err := os.Remove(file); err != nil {
			ingestError(ctx, err)
		}
		ctx.GetLogger().Debugf("Remove file %s", file)
	case 2:
		targetFile := filepath.Join(fs.config.MoveTo, filepath.Base(file))
		if err := os.Rename(file, targetFile); err != nil {
			ingestError(ctx, err)
		}
		ctx.GetLogger().Debugf("Move file %s to %s", file, targetFile)
	}
}

func ignoreLines(ctx api.StreamContext, reader io.Reader, ignoreStartLines int, ignoreEndLines int) io.Reader {
	r, w := io.Pipe()
	go func() {
		e := infra.SafeRun(func() error {
			defer func() {
				w.Close()
				reader.(io.ReadCloser).Close()
			}()
			scanner := bufio.NewScanner(reader)
			scanner.Split(bufio.ScanLines)

			ln := 0
			// This is a queue to store the lines that should be ignored
			tempLines := make([][]byte, 0, ignoreEndLines)
			for scanner.Scan() {
				if ln >= ignoreStartLines {
					if ignoreEndLines > 0 { // the last n line are left in the tempLines
						slot := (ln - ignoreStartLines) % ignoreEndLines
						if len(tempLines) <= slot { // first round
							tempLines = append(tempLines, scanner.Bytes())
						} else {
							_, err := w.Write(tempLines[slot])
							if err != nil {
								ctx.GetLogger().Error(err)
								break
							}
							_, err = w.Write([]byte{'\n'})
							if err != nil {
								ctx.GetLogger().Error(err)
								break
							}
							tempLines[slot] = scanner.Bytes()
						}
					} else {
						_, err := w.Write(scanner.Bytes())
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
			return nil
		})
		if e != nil {
			ctx.GetLogger().Error(e)
		}
	}()
	return r
}

func (fs *Source) Info() (i model.NodeInfo) {
	if fs.reader == nil { // output batch raw, so need encrypt/decompress as a whole and then decode as a whole
		i.NeedBatchDecode = true
		i.NeedDecode = true
	} else if fs.reader.IsBytesReader() { // decrypt/decompress in scan and output raw
		i.NeedDecode = true
		i.HasCompress = true
		i.HasInterval = true
	} else { // decrypt/decompress in scan and output decoded tuple
		i.HasCompress = true
		i.HasInterval = true
	}
	return
}

func GetSource() api.Source {
	return &Source{}
}

var (
	// ingest possibly []byte and tuple
	_ api.PullTupleSource = &Source{}
	_ api.Bounded         = &Source{}
	_ model.InfoNode      = &Source{}
)

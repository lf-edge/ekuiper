// Copyright 2023 EMQ Technologies Co., Ltd.
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
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/lf-edge/ekuiper/internal/compressor"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io/file/writer"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type fileWriter struct {
	File       *os.File
	Writer     io.Writer
	Hook       writerHooks
	Start      time.Time
	Count      int
	Compress   string
	fileBuffer *writer.BufioWrapWriter
	// Whether the file has written any data. It is only used to determine if new line is needed when writing data.
	Written bool
}

func createFileWriter(ctx api.StreamContext, fn string, ft FileType, headers string, compressAlgorithm string) (_ *fileWriter, ge error) {
	ctx.GetLogger().Infof("Create new file writer for %s", fn)
	fws := &fileWriter{Start: conf.GetNow()}
	var (
		f   *os.File
		err error
	)
	Dir := filepath.Dir(fn)
	if _, err = os.Stat(Dir); os.IsNotExist(err) {
		if err := os.Mkdir(Dir, 0o777); err != nil {
			return nil, fmt.Errorf("fail to create file %s: %v", fn, err)
		}
	}

	if _, err = os.Stat(fn); os.IsNotExist(err) {
		if _, err := os.Create(fn); err != nil {
			return nil, fmt.Errorf("fail to create file %s: %v", fn, err)
		}
	}
	f, err = os.OpenFile(fn, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, os.ModeAppend)
	if err != nil {
		return nil, fmt.Errorf("fail to open file sink for %s: %v", fn, err)
	}
	defer func() {
		if ge != nil {
			_ = f.Close()
		}
	}()
	fws.File = f
	switch ft {
	case JSON_TYPE:
		fws.Hook = jsonHooks
	case CSV_TYPE:
		fws.Hook = &csvWriterHooks{header: []byte(headers)}
	case LINES_TYPE:
		fws.Hook = linesHooks
	}

	fws.Compress = compressAlgorithm

	if compressAlgorithm == "" {
		fws.Writer = writer.NewBufioWrapWriter(bufio.NewWriter(f))
	} else {
		fws.fileBuffer = writer.NewBufioWrapWriter(bufio.NewWriter(f))
		fws.Writer, err = compressor.GetCompressWriter(compressAlgorithm, fws.fileBuffer)
		if err != nil {
			return nil, fmt.Errorf("fail to get compress writer for %s: %v", compressAlgorithm, err)
		}
	}
	_, err = fws.Writer.Write(fws.Hook.Header())
	if err != nil {
		return nil, err
	}
	return fws, nil
}

func (fw *fileWriter) Close(ctx api.StreamContext) error {
	var err error
	if fw.File != nil {
		ctx.GetLogger().Debugf("File sync before close")
		_, e := fw.Writer.Write(fw.Hook.Footer())
		if e != nil {
			ctx.GetLogger().Errorf("file sink fails to write footer with error %s.", e)
		}
		if fw.Compress != "" {
			e := fw.Writer.(io.Closer).Close()
			if e != nil {
				ctx.GetLogger().Errorf("file sink fails to close compress writer with error %s.", err)
			}
			err = fw.fileBuffer.Flush()
			if err != nil {
				ctx.GetLogger().Errorf("file sink fails to flush with error %s.", err)
			}
		} else {
			err = fw.Writer.(*writer.BufioWrapWriter).Flush()
			if err != nil {
				ctx.GetLogger().Errorf("file sink fails to flush with error %s.", err)
			}
		}

		err = fw.File.Sync()
		if err != nil {
			ctx.GetLogger().Errorf("file sink fails to sync with error %s.", err)
		}
		ctx.GetLogger().Infof("Close file %s", fw.File.Name())
		return fw.File.Close()
	}
	return nil
}

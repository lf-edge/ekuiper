package file

import (
	"encoding/csv"
	"io"
	"strconv"
	"strings"

	"github.com/lf-edge/ekuiper/pkg/api"
)

type CsvReader struct {
	csvR   *csv.Reader
	config *FileSourceConfig

	ctx  api.StreamContext
	cols []string
}

func (r *CsvReader) Read() (map[string]interface{}, error) {
	record, err := r.csvR.Read()
	if err == io.EOF {
		return nil, err
	}
	if err != nil {
		r.ctx.GetLogger().Warnf("Read file %s encounter error: %v", "fs.file", err)
		return nil, err
	}
	r.ctx.GetLogger().Debugf("Read" + strings.Join(record, ","))

	var m map[string]interface{}
	if r.cols == nil {
		m = make(map[string]interface{}, len(record))
		for i, v := range record {
			m["cols"+strconv.Itoa(i)] = v
		}
	} else {
		m = make(map[string]interface{}, len(r.cols))
		for i, v := range r.cols {
			m[v] = record[i]
		}
	}

	return m, nil
}

func (r *CsvReader) Close() error {
	return nil
}

func CreateCsvReader(ctx api.StreamContext, fileStream io.Reader, config *FileSourceConfig) (FormatReader, error) {
	r := csv.NewReader(fileStream)
	r.Comma = rune(config.Delimiter[0])
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1
	cols := config.Columns
	if config.HasHeader {
		var err error
		ctx.GetLogger().Debug("Has header")
		cols, err = r.Read()
		if err == io.EOF {
			return nil, err
		}
		if err != nil {
			ctx.GetLogger().Warnf("Read file %s encounter error: %v", "fs.file", err)
			return nil, err
		}
		ctx.GetLogger().Debugf("Got header %v", cols)
	}

	reader := &CsvReader{}
	reader.csvR = r
	reader.config = config
	reader.ctx = ctx
	reader.cols = cols

	return reader, nil
}

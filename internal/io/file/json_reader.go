package file

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/lf-edge/ekuiper/contract/v2/api"
)

type JsonReader struct {
	resultMap []map[string]interface{}
	index     int
}

func (r *JsonReader) Read() (map[string]interface{}, error) {
	curr := r.index
	if curr > len(r.resultMap)-1 {
		return nil, io.EOF
	}
	r.index += 1
	return r.resultMap[curr], nil
}

func (r *JsonReader) Close() error {
	return nil
}

func CreateJsonReader(fileStream io.Reader, config *FileSourceConfig, ctx api.StreamContext) (FormatReader, error) {
	r := json.NewDecoder(fileStream)
	reader := &JsonReader{}

	resultMap := make([]map[string]interface{}, 0)
	err := r.Decode(&resultMap)
	if err != nil {
		return nil, fmt.Errorf("loaded %s, check error %s", "fs.file", err)
	}
	reader.resultMap = resultMap
	reader.index = 0
	return reader, nil
}

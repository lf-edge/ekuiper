package reader

import (
	"bufio"
	"fmt"
	"io"

	"github.com/lf-edge/ekuiper/internal/io/file/common"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type LineReader struct {
	scanner *bufio.Scanner
	ctx     api.StreamContext
}

const (
	TUPLE_ERROR int = iota // Display error in tuple
)

type ReaderError struct {
	Code    int
	Message string
}

func (e ReaderError) Error() string {
	return e.Message
}

func BuildError(code int, msg string) *ReaderError {
	return &ReaderError{code, msg}
}

func (r *LineReader) Read() ([]map[string]interface{}, error) {
	succ := r.scanner.Scan()
	if !succ {
		return nil, io.EOF
	}
	m, err := r.ctx.DecodeIntoList(r.scanner.Bytes())
	if err != nil {
		msg := fmt.Sprintf("Invalid data format, cannot decode %s with error %s", r.scanner.Text(), err)
		return nil, BuildError(TUPLE_ERROR, msg)
	} else {
		var tuples []map[string]interface{}
		for _, t := range m {
			tuples = append(tuples, t)
		}
		return tuples, nil
	}
}

func (r *LineReader) Close() error {
	return nil
}

func CreateLineReader(fileStream io.Reader, config *common.FileSourceConfig, ctx api.StreamContext) (FormatReader, error) {
	scanner := bufio.NewScanner(fileStream)
	scanner.Split(bufio.ScanLines)

	reader := &LineReader{}
	reader.scanner = scanner
	reader.ctx = ctx

	return reader, nil
}

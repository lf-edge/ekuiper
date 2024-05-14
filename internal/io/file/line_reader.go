package file

import (
	"bufio"
	"fmt"
	"io"

	"github.com/lf-edge/ekuiper/pkg/api"
)

type LineReader struct {
	scanner *bufio.Scanner
	ctx     api.StreamContext
	list    []map[string]interface{}
}

func (r *LineReader) Read() (map[string]interface{}, error) {
	for len(r.list) == 0 {
		succ := r.scanner.Scan()
		if !succ {
			return nil, io.EOF
		}
		m, err := r.ctx.DecodeIntoList(r.scanner.Bytes())
		if err != nil {
			msg := fmt.Sprintf("Invalid data format, cannot decode %s with error %s", r.scanner.Text(), err)
			return nil, BuildError(TupleError, msg)
		}
		r.list = m
	}

	mm := r.list[0]
	r.list = r.list[1:]

	return mm, nil
}

func (r *LineReader) Close() error {
	return nil
}

func CreateLineReader(ctx api.StreamContext, fileStream io.Reader, config *FileSourceConfig) (FormatReader, error) {
	scanner := bufio.NewScanner(fileStream)
	scanner.Split(bufio.ScanLines)

	reader := &LineReader{}
	reader.scanner = scanner
	reader.ctx = ctx

	return reader, nil
}

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
	reader  io.Reader
}

func (r *LineReader) Read() (map[string]interface{}, error) {
	succ := r.scanner.Scan()
	if !succ {
		return nil, io.EOF
	}
	b := r.scanner.Bytes()
	d := make([]byte, len(b))
	copy(d, b)
	m, err := r.ctx.Decode(d)
	if err != nil {
		msg := fmt.Sprintf("Invalid data format, cannot decode %s with error %s", string(d), err)
		return nil, BuildError(TupleError, msg)
	}
	return m, nil
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
	reader.reader = fileStream

	return reader, nil
}

package reader

import (
	"bufio"
	"io"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func init() {
	modules.RegisterFileStreamReader("lines", func(ctx api.StreamContext) modules.FileStreamReader {
		return &LinesReader{}
	})
}

type LinesReader struct {
	scanner *bufio.Scanner
}

func (r *LinesReader) Provision(ctx api.StreamContext, props map[string]any) error {
	return nil
}

func (r *LinesReader) Bind(ctx api.StreamContext, fileStream io.Reader) error {
	scanner := bufio.NewScanner(fileStream)
	scanner.Split(bufio.ScanLines)
	r.scanner = scanner
	return nil
}

func (r *LinesReader) Read(ctx api.StreamContext) (any, error) {
	succ := r.scanner.Scan()
	if !succ {
		return nil, io.EOF
	}
	return r.scanner.Bytes(), nil
}

func (r *LinesReader) IsBytesReader() bool {
	return true
}

func (r *LinesReader) Close(ctx api.StreamContext) error {
	return nil
}

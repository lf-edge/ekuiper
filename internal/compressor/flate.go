package compressor

import (
	"bytes"
	"compress/flate"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"io"
)

func newFlateCompressor() (*flateCompressor, error) {
	flateWriter, err := flate.NewWriter(nil, flate.DefaultCompression)
	if err != nil {
		return nil, err
	}
	return &flateCompressor{
		writer: flateWriter,
	}, nil
}

type flateCompressor struct {
	writer *flate.Writer
	buffer bytes.Buffer
}

func (g *flateCompressor) Compress(data []byte) ([]byte, error) {
	g.buffer.Reset()
	g.writer.Reset(&g.buffer)
	_, err := g.writer.Write(data)
	if err != nil {
		return nil, err
	}
	err = g.writer.Close()
	if err != nil {
		return nil, err
	}
	return g.buffer.Bytes(), nil
}

func newFlateDecompressor() (*flateDecompressor, error) {
	return &flateDecompressor{reader: flate.NewReader(bytes.NewReader(nil))}, nil
}

type flateDecompressor struct {
	reader io.ReadCloser
}

func (z *flateDecompressor) Decompress(data []byte) ([]byte, error) {
	err := z.reader.(flate.Resetter).Reset(bytes.NewReader(data), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress: %v", err)
	}

	defer func() {
		err := z.reader.Close()
		if err != nil {
			conf.Log.Warnf("failed to close flate decompressor: %v", err)
		}
	}()
	return io.ReadAll(z.reader)
}

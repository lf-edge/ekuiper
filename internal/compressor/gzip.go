package compressor

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"io"
)

func newGzipCompressor() (*gzipCompressor, error) {
	return &gzipCompressor{
		writer: gzip.NewWriter(nil),
	}, nil
}

type gzipCompressor struct {
	writer *gzip.Writer
	buffer bytes.Buffer
}

func (g *gzipCompressor) Compress(data []byte) ([]byte, error) {
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

func newGzipDecompressor() (*gzipDecompressor, error) {
	return &gzipDecompressor{}, nil
}

type gzipDecompressor struct {
	reader *gzip.Reader
}

func (z *gzipDecompressor) Decompress(data []byte) ([]byte, error) {
	if z.reader == nil {
		r, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("failed to decompress: %v", err)
		}
		z.reader = r
	} else {
		err := z.reader.Reset(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("failed to decompress: %v", err)
		}
	}
	defer func() {
		err := z.reader.Close()
		if err != nil {
			conf.Log.Warnf("failed to close gzip decompressor: %v", err)
		}
	}()
	return io.ReadAll(z.reader)
}

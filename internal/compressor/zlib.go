package compressor

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"io"
)

func newZlibCompressor() (*zlibCompressor, error) {
	return &zlibCompressor{
		writer: zlib.NewWriter(nil),
	}, nil
}

type zlibCompressor struct {
	writer *zlib.Writer
	buffer bytes.Buffer
}

func (z *zlibCompressor) Compress(data []byte) ([]byte, error) {
	z.buffer.Reset()
	z.writer.Reset(&z.buffer)
	_, err := z.writer.Write(data)
	if err != nil {
		return nil, err
	}
	err = z.writer.Close()
	if err != nil {
		return nil, err
	}
	return z.buffer.Bytes(), nil
}

func newZlibDecompressor() (*zlibDecompressor, error) {
	return &zlibDecompressor{}, nil
}

type zlibDecompressor struct {
	reader io.ReadCloser
}

func (z *zlibDecompressor) Decompress(data []byte) ([]byte, error) {
	if z.reader == nil {
		r, err := zlib.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("failed to decompress: %v", err)
		}
		z.reader = r
	} else {
		err := z.reader.(zlib.Resetter).Reset(bytes.NewReader(data), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress: %v", err)
		}
	}
	defer func() {
		err := z.reader.Close()
		if err != nil {
			conf.Log.Warnf("failed to close zlib decompressor: %v", err)
		}
	}()
	return io.ReadAll(z.reader)
}

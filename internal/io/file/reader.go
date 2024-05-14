package file

import (
	"fmt"
	"io"

	"github.com/lf-edge/ekuiper/pkg/api"
)

const (
	TupleError int = iota // display error in tuple
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

type FormatReader interface {
	Read() (map[string]interface{}, error) // Reads the next record. Returns EOF when the input has reached its end.
	Close() error
}

func GetReader(ctx api.StreamContext, fileType FileType, fileStream io.Reader, config *FileSourceConfig) (FormatReader, error) {
	switch fileType {
	case JSON_TYPE:
		return CreateJsonReader(ctx, fileStream, config)
	case CSV_TYPE:
		return CreateCsvReader(ctx, fileStream, config)
	case LINES_TYPE:
		return CreateLineReader(ctx, fileStream, config)
	default:
		return nil, fmt.Errorf("invalid file type %s", fileType)
	}
}

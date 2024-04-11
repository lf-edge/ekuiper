package file

import (
	"fmt"
	"io"

	"github.com/lf-edge/ekuiper/contract/v2/api"
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

func GetReader(fileType FileType, fileStream io.Reader, config *FileSourceConfig, ctx api.StreamContext) (FormatReader, error) {
	switch fileType {
	case JSON_TYPE:
		return CreateJsonReader(fileStream, config, ctx)
	case CSV_TYPE:
		return CreateCsvReader(fileStream, config, ctx)
	case LINES_TYPE:
		return CreateLineReader(fileStream, config, ctx)
	default:
		return nil, fmt.Errorf("invalid file type %s", fileType)
	}
}

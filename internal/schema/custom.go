package schema

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

type CustomType struct{}

func (c *CustomType) Scan(logger api.Logger, schemaDir string) (map[string]*modules.Files, error) {
	var newSchemas map[string]*modules.Files
	files, err := os.ReadDir(schemaDir)
	if err != nil {
		return nil, fmt.Errorf("cannot read schema directory: %s", err)
	} else {
		newSchemas = make(map[string]*modules.Files, len(files))
		for _, file := range files {
			fileName := filepath.Base(file.Name())
			ext := filepath.Ext(fileName)
			schemaId := strings.TrimSuffix(fileName, filepath.Ext(fileName))
			ffs, ok := newSchemas[schemaId]
			if !ok {
				ffs = &modules.Files{}
				newSchemas[schemaId] = ffs
			}
			switch ext {
			case ".so":
				ffs.SoFile = filepath.Join(schemaDir, file.Name())
				logger.Infof("schema file %s/%s loaded", schemaDir, schemaId)
			}
		}
	}
	return newSchemas, nil
}

func (c *CustomType) Infer(logger api.Logger, schemaId string, _ string) (ast.StreamFields, error) {
	return nil, nil
}

var _ modules.SchemaTypeDef = &CustomType{}

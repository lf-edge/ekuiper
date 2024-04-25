// Copyright 2022-2024 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlgen

import (
	"fmt"

	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/store"
)

type SqlQueryGenerator interface {
	IndexValuer
	SqlQueryStatement() (string, error)
	UpdateMaxIndexValue(rows map[string]interface{})
}

type IndexValuer interface {
	SetIndexValue(interface{})
	GetIndexValue() interface{}
	GetIndexValueWrap() *store.IndexFieldStoreWrap
}

const DATETIME_TYPE = "DATETIME"

type InternalSqlQueryCfg struct {
	Table                    string              `json:"table"`
	Limit                    int                 `json:"limit"`
	IndexFieldName           string              `json:"indexField"`
	IndexFieldValue          interface{}         `json:"indexValue"`
	IndexFieldDataType       string              `json:"indexFieldType"`
	IndexFieldDateTimeFormat string              `json:"dateTimeFormat"`
	IndexFields              []*store.IndexField `json:"indexFields"`
	store                    *store.IndexFieldStoreWrap
}

func (i *InternalSqlQueryCfg) InitIndexFieldStore() {
	i.store = &store.IndexFieldStoreWrap{}
	if i.IndexFieldName != "" {
		f := &store.IndexField{
			IndexFieldName:           i.IndexFieldName,
			IndexFieldValue:          i.IndexFieldValue,
			IndexFieldDataType:       i.IndexFieldDataType,
			IndexFieldDateTimeFormat: i.IndexFieldDateTimeFormat,
		}
		i.store.Init(f)
		return
	}
	i.store.Init(i.IndexFields...)
}

func (i *InternalSqlQueryCfg) SetIndexValue(v interface{}) {
	switch vv := v.(type) {
	case *store.IndexFieldStore:
		i.store.InitByStore(vv)
		i.store.LoadFromList()
	default:
		i.IndexFieldValue = vv
		i.InitIndexFieldStore()
	}
}

func (i *InternalSqlQueryCfg) GetIndexValueWrap() *store.IndexFieldStoreWrap {
	return i.store
}

func (i *InternalSqlQueryCfg) GetIndexValue() interface{} {
	return i.store.GetStore()
}

type sqlConfig struct {
	TemplateSqlQueryCfg *TemplateSqlQueryCfg `json:"templateSqlQueryCfg"`
	InternalSqlQueryCfg *InternalSqlQueryCfg `json:"internalSqlQueryCfg"`
}

func (cfg *sqlConfig) Init(props map[string]interface{}) error {
	err := cast.MapToStruct(props, &cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}

	if cfg.TemplateSqlQueryCfg == nil && cfg.InternalSqlQueryCfg == nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}

	if cfg.TemplateSqlQueryCfg != nil {
		if len(cfg.TemplateSqlQueryCfg.IndexFields) > 0 && cfg.TemplateSqlQueryCfg.IndexFieldName != "" {
			return fmt.Errorf("indexFields and indexField can't be defined at the same time")
		}
		if cfg.TemplateSqlQueryCfg.IndexFieldDataType == DATETIME_TYPE && cfg.TemplateSqlQueryCfg.IndexFieldDateTimeFormat != "" {
			t, err := cast.InterfaceToTime(cfg.TemplateSqlQueryCfg.IndexFieldValue, cfg.TemplateSqlQueryCfg.IndexFieldDateTimeFormat)
			if err != nil {
				err = fmt.Errorf("TemplateSqlQueryCfg InterfaceToTime datetime convert got error %v", err)
				return err
			}
			cfg.TemplateSqlQueryCfg.IndexFieldValue = t
		}
		if err := formatIndexFieldsDatetime(cfg.TemplateSqlQueryCfg.IndexFields); err != nil {
			return err
		}

		cfg.TemplateSqlQueryCfg.InitIndexFieldStore()
	}

	if cfg.InternalSqlQueryCfg != nil {
		if len(cfg.InternalSqlQueryCfg.IndexFields) > 0 && cfg.InternalSqlQueryCfg.IndexFieldName != "" {
			return fmt.Errorf("indexFields and indexField can't be defined at the same time")
		}
		if cfg.InternalSqlQueryCfg.IndexFieldDataType == DATETIME_TYPE &&
			cfg.InternalSqlQueryCfg.IndexFieldDateTimeFormat != "" {
			t, err := cast.InterfaceToTime(cfg.InternalSqlQueryCfg.IndexFieldValue, cfg.InternalSqlQueryCfg.IndexFieldDateTimeFormat)
			if err != nil {
				err = fmt.Errorf("InternalSqlQueryCfg InterfaceToTime datetime convert got error %v", err)
				return err
			}
			cfg.InternalSqlQueryCfg.IndexFieldValue = t
		}
		if err := formatIndexFieldsDatetime(cfg.InternalSqlQueryCfg.IndexFields); err != nil {
			return err
		}
		cfg.InternalSqlQueryCfg.InitIndexFieldStore()
	}

	return nil
}

func formatIndexFieldsDatetime(indexFields []*store.IndexField) error {
	for _, field := range indexFields {
		if field.IndexFieldDataType == DATETIME_TYPE && field.IndexFieldDateTimeFormat != "" {
			t, err := cast.InterfaceToTime(field.IndexFieldValue, field.IndexFieldDateTimeFormat)
			if err != nil {
				err = fmt.Errorf("InterfaceToTime datetime convert got error %v", err)
				return err
			}
			field.IndexFieldValue = t
		}
	}
	return nil
}

func GetQueryGenerator(driver string, props map[string]interface{}) (SqlQueryGenerator, error) {
	cfg := &sqlConfig{}
	err := cfg.Init(props)
	if err != nil {
		return nil, err
	}

	if cfg.TemplateSqlQueryCfg != nil {
		ge, err := NewTemplateSqlQuery(cfg.TemplateSqlQueryCfg)
		if err != nil {
			return nil, err
		} else {
			return ge, nil
		}
	}

	switch driver {
	case "sqlserver":
		return NewSqlServerQuery(cfg.InternalSqlQueryCfg), nil
	case "godror", "oracle":
		return NewOracleQueryGenerate(cfg.InternalSqlQueryCfg), nil
	default:
		return NewCommonSqlQuery(cfg.InternalSqlQueryCfg), nil
	}
}

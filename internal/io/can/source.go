// Copyright 2023 EMQ Technologies Co., Ltd.
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

package can

import (
	"fmt"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type Conf struct {
	Network string `json:"network,omitempty"`
	Address string `json:"address,omitempty"`
}

type source struct {
	conf *Conf
}

func (s *source) Configure(_ string, props map[string]interface{}) error {
	cfg := &Conf{
		Network: "can",
		Address: "can0",
	}
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return err
	}
	if cfg.Network != "can" && cfg.Network != "udp" {
		return fmt.Errorf("unsupported network %s", cfg.Network)
	}
	if cfg.Address == "" {
		return fmt.Errorf("address is required")
	}
	s.conf = cfg
	conf.Log.Debugf("Initialized with configurations %#v.", cfg)
	return nil
}

func (s *source) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Closing can source")
	return nil
}

func GetSource() api.Source {
	return &source{}
}

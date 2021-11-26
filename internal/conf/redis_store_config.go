package conf

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

type optional struct {
	Password string `json:"password"`
}

type edgeXConfig struct {
	Protocol    string   `json:"protocol"`
	Server      string   `json:"server"`
	Port        int      `json:"port"`
	Type        string   `json:"type"`
	MessageType string   `json:"message_type"`
	OptionalCfg optional `json:"optional"`
}

func RedisStorageConSelectorApply(connectionSelector string, conf *KuiperConf) error {
	sel := ConSelector{
		ConnSelectorStr: connectionSelector,
	}

	err := sel.Init()
	if err != nil {
		return err
	}

	//this should be edgeX redis config
	kvs, err := sel.ReadCfgFromYaml()
	if err != nil {
		return err
	}

	redisCfg := edgeXConfig{}
	err = cast.MapToStruct(kvs, &redisCfg)
	if err != nil {
		return err
	}

	if redisCfg.Type != "redis" || redisCfg.Protocol != "redis" {
		return fmt.Errorf("redis storage connection selector %s only support redis mesage bus, but got %v", sel.ConnSelectorStr, kvs)
	}

	conf.Store.Redis.Host = redisCfg.Server
	conf.Store.Redis.Port = redisCfg.Port
	conf.Store.Redis.Password = redisCfg.OptionalCfg.Password
	return nil
}

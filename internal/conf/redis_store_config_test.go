package conf

import (
	"os"
	"testing"
)

func TestRedisStorageConSelectorApply(t *testing.T) {
	type args struct {
		conf        *KuiperConf
		conSelector string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "pass",
			args: args{
				conf:        &KuiperConf{},
				conSelector: "edgex.redisMsgBus",
			},
			wantErr: false,
		},
		{
			name: "do not support mqtt message bus type, fail",
			args: args{
				conf:        &KuiperConf{},
				conSelector: "edgex.mqttMsgBus",
			},
			wantErr: true,
		},
		{
			name: "not exist connection selector",
			args: args{
				conf:        &KuiperConf{},
				conSelector: "noexist.mqtt",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := RedisStorageConSelectorApply(tt.args.conSelector, tt.args.conf); (err != nil) != tt.wantErr {
				t.Errorf("RedisStorageConSelectorApply() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRedisStorageConSelector(t *testing.T) {
	envs := map[string]string{
		"KUIPER__STORE__TYPE":                                "redis",
		"KUIPER__STORE__REDIS__CONNECTIONSELECTOR":           "edgex.redisMsgBus",
		"CONNECTION__EDGEX__REDISMSGBUS__SERVER":             "edgex-redis",
		"CONNECTION__EDGEX__REDISMSGBUS__OPTIONAL__PASSWORD": "password",
	}

	for key, value := range envs {
		err := os.Setenv(key, value)
		if err != nil {
			t.Error(err)
		}
	}

	InitConf()

	if Config.Store.Type != "redis" {
		t.Errorf("env variable should set it to redis")
	}
	if Config.Store.Redis.Host != "edgex-redis" {
		t.Errorf("env variable should set it to edgex-redis")
	}
	if Config.Store.Redis.Password != "password" {
		t.Errorf("env variable should set it to password")
	}
}

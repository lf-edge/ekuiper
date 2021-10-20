package connection

import (
	"os"
	"reflect"
	"testing"
)

func Test_getConnectionConf(t *testing.T) {
	type args struct {
		connectionSelector string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]interface{}
		wantErr bool
	}{
		{
			name: "mqtt:localConnection",
			args: args{
				connectionSelector: "mqtt.localConnection",
			},
			want: map[string]interface{}{
				"servers":  []interface{}{"tcp://127.0.0.1:1883"},
				"username": "ekuiper",
				"password": "password",
				"clientid": "ekuiper",
			},
			wantErr: false,
		},
		{
			name: "mqtt:cloudConnection",
			args: args{
				connectionSelector: "mqtt.cloudConnection",
			},
			want: map[string]interface{}{
				"servers":  []interface{}{"tcp://broker.emqx.io:1883"},
				"username": "user1",
				"password": "password",
			},
			wantErr: false,
		},
		{
			name: "mqtt:mqtt_conf3 not exist",
			args: args{
				connectionSelector: "mqtt.mqtt_conf3",
			},
			wantErr: true,
		},
		{
			name: "edgex:redisMsgBus",
			args: args{
				connectionSelector: "edgex.redisMsgBus",
			},
			want: map[string]interface{}{
				"protocol": "redis",
				"server":   "127.0.0.1",
				"port":     6379,
				"type":     "redis",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ConSelector{
				ConnSelectorCfg: tt.args.connectionSelector,
			}
			_ = c.Init()

			got, err := c.ReadCfgFromYaml()
			if (err != nil) != tt.wantErr {
				t.Errorf("getConnectionConf() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getConnectionConf() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getConnectionConfWithEnv(t *testing.T) {
	mqttServerKey := "CONNECTION__MQTT__LOCALCONNECTION__SERVERS"
	mqttServerValue := "[tcp://broker.emqx.io:1883]"

	edgexPortKey := "CONNECTION__EDGEX__REDISMSGBUS__PORT"
	edgexPortValue := "6666"

	err := os.Setenv(mqttServerKey, mqttServerValue)
	if err != nil {
		t.Error(err)
	}
	err = os.Setenv(edgexPortKey, edgexPortValue)
	if err != nil {
		t.Error(err)
	}

	type args struct {
		connectionSelector string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]interface{}
		wantErr bool
	}{
		{
			name: "mqtt:localConnection",
			args: args{
				connectionSelector: "mqtt.localConnection",
			},
			want: map[string]interface{}{
				"servers":  []interface{}{"tcp://broker.emqx.io:1883"},
				"username": "ekuiper",
				"password": "password",
				"clientid": "ekuiper",
			},
			wantErr: false,
		},
		{
			name: "edgex:redisMsgBus",
			args: args{
				connectionSelector: "edgex.redisMsgBus",
			},
			want: map[string]interface{}{
				"protocol": "redis",
				"server":   "127.0.0.1",
				"port":     int64(6666),
				"type":     "redis",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ConSelector{
				ConnSelectorCfg: tt.args.connectionSelector,
			}
			_ = c.Init()

			got, err := c.ReadCfgFromYaml()
			if (err != nil) != tt.wantErr {
				t.Errorf("getConnectionConf() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getConnectionConf() got = %v, want %v", got, tt.want)
			}
		})
	}
}

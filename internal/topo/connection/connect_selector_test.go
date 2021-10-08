package connection

import (
	"os"
	"reflect"
	"testing"
)

func Test_getConnectionConf(t *testing.T) {
	type args struct {
		connectionType     string
		connectionSelector string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]interface{}
		wantErr bool
	}{
		{
			name: "mqtt:mqtt_conf1",
			args: args{
				connectionType:     "mqtt",
				connectionSelector: "mqtt_conf1",
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
			name: "mqtt:mqtt_conf2",
			args: args{
				connectionType:     "mqtt",
				connectionSelector: "mqtt_conf2",
			},
			want: map[string]interface{}{
				"servers": []interface{}{"tcp://127.0.0.1:1883"},
			},
			wantErr: false,
		},
		{
			name: "mqtt:mqtt_conf3 not exist",
			args: args{
				connectionType:     "mqtt",
				connectionSelector: "mqtt_conf3",
			},
			wantErr: true,
		},
		{
			name: "mqtts:mqtt_conf3 not exist",
			args: args{
				connectionType:     "mqtts",
				connectionSelector: "mqtt_conf3",
			},
			wantErr: true,
		},
		{
			name: "edgex:edgex_conf1",
			args: args{
				connectionType:     "edgex",
				connectionSelector: "edgex_conf1",
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
				Type:   tt.args.connectionType,
				CfgKey: tt.args.connectionSelector,
			}

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
	mqttServerKey := "CONNECTION__MQTT__MQTT_CONF1__SERVERS"
	mqttServerValue := "[tcp://broker.emqx.io:1883]"

	edgexPortKey := "CONNECTION__EDGEX__EDGEX_CONF1__PORT"
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
		connectionType     string
		connectionSelector string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]interface{}
		wantErr bool
	}{
		{
			name: "mqtt:mqtt_conf1",
			args: args{
				connectionType:     "mqtt",
				connectionSelector: "mqtt_conf1",
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
			name: "edgex:edgex_conf1",
			args: args{
				connectionType:     "edgex",
				connectionSelector: "edgex_conf1",
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
				Type:   tt.args.connectionType,
				CfgKey: tt.args.connectionSelector,
			}

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

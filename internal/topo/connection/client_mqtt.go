package connection

import (
	"crypto/tls"
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"strings"
)

func init() {
	registerClientFactory("mqtt", func(s *ConSelector) Client {
		return &MQTTClient{selector: s}
	})
}

type MQTTConnectionConfig struct {
	Servers            []string `json:"servers"`
	PVersion           string   `json:"protocolVersion"`
	ClientId           string   `json:"clientid"`
	Uname              string   `json:"username"`
	Password           string   `json:"password"`
	Certification      string   `json:"certificationPath"`
	PrivateKPath       string   `json:"privateKeyPath"`
	InsecureSkipVerify bool     `json:"insecureSkipVerify"`
}

type MQTTClient struct {
	srv      string
	clientid string
	pVersion uint
	uName    string
	password string
	certPath string
	pkeyPath string
	Insecure bool

	selector *ConSelector
	conn     MQTT.Client
}

func (ms *MQTTClient) CfgValidate(props map[string]interface{}) error {

	cfg := MQTTConnectionConfig{}

	err := cast.MapToStructStrict(props, &cfg)
	if err != nil {
		return fmt.Errorf("failed to get config for %s, the error is %s", ms.selector.ConnSelectorCfg, err)
	}

	if srvs := cfg.Servers; srvs != nil && len(srvs) > 0 {
		ms.srv = srvs[0]
	} else {
		return fmt.Errorf("missing server property for %s", ms.selector.ConnSelectorCfg)
	}

	if cfg.ClientId == "" {
		if newUUID, err := uuid.NewUUID(); err != nil {
			return fmt.Errorf("failed to get uuid for %s, the error is %s", ms.selector.ConnSelectorCfg, err)
		} else {
			ms.clientid = newUUID.String()
		}
	} else {
		ms.clientid = cfg.ClientId
	}

	ms.pVersion = 3
	if cfg.PVersion == "3.1.1" {
		ms.pVersion = 4
	}

	if cfg.Certification != "" || cfg.PrivateKPath != "" {
		ms.certPath, err = conf.ProcessPath(cfg.Certification)
		if err != nil {
			return fmt.Errorf("failed to get certPath for %s, the error is %s", ms.selector.ConnSelectorCfg, err)
		}

		ms.pkeyPath, err = conf.ProcessPath(cfg.PrivateKPath)
		if err != nil {
			return fmt.Errorf("failed to get keyPath for %s, the error is %s", ms.selector.ConnSelectorCfg, err)
		}
	}

	ms.uName = cfg.Uname
	ms.password = strings.Trim(cfg.Password, " ")
	ms.Insecure = cfg.InsecureSkipVerify

	return nil
}

func (ms *MQTTClient) GetClient() (interface{}, error) {

	opts := MQTT.NewClientOptions().AddBroker(ms.srv).SetProtocolVersion(ms.pVersion).SetCleanSession(false)

	if ms.certPath != "" && ms.pkeyPath != "" {
		if cer, err := tls.LoadX509KeyPair(ms.certPath, ms.pkeyPath); err != nil {
			return nil, fmt.Errorf("error when load cert/key for %s, the error is: %s", ms.selector.ConnSelectorCfg, err)
		} else {
			opts.SetTLSConfig(&tls.Config{Certificates: []tls.Certificate{cer}, InsecureSkipVerify: ms.Insecure})
		}
	} else {
		if ms.uName != "" {
			opts = opts.SetUsername(ms.uName)
		}
		if ms.password != "" {
			opts = opts.SetPassword(ms.password)
		}
	}
	opts = opts.SetClientID(ms.clientid)
	opts = opts.SetAutoReconnect(true)

	c := MQTT.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		conf.Log.Errorf("The connection to mqtt broker failed for connection selector: %s ", ms.selector.ConnSelectorCfg)
		return nil, fmt.Errorf("found error when connecting for connection selector %s: %s", ms.selector.ConnSelectorCfg, token.Error())
	}
	conf.Log.Infof("The connection to mqtt broker is established successfully for connection selector: %s.", ms.selector.ConnSelectorCfg)

	ms.conn = c
	return c, nil
}

func (ms *MQTTClient) CloseClient() error {
	conf.Log.Infof("Closing the connection to mqtt broker for connection selector: %s", ms.selector.ConnSelectorCfg)
	if ms.conn != nil && ms.conn.IsConnected() {
		ms.conn.Disconnect(5000)
	}
	return nil
}

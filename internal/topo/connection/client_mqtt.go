package connection

import (
	"crypto/tls"
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/cert"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"strings"
)

func init() {
	registerClientFactory("mqtt", func(s *conf.ConSelector) Client {
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
	RootCaPath         string   `json:"rootCaPath"`
	InsecureSkipVerify bool     `json:"insecureSkipVerify"`
}

type MQTTClient struct {
	srv      string
	clientid string
	pVersion uint
	uName    string
	password string
	tls      *tls.Config

	selector *conf.ConSelector
	conn     MQTT.Client
}

func (ms *MQTTClient) CfgValidate(props map[string]interface{}) error {

	cfg := MQTTConnectionConfig{}

	err := cast.MapToStructStrict(props, &cfg)
	if err != nil {
		return fmt.Errorf("failed to get config for %s, the error is %s", ms.selector.ConnSelectorStr, err)
	}

	if srvs := cfg.Servers; srvs != nil && len(srvs) > 0 {
		ms.srv = srvs[0]
	} else {
		return fmt.Errorf("missing server property for %s", ms.selector.ConnSelectorStr)
	}

	if cfg.ClientId == "" {
		if newUUID, err := uuid.NewUUID(); err != nil {
			return fmt.Errorf("failed to get uuid for %s, the error is %s", ms.selector.ConnSelectorStr, err)
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

	tlsOpts := cert.TlsConfigurationOptions{
		SkipCertVerify: cfg.InsecureSkipVerify,
		CertFile:       cfg.Certification,
		KeyFile:        cfg.PrivateKPath,
		CaFile:         cfg.RootCaPath,
	}
	conf.Log.Infof("Connect MQTT broker with TLS configs: %v for connection selector: %s.", tlsOpts, ms.selector.ConnSelectorStr)
	tlscfg, err := cert.GenerateTLSForClient(tlsOpts)
	if err != nil {
		return err
	}

	ms.tls = tlscfg

	ms.uName = cfg.Uname
	ms.password = strings.Trim(cfg.Password, " ")

	return nil
}

func (ms *MQTTClient) GetClient() (interface{}, error) {

	opts := MQTT.NewClientOptions().AddBroker(ms.srv).SetProtocolVersion(ms.pVersion).SetCleanSession(false)

	opts = opts.SetTLSConfig(ms.tls)

	if ms.uName != "" {
		opts = opts.SetUsername(ms.uName)
	}
	if ms.password != "" {
		opts = opts.SetPassword(ms.password)
	}
	opts = opts.SetClientID(ms.clientid)
	opts = opts.SetAutoReconnect(true)

	c := MQTT.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		conf.Log.Errorf("The connection to mqtt broker failed for connection selector: %s ", ms.selector.ConnSelectorStr)
		return nil, fmt.Errorf("found error when connecting for connection selector %s: %s", ms.selector.ConnSelectorStr, token.Error())
	}
	conf.Log.Infof("The connection to mqtt broker is established successfully for connection selector: %s.", ms.selector.ConnSelectorStr)

	ms.conn = c
	return c, nil
}

func (ms *MQTTClient) CloseClient() error {
	conf.Log.Infof("Closing the connection to mqtt broker for connection selector: %s", ms.selector.ConnSelectorStr)
	if ms.conn != nil && ms.conn.IsConnected() {
		ms.conn.Disconnect(5000)
	}
	return nil
}

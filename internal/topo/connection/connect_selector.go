package connection

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"strings"
)

var SUPPORTE_CONTYPE = []string{"mqtt", "edgex"}

const CONNECTION_CONF = "connections/connection.yaml"

type ConSelector struct {
	ConnSelectorCfg string

	Type          string // mqtt edgex
	CfgKey        string // config key
	SupportedType []string
}

func (c *ConSelector) Init() error {

	c.SupportedType = SUPPORTE_CONTYPE

	conTypeSel := strings.SplitN(c.ConnSelectorCfg, ".", 2)
	if len(conTypeSel) != 2 {
		return fmt.Errorf("not a valid connection selector : %s", c.ConnSelectorCfg)
	}
	c.Type = strings.ToLower(conTypeSel[0])
	c.CfgKey = strings.ToLower(conTypeSel[1])
	return nil
}

func (c *ConSelector) ReadCfgFromYaml() (props map[string]interface{}, err error) {

	var (
		found = false
	)

	cfg := make(map[string]interface{})
	err = conf.LoadConfigByName(CONNECTION_CONF, &cfg)
	if err != nil {
		return nil, err
	}

	if cons, ok := cfg[c.Type]; ok {
		if connItems, ok1 := cons.(map[string]interface{}); ok1 {
			if conItem, ok := connItems[c.CfgKey]; ok {
				if item, ok1 := conItem.(map[string]interface{}); ok1 {
					props = item
					found = true
				}
			}
		}
	}

	if !found {
		return nil, fmt.Errorf("not found connection Type and Selector:  %s.%s", c.Type, c.CfgKey)
	}

	return
}

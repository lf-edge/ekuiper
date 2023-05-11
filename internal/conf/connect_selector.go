package conf

import (
	"fmt"
	"strings"
)

type ConSelector struct {
	ConnSelectorStr string
	Type            string // mqtt edgex
	CfgKey          string // config key
}

func (c *ConSelector) Init() error {
	conTypeSel := strings.SplitN(c.ConnSelectorStr, ".", 2)
	if len(conTypeSel) != 2 {
		return fmt.Errorf("not a valid connection selector : %s", c.ConnSelectorStr)
	}
	c.Type = strings.ToLower(conTypeSel[0])
	c.CfgKey = conTypeSel[1]
	return nil
}

func (c *ConSelector) ReadCfgFromYaml() (props map[string]interface{}, err error) {
	yamlOps, err := NewConfigOperatorFromConnectionYaml(c.Type)
	if err != nil {
		return nil, err
	}

	cfg := yamlOps.CopyConfContent()
	if len(cfg) == 0 {
		return nil, fmt.Errorf("fail to parse yaml for connection Type %s", c.Type)
	} else {
		if cons, found := cfg[strings.ToLower(c.CfgKey)]; found {
			props = cons
		} else if cons, found := cfg[c.CfgKey]; found {
			props = cons
		} else {
			return nil, fmt.Errorf("not found connection Type and Selector:  %s.%s", c.Type, c.CfgKey)
		}
	}

	jsonPath := "sources/" + c.Type + ".json"
	if c.Type == "mqtt" {
		jsonPath = "mqtt_source.json"
	}

	err = CorrectsConfigKeysByJson(props, jsonPath)
	return props, err
}

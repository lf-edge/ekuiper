package connection

import (
	"github.com/lf-edge/ekuiper/internal/conf"
)

type ClientFactoryFunc func(super *conf.ConSelector) Client

type Client interface {
	CfgValidate(map[string]interface{}) error
	GetClient() (interface{}, error)
	CloseClient() error
}

type clientWrapper struct {
	cli    Client
	conn   interface{}
	refCnt uint32
}

func NewClientWrapper(client Client, selector *conf.ConSelector) (*clientWrapper, error) {
	props, err := selector.ReadCfgFromYaml()
	if err != nil {
		return nil, err
	}
	err = client.CfgValidate(props)
	if err != nil {
		return nil, err
	}
	var con interface{}

	con, err = client.GetClient()
	if err != nil {
		return nil, err
	}

	cliWpr := &clientWrapper{
		cli:    client,
		conn:   con,
		refCnt: 1,
	}

	return cliWpr, nil
}

func (c *clientWrapper) addRef() {
	c.refCnt = c.refCnt + 1
}

func (c *clientWrapper) subRef() {
	c.refCnt = c.refCnt - 1
}

func (c *clientWrapper) IsRefEmpty() bool {
	return c.refCnt == 0
}

func (c *clientWrapper) clean() {
	_ = c.cli.CloseClient()
}

func (c *clientWrapper) getInstance() interface{} {
	return c.conn
}

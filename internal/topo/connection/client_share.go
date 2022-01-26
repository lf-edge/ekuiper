// Copyright 2022 EMQ Technologies Co., Ltd.
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

package connection

type ClientFactoryFunc func() Client

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

func NewClientWrapper(client Client, props map[string]interface{}) (*clientWrapper, error) {

	err := client.CfgValidate(props)
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

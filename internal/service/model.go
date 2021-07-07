// Copyright 2021 EMQ Technologies Co., Ltd.
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

package service

type (
	protocol string
	schema   string
)

const (
	REST    protocol = "rest"
	GRPC             = "grpc"
	MSGPACK          = "msgpack-rpc"
)

const (
	PROTOBUFF schema = "protobuf"
)

type (
	author struct {
		Name    string `json:"name"`
		Email   string `json:"email"`
		Company string `json:"company"`
		Website string `json:"website"`
	}
	fileLanguage struct {
		English string `json:"en_US"`
		Chinese string `json:"zh_CN"`
	}
	about struct {
		Author      *author       `json:"author"`
		HelpUrl     *fileLanguage `json:"helpUrl"`
		Description *fileLanguage `json:"description"`
	}
	mapping struct {
		Name        string        `json:"name"`
		ServiceName string        `json:"serviceName"`
		Description *fileLanguage `json:"description"`
	}
	binding struct {
		Name        string                 `json:"name"`
		Description *fileLanguage          `json:"description"`
		Address     string                 `json:"address"`
		Protocol    protocol               `json:"protocol"`
		SchemaType  schema                 `json:"schemaType"`
		SchemaFile  string                 `json:"schemaFile"`
		Functions   []*mapping             `json:"functions"`
		Options     map[string]interface{} `json:"options"`
	}

	conf struct {
		About      *about              `json:"about"`
		Interfaces map[string]*binding `json:"interfaces"`
	}
)

// The external function's location, currently service.interface.
type serviceInfo struct {
	About      *about
	Interfaces map[string]*interfaceInfo
}

type schemaInfo struct {
	SchemaType schema
	SchemaFile string
}

type interfaceInfo struct {
	Desc      *fileLanguage
	Addr      string
	Protocol  protocol
	Schema    *schemaInfo
	Functions []string
	Options   map[string]interface{}
}

type restOption struct {
	InsecureSkipVerify bool              `json:"insecureSkipVerify"`
	Headers            map[string]string `json:"headers"`
}

type functionContainer struct {
	ServiceName   string
	InterfaceName string
	MethodName    string
}

type FunctionExec struct {
	Protocol protocol
	Addr     string
}

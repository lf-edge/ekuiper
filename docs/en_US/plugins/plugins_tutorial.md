# A lightweight loT edge stream processing - eKuiper plugin development tutorial

[LF Edge eKuiper](https://www.lfedge.org/projects/ekuiper/) is a lightweight loT streaming data processing software
based on SQL. It provides a set of plugin mechanism for implementing customized source, sink and SQL function to extend
the ability of stream processing. This tutorial gives a detailed introduction to the process of development,
compilation, and deployment of the eKuiper plugin.

## Overview

eKuiper plugin is based on the plugin mechanism of Golang, users can build loosely-coupled plugin applications,  dynamic loading and binding when it is running. However, because the limitation of the Golang plugin system, the compilation and usage of the eKuiper plugin also have corresponding limitations:
- The plugin does not support Windows system
- The compilation environment of the plugin is required to be as consistent as possible with eKuiper. Including but not limited to:
    - The same Go version
    - The version of the same libraries plugin and eKuiper depend on must be completely the same, including eKuiper itself
    - The plugin needs to be completely consistent with GOPATH of the eKuiper compilation environment
    

These limitations are relatively strict, and they almost require compiling and running the plugin and eKuiper on the same machine. It often results in the plugin which complied by the development environment can not be used in producing eKuiper. This article gives a detailed introduction to one reliable and available plugin development environment setting and process, which is recommended to the eKuiper plugin developer to use. Generally, the process for development and usage of the plugin is as follows:

- Development
    - Create and develop the plugin project
    - Compile and debug the plugin
- Deployment
    - Compile plugins which can be used for the production environment
    - Deploy plugins to the production environment

## Plugin development 

Developing plugin is generally carried out in the development environment. eKuiper plugins will be deployed to the production environment after passing debugging and running the development environment.

### Create and develop the plugin project

There are some plugin examples in the *extensions* directory of the eKuiper project source code. The user customized plugin can also be developed in the eKuiper project. However, users usually need to create the new project outside of the eKuiper project to develop customized plugins, to manage code more conveniently. It's recommended to use Go module to develop plugin projects, the typical structure of project is listed as following.

```
plugin_project
  sources         //source code directory of the plugin source 
    mysource.go
  sinks           //source code directory of the plugin sink
    mysink.go
  functions       //source code directory of the plugin function
    myfunction.go
  target          //directory of compiling results   
  go.mod          //file go module
```

Developing a plugin needs to extend the interface in eKuiper, so it must depend on the eKuiper project. The simplest go.mod also needs to include the dependency for eKuiper. A typical go.mod is as follows:
```go
module samplePlugin

go 1.16

require (
	github.com/lf-edge/ekuiper v0.0.0-20200323140757-60d00241372b
)
```

The eKuiper plugin has three types. The source code can be put into the corresponding directory. For the detailed method of plugin development: [LF Edge eKuiper extension](../extension/overview.md). This article will take the Sink plugin as an example to introduce the process of plugin development and deployment. We will develop a basic MySql sink, for write stream output data to the MySql database.

- Create plugin the project samplePlugin with the above directory structure
- Create file mysql.go under the sinks directory
- Edit file mysql.go for implementing the plugin
    -  Implement [api.Sink](https://github.com/lf-edge/ekuiper/blob/master/pkg/api/stream.go) interface
    - Export Symbol: Mysql. It could be a constructor function so that each rule can instantiate an own mysql plugin instance. Or it could be the struct which means every rule will share a singleton of the plugin. If the plugin has states like the connection, the first approach is preferred.
- Edit go.mod, add Mysql driver module

The complete source code of mysql.go is as follows:

```go
package main

// This is a simplified mysql sink which is for test and tutorial only

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/lf-edge/ekuiper/common"
	"github.com/lf-edge/ekuiper/xstream/api"
)

type mysqlConfig struct {
	Url   string `json:"url"`
	Table string `json:"table"`
}

type mysqlSink struct {
	conf *mysqlConfig
	//The db connection instance
	db *sql.DB
}

func (m *mysqlSink) Configure(props map[string]interface{}) error {
	cfg := &mysqlConfig{}
	err := common.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Url == "" {
		return fmt.Errorf("property Url is required")
	}
	if cfg.Table == "" {
		return fmt.Errorf("property Table is required")
	}
	return nil
}

func (m *mysqlSink) Open(ctx api.StreamContext) (err error) {
	logger := ctx.GetLogger()
	logger.Debug("Opening mysql sink")
	m.db, err = sql.Open("mysql", m.conf.Url)
	return
}

// This is a simplified version of data collect which just insert the received string into hardcoded name column of the db
func (m *mysqlSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	if v, ok := item.([]byte); ok {
		//TODO in production: deal with various data type of the unmarshalled item.
		// It is a json string of []map[string]interface{} by default;
		// And it is possible to be any other kind of data if the sink `dataTemplate` is set
		logger.Debugf("mysql sink receive %s", item)
		//TODO hard coded column here. In production, we'd better get the column/value pair from the item
		sql := fmt.Sprintf("INSERT INTO %s (`name`) VALUES ('%s')", m.conf.Table, v)
		logger.Debugf(sql)
		insert, err := m.db.Query(sql)
		if err != nil {
			return err
		}
		defer insert.Close()
	} else {
		logger.Debug("mysql sink receive non byte data")
	}
	return nil
}

func (m *mysqlSink) Close(ctx api.StreamContext) error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

// export the constructor function to be used to instantiates the plugin
func Mysql() api.Sink {
	return &mysqlSink{}
}
```
 The complete code of go.mod is as follows:
 ```go
module samplePlugin

go 1.16

require (
	github.com/lf-edge/ekuiper v0.0.0-20200323140757-60d00241372b
	github.com/go-sql-driver/mysql v1.5.0
)
 ```

### Compile and debug the plugin

The environment of compiling the plugin should be consistent with that of  eKuiper. In the development environment, the typical usage is that locally download and compile eKuiper and plugin, and then debug plugin functions in the local eKuiper,  or compile the plugin in the docker container of eKuiper and use the eKuiper container to debug it.

#### Compile locally

Developers can locally compile eKuiper and the plugin for debugging, which steps are as follows:
1. Download eKuiper source code: `git clone https://github.com/lf-edge/ekuiper.git`
2.  Compile eKuiper: run `make` under the eKuiper directory
3. Compile the plugin:
    1. Run `go mod edit -replace github.com/lf-edge/ekuiper=$eKuiperPath` under the plugin project, make the eKuiper dependence point to the local eKuiper, and then please replace the download directory of step 1 by $eKuiperPath, the same below.
   2. Because the Golang plugin system has a very strict limitation of the dependency paths, it is more safe to build the plugin from the eKuiper main project. In the eKuiper project, add a new *mod* file with a name like *sample.mod* to point to your plugin project.
      ```
      module github.com/lf-edge/ekuiper
      go 1.16
      require samplePlugin v0.0.0
      replace samplePlugin => ../samplePlugin     
      ```
   3. In eKuiper path, compile the plugin so with the newly added mod file
       ```go
       go build -trimpath -modfile sample.mod --buildmode=plugin -o $eKuiperPath/_build/$build/plugins/sinks/Mysql@v1.0.0.so ../samplePlugin/sinks/mysql.go
       ```

### Docker compile

eKuiper provides different docker images for different purpose. The development docker image should be used for compiling plugins. From 0.4.0, the eKuiper image with tag x.x.x (e.g. `kuiper:0.4.0`) is the development docker image. For 0.3.x, eKuiper image with tag x.x.x-dev (e.g. `kuiper:0.3.0-dev`) is the development docker image. Compared with the running version, the development version provides the development environment of Go, which lets users compile the plugin that can be completely compatible with the officially published version of eKuiper. The compiling steps in docker are as follows:
1. Run docker of the development version of eKuiper. Users need to mount the local plugin directory to the directory in docker, and then they can access and compile the plugin project in docker. The author's plugin project is located in the local `/var/git` directory. We map the local directory `/var/git` to the `/home` directory in docker by using the following commands.
    ```go
    docker run -d --name kuiper-dev --mount type=bind,source=/var/git,target=/home lfedge/ekuiper:0.3.0-dev
    ```
2. The principle of compiling plugins in docker environment is the same as the local compilation. The compiled plugin is locating in the target directory of the plugin project.
    ```go
    -- In host
    # docker exec -it kuiper-dev /bin/sh
    
    -- In docker instance
   // Add the sample.mod to eKuiper project then run the below build command
    # go build -trimpath -modfile sample.mod --buildmode=plugin -o /home/samplePlugin/target/plugins/sinks/Mysql@v1.0.0.so ../samplePlugin/sinks/mysql.go
    ```
You can use below sample shell script in your plugin project to automatically build and package the plugins. Please modify the variables at the beginning of the script to meet the requirements of different environments.

```shell script
#!/bin/sh
export EKUIPER_SOURCE=../ekuiper
export PLUGIN_TARGET=$EKUIPER_SOURCE/plugins
export ETC_TARGET=$EKUIPER_SOURCE/etc
export ZIP_TARGET=plugins
export VERSION=0.0.1

go mod edit -replace github.com/lf-edge/ekuiper=$EKUIPER_SOURCE

go build -trimpath --buildmode=plugin -o $PLUGIN_TARGET/sinks/Mysql@v$VERSION.so sinks/mysql.go

## zip the output
mkdir $ZIP_TARGET/sinks
zip -o $ZIP_TARGET/sinks/mysql.zip $PLUGIN_TARGET/sinks/Mysql@v$VERSION.so
```

### Debug and run the plugin

Run eKuiper in the local or **Develop** Docker, create streams and rules, set action of the rule to mysql, then users can test the customized mysql sink plugin. Please refer [eKuiper documentation](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/getting_started.md) for the steps of creating streams and rules. The following provides a rule using the mysql plugin for reference.
```
{
  "id": "ruleTest",
  "sql": "SELECT * from demo",
  "actions": [
    {
      "log": {},
      "mysql":{
        "url": "user:test@tcp(localhost:3307)/user",
        "table": "test"
      }
    }
  ]
}
```

During development testing, it is also fine to manually copy the compiled .so file and the .yaml file(if any) to the corresponding folders and then restart eKuiper. In development docker image, the default eKuiper location is `/usr/local/kuiper`.
It should be noted that loading the new version after compiling the plugin again needs to restart eKuiper.

## Plugin deployment

If the production environment and development environment are different, the developed plugin needs to be compiled again and deployed to the production environment. Assuming that the production environment adopts eKuiper docker to deploy, this article will describe how to deploy the plugin to the production environment.  

### Plugin compilation

The plugin should use the same environment as the production environment  eKuiper to compile in principle. If the production environment is eKuiper docker, should use the dev docker environment that has the same version as the production environment to compile the plugin. For example, if the production environment uses docker mirroring [lfedge/ekuiper:0.3.0](https://registry.hub.docker.com/layers/lfedge/ekuiper/0.3.0/images/sha256-0e3543d33f6f8c56de044d5ff001fd39b9e26f82219ca5fd25605953ed33580e?context=explore), the plugin should be compiled in [lfedge/ekuiper:0.3.0-dev](https://registry.hub.docker.com/layers/lfedge/ekuiper/0.3.0-dev/images/sha256-a309d3821b55b01dc01c4f4a04e83288bf5526325f0073197387f2ca425260d0?context=explore) environment.

Please refer [Docker compile](#docker编译) for the compilation process. The compiled plugin can be tested in the Development Docker image before deploying.

### Plugin deployment 

Users can use [REST API](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/restapi/plugins.md) or [CLI](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/cli/plugins.md) to manage plugins. The following takes the REST API as an example to deploy the plugin compiled in the previous step to the production environment. 

1. Package the plugin and put it into the http server. Package the file `.so` of the plugin compiled in the previous step and the default configuration file (only required for source) `.yaml` into a `.zip` file (assuming that the file is `mysqlSink.zip`). Put this file into the http server that the production environment can also access. 
    - Some plugin may depend on libs that are not installed on eKuiper environment. The user can either install them manually in the eKuiper server or put the install script and dependencies in the plugin zip and let the plugin management system do the installation. Please refer to [ Plugin File Format](../restapi/plugins.md#plugin-file-format) for detail.
2. Use REST API to create plugins:
   ```
   POST http://{$production_eKuiper_ip}:9081/plugins/sinks
   Content-Type: application/json
   
   {"name":"mysql","file":"http://{$http_server_ip}/plugins/sinks/mysqlSink.zip"}
   ```
3. Verify whether the plugin was created successfully or not 
    ```
    GET http://{$production_eKuiper_ip}:9081/plugins/sinks/mysql
    ```
    Return
    ```json
    {
       "name": "mysql",
       "version": "1.0.0"
    }
    ```

So far, the plugin has been deployed successfully. Users can create rules with mysql sink for verification.


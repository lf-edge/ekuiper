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

Developing plugin is generally carried out in the development environment. eKuiper plugins will be deployed to the production environment after passing debugging and running the development environment. Since the limitation of Golang plugin system, here we provide two practical ways for plugin developer to set up environment for eKuiper plugin: **create the plugin project inside eKuiper** and **create the plugin project outside eKuiper**.
The eKuiper plugin has three types: **sources**,**functions** and **sinks**, for the detailed method of plugin development: [LF Edge eKuiper extension](../extension/overview.md). This article will take the Sink plugin as an example to introduce the process of plugin development and deployment. We will develop a basic MySql sink, for write stream output data to the MySql database. The workflow list as followings:

- Create the plugin project
- Create file mysql.go under the sinks directory
- Edit file mysql.go for implementing the plugin
    - Implement [api.Sink](https://github.com/lf-edge/ekuiper/blob/master/pkg/api/stream.go) interface
    - Export Symbol: Mysql. It could be a constructor function so that each rule can instantiate an own mysql plugin instance. Or it could be the struct which means every rule will share a singleton of the plugin. If the plugin has states like the connection, the first approach is preferred.
- Edit go.mod, add Mysql driver module
- Build the eKuiper and plugin

### Create the plugin project inside eKuiper

When users develop the plugin by this way, he must firstly download the eKuiper source code and run `make` command inside the eKuiper root directory. There are some plugin examples in the *extensions* directory of the eKuiper project source code. The advantage is all existing official plugins are developed by this way so new plugin developer can directly start instead of setting up a new project from scratch.
Users can also use extensions directory to develop the plugins, the structure of project is like this:
```
extensions
  sinks
    mysql
      mysql.go
  go.mod
```
The extensions directory is using Go module to manage the existing example plugins packages, so users just need create a new directory for their plugin, put their code there and update the dependency in go.mod file.

Next users need edit the mysql.go and implement the plugin. The complete source code of mysql.go is as follows:
```go
package main

// This is a simplified mysql sink which is for test and tutorial only

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
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
	err := cast.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.Url == "" {
		return fmt.Errorf("property Url is required")
	}
	if cfg.Table == "" {
		return fmt.Errorf("property Table is required")
	}
	m.conf = cfg
	return nil
}

func (m *mysqlSink) Open(ctx api.StreamContext) (err error) {
	logger := ctx.GetLogger()
	logger.Debugf("Opening mysql sink %v", m.conf)
	m.db, err = sql.Open("mysql", m.conf.Url)
	if err != nil {
		logger.Error(err)
	}
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

### Create the plugin project outside eKuiper

However, users usually need to create the new project outside of the eKuiper project to develop customized plugins, to manage code more conveniently. It's recommended to use Go module to develop plugin projects, the code structure of project is listed as following. 

```
samplePlugin
  sinks           //source code directory of the plugin sink
    mysql.go
  target          //directory of compiling results   
  go.mod          //file go module
```

Next we need edit the mysql.go and implement the plugin, users can use the same code previously introduced.
Developing a plugin needs to extend the interface in eKuiper, so it must depend on the eKuiper project. A typical go.mod is as follows:
```go
module samplePlugin

go 1.16

require (
	github.com/lf-edge/ekuiper v0.0.0-20200323140757-60d00241372b
)
```

mysql.go also have a dependency for mysql package, so the finial go.mod is this.
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

If users create plugin project inside eKuiper, then he can just use the following method to build the plugin.
```shell
   # compile the eKuiper
   go build -trimpath -o ./_build/$build/bin/kuiperd cmd/kuiperd/main.go
    
   # compile the plugin that using the extensions folder within eKuiper project
   go build -trimpath -modfile extensions.mod --buildmode=plugin -o ./_build/$build/plugins/sinks/Mysql@v1.0.0.so extensions/sinks/mysql/mysql.go
 ```

However, if developers create plugin project outside eKuiper, he needs following steps to compile eKuiper and the plugin for debugging:
1. Download eKuiper source code: `git clone https://github.com/lf-edge/ekuiper.git`
2. Compile eKuiper: run `make` under the eKuiper directory
3. eKuiper main path set: set environment variable `eKuiperPath` by the download directory of step 1, e.g. `export eKuiperPath=/go/ekuiper`
4. Build env set up: in order to compile the plugin in the same environment with ekuiper, users need do some special steps to set up the build environment.
   1. Run `go mod edit -replace github.com/lf-edge/ekuiper=$eKuiperPath` under the plugin project, make the eKuiper dependence point to the local eKuiper, here the `$eKuiperPath` will be replaced by the download directory of step 1, the same below.
      Here is an example after this step
      ```
      module samplePlugin

      go 1.16

      require (
        github.com/lf-edge/ekuiper v0.0.0-20200323140757-60d00241372b
        github.com/go-sql-driver/mysql v1.5.0
      )
      replace github.com/lf-edge/ekuiper => /go/ekuiper # replace ekuiper with local ekuiper module
      
      ```
   2. Because the Golang plugin system has a very strict limitation of the dependency paths, it is more safe to build the plugin from the eKuiper main project. In the eKuiper project, add a new *mod* file with a name like *sample.mod* to point to your plugin project.
      Here is an example if your plugin project in the same directory with eKuiper project
      ```
      module github.com/lf-edge/ekuiper
      go 1.16
      require samplePlugin v0.0.0
      replace samplePlugin => ../samplePlugin   # replace samplePlugin with local samplePlugin module
      ```
   3. After these steps, the plugin and eKuiper project will look like this
      ```
      ekuiper
        sample.mod         //new added sample.mod file in step2
        go.mod             //existing ekuiper default mod file
        extensions.mod     //existing extensions mod file for plugins
      samplePlugin
        go.mod             //new plugin project default mod file
      ```
 4. compile eKuiper and plugin inside the eKuiper project
       ```shell
         # compile the eKuiper
       go build -trimpath -o $eKuiperPath/_build/$build/bin/kuiperd cmd/kuiperd/main.go
    
         # compile the plugin that using self-managed project within eKuiper project
       go build -trimpath -modfile sample.mod --buildmode=plugin -o $eKuiperPath/_build/$build/plugins/sinks/Mysql@v1.0.0.so ../samplePlugin/sinks/mysql.go
       ```

#### Docker compile

eKuiper provides different docker images for different purpose. The development docker image should be used for compiling plugins. From 0.4.0, the eKuiper image with tag x.x.x (e.g. `lfedge/ekuiper:0.4.0`) is the development docker image. Compared with the running version, the development version provides the development environment of Go, which lets users compile the plugin that can be completely compatible with the officially published version of eKuiper. The compiling steps in docker are as follows:
1. Run docker of the development version of eKuiper. Users need to mount the local plugin directory to the directory in docker, and then they can access and compile the plugin project in docker. The author's plugin project is located in the local `/var/git` directory. We map the local directory `/var/git` to the `/home` directory in docker by using the following commands.
    ```go
    docker run -d --name kuiper-dev --mount type=bind,source=/var/git,target=/home lfedge/ekuiper:1.3.0
    ```
2. The principle of compiling plugins in docker environment is the same as the local compilation. The compiled plugin is locating in the target directory of the plugin project.
    1. get into the compiling docker environment  
       ```shell
         # In host
         docker exec -it kuiper-dev /bin/sh
       ```
    2. eKuiper main path set: in compiling docker image, eKuiper project located in `/go/kuiper` 
       ```shell
         # In docker instance
         export EKUIPER_SOURCE=/go/kuiper
       ```
    3. do the necessary steps listed in former compile locally steps, the structure should like this
       ``` 
       /go/kuiper
         go.mod
         sample.mod
       /home/samplePlugin
         sinks           
           mysql.go     
         go.mod
       ```
    4. go to the eKuiper project path and run the local command    
       ```shell 
         # In docker instance
         # Add the sample.mod to eKuiper project then run the below build command
         go build -trimpath -modfile sample.mod --buildmode=plugin -o /home/samplePlugin/target/plugins/sinks/Mysql@v1.0.0.so /home/samplePlugin/sinks/mysql.go
       ```
You can use below sample shell script in your plugin project to automatically build and package the plugins. Please modify the variables at the beginning of the script to meet the requirements of different environments.

```shell script
export EKUIPER_SOURCE=/go/kuiper
export PLUGIN_SOURCE=/home/samplePlugin
export PLUGIN_TARGET=$PLUGIN_SOURCE/plugins
export VERSION=0.0.1


mkdir -p $PLUGIN_TARGET/sinks

# replace eKuiper dependency with local eKuiper
go mod edit -replace github.com/emqx/kuiper=$EKUIPER_SOURCE

# go to eKuiper main path
cd $EKUIPER_SOURCE

cat <<EOF >$EKUIPER_SOURCE/sample.mod
module github.com/lf-edge/ekuiper
go 1.16
require samplePlugin v0.0.0
replace samplePlugin => $PLUGIN_SOURCE
EOF

go mod download -modfile sample.mod github.com/go-sql-driver/mysql
go build -trimpath -modfile sample.mod  --buildmode=plugin -o $PLUGIN_TARGET/sinks/Mysql@v$VERSION.so $PLUGIN_SOURCE/sinks/mysql.go

cd $PLUGIN_SOURCE


echo $PLUGIN_TARGET/sinks/Mysql@v$VERSION.so

## zip the output
zip -o $PLUGIN_TARGET/sinks/mysql.zip $PLUGIN_TARGET/sinks/Mysql@v$VERSION.so
```

#### Debug and run the plugin

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

The plugin should use the same environment as the production environment eKuiper to compile in principle. If the production environment is eKuiper docker, should use the dev docker environment that has the same version as the production environment to compile the plugin. For example, if the production environment uses docker mirroring [lfedge/ekuiper:0.4.0-slim](https://registry.hub.docker.com/layers/lfedge/ekuiper/0.4.0-alpine/images/sha256-f79e9afd020a05f443d1864ee08007fe472e0d15e266d48a1f636fbd0343d507?context=explore), the plugin should be compiled in [lfedge/ekuiper:0.4.0](https://registry.hub.docker.com/layers/lfedge/ekuiper/0.4.0/images/sha256-dcc1420cbbd501aedd1bfe4093818a69726de1d6365974b69e99e1d5bc671836?context=explore) environment.

Please refer [Docker compile](#Docker-compile) for the compilation process. The compiled plugin can be tested in the Development Docker image before deploying.

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


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
  - The version of the same libraries plugin and eKuiper depend on must be completely the same
  - The plugin needs to be completely consistent with GOPATH of the eKuiper compilation environment

These limitations are relatively strict, and they almost require compiling and running the plugin and eKuiper on the same machine. It often results in the plugin which complied by the development environment can not be used in producing eKuiper. This article gives a detailed introduction to one reliable and available plugin development environment setting and process, which is recommended to the eKuiper plugin developer to use. Generally, the process for development and usage of the plugin is as follows:

- Development
  - Create and develop the plugin project
  - Compile and debug the plugin
- Deployment
  - Compile plugins which can be used for the production environment
  - Deploy plugins to the production environment

## Plugin development

Developing plugin is generally carried out in the development environment. eKuiper plugins will be deployed to the
production environment after passing debugging and running the development environment. The eKuiper plugin has three
types: **sources**,**functions** and **sinks**, for the detailed method of plugin
development: [LF Edge eKuiper extension](../../overview.md). This article will take the Sink plugin as an example to
introduce the process of plugin development and deployment. We will develop a basic MySql sink, for write stream output
data to the MySql database. The workflow list as followings:

- Create the plugin project
- Create file mysql.go under the sinks directory
- Edit file mysql.go for implementing the plugin
  - Implement [api.TupleCollector](https://github.com/lf-edge/ekuiper/blob/master/contract/api/sink.go) interface. If
    the output data is in a standard format, you can also implement the api.BytesCollector interface.
  - Export Symbol: Mysql. It could be a constructor function so that each rule can instantiate an own mysql plugin instance. Or it could be the struct which means every rule will share a singleton of the plugin. If the plugin has states like the connection, the first approach is preferred.
- Edit go.mod, add Mysql driver module
- Build the eKuiper and plugin

### Compose the Plugin

Users usually need to create a new project outside of the eKuiper project to develop customized plugins, to manage code
more conveniently. It's recommended to use Go module to develop plugin projects, the code structure of project is listed
as following.

```text
samplePlugin
  sinks           //source code directory of the plugin sink
    mysql.go
  go.mod          //file go module
```

Next, users need to edit the `mysql.go` file to implement the plugin code. Here is a sample `mysql.go` source code for
reference:

```go
package main

import (
  "database/sql"
  "fmt"

  _ "github.com/go-sql-driver/mysql"
  "github.com/lf-edge/ekuiper/contract/v2/api"
  "github.com/mitchellh/mapstructure"
)

type mysqlConfig struct {
  Url   string `json:"url"`
  Table string `json:"table"`
}

type mysqlSink struct {
  conf *mysqlConfig
  //数据库连接实例
  db *sql.DB
}

func (m *mysqlSink) Provision(ctx api.StreamContext, configs map[string]any) error {
  cfg := &mysqlConfig{}
  config := &mapstructure.DecoderConfig{
    TagName: "json",
    Result:  cfg,
  }
  decoder, err := mapstructure.NewDecoder(config)
  if err != nil {
    return err
  }
  err = decoder.Decode(configs)
  if err != nil {
    return fmt.Errorf("read properties %v fail with error: %v", configs, err)
  }
  if cfg.Url == "" {
    return fmt.Errorf("property Url is required")
  }
  if cfg.Table == "" {
    return fmt.Errorf("property Table is required")
  }
  m.conf = cfg
  ctx.GetLogger().Infof("mysql provisioning started with props: %v", cfg)
  return nil
}

func (m *mysqlSink) Connect(ctx api.StreamContext) error {
  ctx.GetLogger().Debugf("Opening mysql sink %v", m.conf)
  var err error
  m.db, err = sql.Open("mysql", m.conf.Url)
  return err
}

// 该函数为数据处理简化函数。
func (m *mysqlSink) Collect(ctx api.StreamContext, item api.MessageTuple) error {
  ctx.GetLogger().Debugf("mysql sink receive %s", item)
  v, ok := item.Value("name", "")
  if !ok {
    return fmt.Errorf("receive value does not have name field")
  }
  //TODO 此处列名写死。生产环境中一般可从item中的键值对获取列名
  sql := fmt.Sprintf("INSERT INTO %s (`name`) VALUES ('%s')", m.conf.Table, v)
  ctx.GetLogger().Debugf(sql)
  insert, err := m.db.Query(sql)
  if err != nil {
    return err
  }
  defer insert.Close()
  return nil
}

// 该函数为数据处理简化函数。
func (m *mysqlSink) CollectList(ctx api.StreamContext, item api.MessageTupleList) error {
  ctx.GetLogger().Debugf("mysql sink receive %s", item)
  if item.Len() <= 0 {
    return fmt.Errorf("receive empty item")
  }
  item.RangeOfTuples(func(index int, tuple api.MessageTuple) bool {
    v, ok := tuple.Value("", "name")
    if !ok {
      return false
    }
    //TODO 此处列名写死。生产环境中一般可从item中的键值对获取列名
    sql := fmt.Sprintf("INSERT INTO %s (`name`) VALUES ('%s')", m.conf.Table, v)
    ctx.GetLogger().Debugf(sql)
    insert, err := m.db.Query(sql)
    if err != nil {
      return false
    }
    defer insert.Close()
    return true
  })
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

Plugin development requires extending interfaces within eKuiper, so it must depend on the eKuiper contract project (a
sub-project of the eKuiper project). The simplest `go.mod` should also include a dependency on the contract. A
typical `go.mod` is as follows:

```go
module samplePlugin

go 1.24

require (
github.com/lf-edge/ekuiper/contract/v2 v2.0.0-alpha.5
)
```

mysql.go also have a dependency for mysql package, so the finial go.mod is this.

```go
module samplePlugin

go 1.24

require (
github.com/lf-edge/ekuiper/contract/v2 v2.0.0
github.com/go -sql-driver/mysql v1.5.0
)
```

**Note**: The Go version of the plugin project and the version of the dependent contract project must be exactly the
same as the eKuiper main project. Additionally, try to avoid the plugin project depending on the main project, as any
minor changes to the main project will cause the plugin to become invalid and require recompilation.

### Compile and debug the plugin

The environment of compiling the plugin should be consistent with that of  eKuiper. In the development environment, the typical usage is that locally download and compile eKuiper and plugin, and then debug plugin functions in the local eKuiper,  or compile the plugin in the docker container of eKuiper and use the eKuiper container to debug it.

#### Compile locally

Users can use the following method to build the plugin.

```shell
   go build -trimpath --buildmode=plugin -o Mysql@v1.0.0.so ./sinks/mysql.go
```

In the plugin project, the compilation will produce `Mysql@v1.0.0.so` for the next step of debugging and deployment.

**Note**: There are restrictions on the naming of plugins. See details in [Plugin Overview](../overview.md).

#### Docker compile

eKuiper provides different docker images for different purpose. The development docker image should be used for
compiling plugins. Since 1.7.1, the development docker image tag format is `x.x.x-dev`(From 0.4.0 to 1.7.0, the tag
format is x.x.x) . Compared with the running version, the development version provides the development environment of
Go, which lets users compile the plugin that can be completely compatible with the officially published version of
eKuiper.

1. Run docker of the development version of eKuiper. Users need to mount the local plugin directory to the directory in docker, and then they can access and compile the plugin project in docker. The author's plugin project is located in the local `/var/git` directory. We map the local directory `/var/git` to the `/go/plugins` directory in docker by using the following commands.

    ```shell
    docker run -d --name kuiper-dev --mount type=bind,source=/var/git,target=/go/plugins lfedge/ekuiper:2.0.0
    ```

2. The principle of compiling plugins in docker environment is the same as the local compilation. The compiled plugin is locating in the target directory of the plugin project.
    1. get into the compiling docker environment

       ```shell
         # In host
         docker exec -it kuiper-dev /bin/sh
       ```

3. Enter the plugin directory `/go/plugins` and execute the following command:

    ```shell
      # In docker instance
      go build -trimpath --buildmode=plugin -o ./kuiper/_build/$build/plugins/sinks/Mysql@v1.0.0.so ./samplePlugin/sinks/mysql.go
    ```

eKuiper offers an Alpine version of its image, but it does not come with the Go environment pre-installed. To compile
plugins using the Alpine image, users will need to install the necessary dependencies themselves. Alternatively, users
can opt to use the Golang image as their base environment, which includes the Go environment and simplifies the plugin
compilation process(If you are using the golang 1.24 version image and want to compile eKuiper plugins, you can use the
provided base image (https://github.com/lf-edge/ekuiper/pkgs/container/ekuiper%2Fbase) as the base environment. Plugins
compiled using this base image will not encounter the "Error loading shared library libresolve.so.2" when deployed to
the alpine version of eKuiper). Here are the specific steps to follow when using the Golang image as the base
environment:

1. To use the Golang image as the base environment, you'll need to make sure that you have the correct version of the Golang image installed. Additionally, you'll need to mount the local plugin directory and eKuiper source code into a directory within Docker, so that you can access and compile the plugin project within the Docker environment.
Assuming that your plugin project is located in the local directory `/var/git`, you can map this directory to the `/go/plugins` directory within Docker using the following command:

    ```shell
    docker run --rm -it -v /var/git:/go/plugins -w /go/plugins golang:1.24.5 /bin/sh
    ```

2. To obtain the compiled plugin, execute the following command:

    ``` shell
   # In docker instance
   go build -trimpath --buildmode=plugin -o Mysql@v1.0.0.so ./samplePlugin/sinks/mysql.go
   ```

#### Debug and run the plugin

Run eKuiper in the local or **Develop** Docker, create streams and rules, set action of the rule to mysql, then users can test the customized mysql sink plugin. Please refer [eKuiper documentation](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/getting_started/getting_started.md) for the steps of creating streams and rules. The following provides a rule using the mysql plugin for reference.

```json
{
  "id": "ruleTest",
  "sql": "SELECT * from demo",
  "actions": [
    {
      "log": {},
      "mysql":{
        "url": "user:password@tcp(localhost:3306)/database",
        "table": "test"
      }
    }
  ]
}
```

**note**: The interface implemented in mysql.go inserts data into a table only with column name. During development testing, it is also fine to manually copy the compiled .so file and the .yaml file(if any) to the corresponding folders and then restart eKuiper. In development docker image, the default eKuiper location is `/usr/local/kuiper`. It should be noted that `loading the new version after compiling the plugin again needs to restart eKuiper`.

## Plugin deployment

If the production environment and development environment are different, the developed plugin needs to be compiled again and deployed to the production environment. Assuming that the production environment adopts eKuiper docker to deploy, this article will describe how to deploy the plugin to the production environment.

### Compilation

The plugin should use the same environment as the production environment eKuiper to compile in principle. If the production environment is eKuiper docker, should use the dev docker environment that has the same version as the production environment to compile the plugin. For example, if the production environment uses docker mirroring [lfedge/ekuiper:0.4.0-slim](https://registry.hub.docker.com/layers/lfedge/ekuiper/0.4.0-alpine/images/sha256-f79e9afd020a05f443d1864ee08007fe472e0d15e266d48a1f636fbd0343d507?context=explore), the plugin should be compiled in [lfedge/ekuiper:0.4.0](https://registry.hub.docker.com/layers/lfedge/ekuiper/0.4.0/images/sha256-dcc1420cbbd501aedd1bfe4093818a69726de1d6365974b69e99e1d5bc671836?context=explore) environment.

Please refer [Docker compile](#Docker-compile) for the compilation process. The compiled plugin can be tested in the Development Docker image before deploying.

### Deployment

Users can use [REST API](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/restapi/plugins.md) or [CLI](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/cli/plugins.md) to manage plugins. The following takes the REST API as an example to deploy the plugin compiled in the previous step to the production environment.

1. Package the plugin and put it into the http server. Package the file `.so` of the plugin compiled in the previous step and the default configuration file (only required for source) `.yaml` into a `.zip` file (assuming that the file is `mysqlSink.zip`). Put this file into the http server that the production environment can also access.
    - Some plugin may depend on libs that are not installed on eKuiper environment. The user can either install them manually in the eKuiper server or put the install script and dependencies in the plugin zip and let the plugin management system do the installation. Please refer to [Plugin File Format](../../../api/restapi/plugins.md#plugin-file-format) for detail.
2. Use REST API to create plugins:

   ```shell
   POST http://{$production_eKuiper_ip}:9081/plugins/sinks
   Content-Type: application/json
 
   {"name":"mysql","file":"http://{$http_server_ip}/plugins/sinks/mysqlSink.zip"}
   ```

3. Verify whether the plugin was created successfully or not

    ```shell
    GET http://{$production_eKuiper_ip}:9081/plugins/sinks/mysql
    ```

    Return

    ```json
    {
       "name": "mysql",
       "version": "1.0.0"
    }
    ```

Note: if you intend to deploy the plugin in an Alpine environment, you may encounter an error stating `Error loading shared library libresolve.so.2` after completing the above steps(We are planning to develop a dedicated image for alpine system development, namely the alpine-dev version. Please stay tuned for our latest updates and developments.). In such a case, here is a solution you can try:

```shell
# In docker instance
apk add gcompat
cd /lib
ln libgcompat.so.0  /usr/lib/libresolve.so.2
```

So far, the plugin has been deployed successfully. Users can create rules with mysql sink for verification.

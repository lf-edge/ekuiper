# eKuiper 插件开发教程

[LF Edge eKuiper - 基于 SQL 的轻量级流式数据处理软件](https://github.com/lf-edge/ekuiper) 提供了一套插件机制用于实现自定义源（source），目标（sink）以及 SQL 函数（function）以扩展流处理功能。本教程详细介绍了 eKuiper 插件的开发编译和部署过程。

## 概览

eKuiper 插件机制基于 Go 语言的插件机制，使用户可以构建松散耦合的插件程序，在运行时动态加载和绑定。同时，由于 GO 语言插件系统的限制， eKuiper 插件的编译和使用也有相应的限制：

- 插件不支持 windows 系统
- 插件编译环境要求跟 eKuiper 编译环境尽量一致，包括但不限于
  - 相同的 GO 版本
  - 插件与 eKuiper 自身依赖的相同包版本必须完全一致
  - 插件与 eKuiper 编译环境的 GOPATH 必须完全一致

这些限制较为苛刻，几乎要求插件和 eKuiper 在同一台机器编译运行，经常导致开发环境编译出的插件无法在生产 eKuiper 上使用。本文详细介绍了一种切实可用的插件开发环境设置和流程，推荐给 eKuiper 插件开发者使用。插件的开发和使用一般有如下流程：

- 开发
  - 创建并开发插件项目
  - 编译调试插件
- 部署
  - 编译生产环境可用插件
  - 部署插件到生产环境

## 插件开发

插件开发一般在开发环境中进行。在开发环境调试运行通过后再部署到生产环境中。eKuiper
插件有三种类型：源，函数和目标，插件开发的详细方法请参看 [LF Edge eKuiper 扩展](../../overview.md) 。本文以目标(sink)
为例，介绍插件的开发部署过程。我们将开发一个最基本的 MySql 目标，用于将流输出写入到 MySql 数据库中。涉及到的工作流程大致如下：

- 新建名为 samplePlugin 的插件项目
- 在 sinks 目录下，新建 mysql.go 文件
- 编辑 mysql.go 文件以实现插件
  - 实现 [api.TupleCollector](https://github.com/lf-edge/ekuiper/blob/master/contract/api/sink.go) 接口。若输出数据为标准格式，也可以实现
    api.BytesCollector 接口。
  - 导出 Symbol：Mysql。它既可以是一个“构造函数”，也可以是结构体本身。当导出构造函数时，使用该插件的规则初始化时会用此函数创建该插件的实例；当导出为结构体时，所有使用该插件的规则将公用该插件同一个单例。如果插件有状态，例如数据库连接，建议使用第一种方法。
- 编辑 go.mod, 添加 mysql 驱动模块
- 编译构建 eKuiper 和目标插件

### 编写插件

为了便于代码管理，一般应当在 eKuiper 项目之外另建项目开发自定义插件。插件项目建议使用 Go module，项目目录如下图所示：

```text
samplePlugin
  sinks           //source code directory of the plugin sink
    mysql.go
  go.mod          //file go module
```

下一步用户需要编辑 mysql.go 文件，实现插件代码。这里有一份 mysql.go 源码可供参考：

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

插件开发需要扩展 eKuiper 内的接口，因此必须依赖于 eKuiper contract 项目（eKuiper 项目的子项目）。最简单的 go.mod 也需要包含对
contract 的依赖。典型的 go.mod 如下：

```go
module samplePlugin

go 1.24

require (
github.com/lf-edge/ekuiper/contract/v2 v2.0.0-alpha.5
)
```

除此之外，mysql.go 还有对 mysql 包的依赖，所以 go.mod 完整代码如下

```go
module samplePlugin

go 1.24

require (
github.com/lf-edge/ekuiper/contract/v2 v2.0.0
 github.com/go-sql-driver/mysql v1.5.0
)
```

**请注意**：插件项目的 go 版本和依赖的 contract 项目版本必须与 eKuiper
主项目完全一致。此外，尽量避免插件项目依赖主项目，否则主项目任何小改动都会导致插件失效，需要重新编译。

### 编译调试插件

编译插件应当与编译 eKuiper 的环境一致。在开发环境中，典型的用法是在本地下载并编译 eKuiper 和插件，然后在本地 eKuiper 上调试插件功能；也可以在 eKuiper 的 docker 容器中编译插件，并用 eKuiper 容器运行调试。

#### 本地编译

用户可以在插件项目用以下命令来编译插件：

```shell
   go build -trimpath --buildmode=plugin -o Mysql@v1.0.0.so ./sinks/mysql.go
```

在插件项目中将编译出 `Mysql@v1.0.0.so` 用于下一步的调试部署。

**注意**：插件命名有限制，详见[插件总览](../overview.md)。

#### Docker编译

eKuiper 提供了开发版本 docker 镜像。从 1.7.1 开始，开发镜像为 x.x.x-dev (0.4.0 到 1.7.0 之间版本的开发镜像为
x.x.x，例如`lfedge/ekuiper:0.4.0`。)；与运行版本相比，开发版提供了 go 开发环境，使得用户可以在编译出在 eKuiper
正式发布版本中完全兼容的插件。

1. 运行 eKuiper 开发版本 docker。需要把本地插件目录 mount 到 docker 里的目录中，这样才能在 docker 中访问插件项目并编译。笔者的插件项目位于本地 `/var/git` 目录。下面的命令中，我们把本地的 `/var/git`目录映射到 docker 内的 `/go/plugins` 目录中。

   ```shell
   docker run -d --name kuiper-dev --mount type=bind,source=/var/git,target=/go/plugins lfedge/ekuiper:2.0.0
   ```

2. 在 docker 环境中编译插件，其原理与本地编译一致。编译出的插件置于插件项目的 target 目录中
   1. 进入开发版本docker容器中

      ```shell
       # In host
       docker exec -it kuiper-dev /bin/sh
      ```

   2. 进入插件目录 /go/plugins 目录，执行下面命令

      ```shell
      # In docker instance
      go build -trimpath --buildmode=plugin -o ./kuiper/_build/$build/plugins/sinks/Mysql@v1.0.0.so ./samplePlugin/sinks/mysql.go
      ```

eKuiper 也提供了精简的 alpine 版本，但是不包含 go 环境。用户可以使用 alpine 版本的镜像来编译插件，但这就需要用户自己安装相应的依赖。用户也可以使用
golang 镜像作为基础环境(如果您使用的是 golang 1.24 版本的镜像，并且想要编译 eKuiper 插件，您可以使用提供的 base
image (<https://github.com/lf-edge/ekuiper/pkgs/container/ekuiper%2Fbase>)作为基础环境。使用这个 base image 所编译的插件，在部署到
alpine版本 的 eKuiper 时，不会出现`Error loading shared library libresolve.so.2`的错误)。具体步骤如下:

1. 运行 golang 相应版本 docker。需要把本地插件目录和 eKuiper 源码 mount 到 docker 里的目录中，这样才能在 docker 中访问插件项目并编译。笔者的插件项目位于本地 `/var/git` 目录。下面的命令中，我们把本地的 `/var/git` 目录映射到 docker 内的 `/go/plugins` 目录中。

   ```shell
   docker run --rm -it -v /var/git:/go/plugins -w /go/plugins golang:1.24.1 /bin/sh
   ```

2. 执行下面命令，便可以得到编译好的插件

   ```shell
   # In docker instance
   go build -trimpath --buildmode=plugin -o Mysql@v1.0.0.so ./samplePlugin/sinks/mysql.go
   ```

### 调试运行插件

在本地或 **开发** Docker 中启动 eKuiper，创建流和规则，规则的 action 设置为 mysql 即可对自定义的 mysql sink 插件进行测试。创建流和规则的步骤请参考 [eKuiper 文档](https://github.com/lf-edge/ekuiper/blob/master/docs/zh_CN/getting_started/getting_started.md) 。以下提供一个使用了 mysql 插件的规则供参考。

```text
{
  "id": "ruleTest",
  "sql": "SELECT * from demo",
  "actions": [
    {
      "log": {},
      "mysql":{
        "url": "user:test@tcp(localhost:3306)/database",
        "table": "test"
      }
    }
  ]
}
```

**注意**：mysql.go 中实现的接口在表中插入数据时只能插入列名为name的数据。此外，开发调试中，也可以直接把插件 so 文件复制到相应 plugins 目录下，并重启 eKuiper 进行调试。开发环境的 Docker 镜像，eKuiper 默认在 `/usr/local/kuiper` 目录下。需要注意的是，`插件重新编译后需要重启 eKuiper 才能载入新的版本`。

## 插件部署

eKuiper 生产环境和开发环境如果不同，开发的插件需要重新编译并部署到生产环境。假设生产环境采用 eKuiper docker 进行部署，本节将描述如何部署插件到生产环境中。

### 编译

插件原则上应该与生产环境 eKuiper 采用相同环境进行编译。假设生产环境为 eKuiper docker，则应当采用与生产环境相同版本的 dev docker 环境编译插件。例如，生产环境采用 [lfedge/ekuiper:0.4.0-slim](https://registry.hub.docker.com/layers/lfedge/ekuiper/0.4.0-alpine/images/sha256-f79e9afd020a05f443d1864ee08007fe472e0d15e266d48a1f636fbd0343d507?context=explore) 的docker镜像，则插件需要在[lfedge/ekuiper:0.4.0](https://registry.hub.docker.com/layers/lfedge/ekuiper/0.4.0/images/sha256-dcc1420cbbd501aedd1bfe4093818a69726de1d6365974b69e99e1d5bc671836?context=explore) 的环境中进行编译。

编译过程请参考 [Docker 编译](#Docker编译)。编译完成的插件可以直接在开发 Docker 中进行调试。

### 部署

可以采用 [REST API](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/restapi/plugins.md) 或者 [CLI](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/cli/plugins.md) 进行插件管理。下文以 REST API 为例，将上一节编译的插件部署到生产环境中。

1. 插件打包并放到 http 服务器。将上一节编译好的插件 `.so` 文件及默认配置文件（只有 source 需要） `.yaml` 文件一起打包到一个 `.zip` 文件中，假设为 `mysqlSink.zip`。把该文件放置到生产环境也可访问的 http 服务器中。
   - 某些插件可能依赖 eKuiper 环境未安装的库。用户可以选择自行到 eKuiper 服务器安装依赖或者在插件包中放入名为 install.sh 安装脚本和依赖。插件管理系统会运行插件包中的 install.sh 文件。详情请参考 [插件文件格式](../../../api/restapi/plugins.md#插件文件格式)。

2. 使用 REST API 创建插件：

   ```shell
   POST http://{$production_eKuiper_ip}:9081/plugins/sinks
   Content-Type: application/json

   {"name":"mysql","file":"http://{$http_server_ip}/plugins/sinks/mysqlSink.zip"}
   ```

3. 验证插件是否创建成功

   ```shell
   GET http://{$production_eKuiper_ip}:9081/plugins/sinks/mysql
   ```

   返回

   ```json
   {
     "name": "mysql",
     "version": "1.0.0"
   }
   ```

注意：如果是在 alpine 环境中部署插件，执行上述步骤后，可能会出现 `Error loading shared library libresolve.so.2` 错误（我们计划开发一个针对 alpine 的专门用于开发的镜像，即 alpine-dev 版本的镜像，敬请期待），这里提供了一种解决方案：

```shell
# In docker instance
apk add gcompat
cd /lib
ln libgcompat.so.0  /usr/lib/libresolve.so.2
```

至此，插件部署成功。可以创建带有 mysql sink 的规则进行验证。

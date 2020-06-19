# Kuiper 插件开发教程

[EMQ X Kuiper - 基于 SQL 的轻量级流式数据处理软件](https://github.com/emqx/kuiper)提供了一套插件机制用于实现自定义源（source），目标（sink）以及 SQL 函数（function）以扩展流处理功能。本教程详细介绍了Kuiper插件的开发编译和部署过程。

## 概览

Kuiper 插件机制基于 Go 语言的插件机制，使用户可以构建松散耦合的插件程序，在运行时动态加载和绑定。同时，由于 GO 语言插件系统的限制， Kuiper 插件的编译和使用也有相应的限制：
- 插件不支持 windows 系统
- 插件编译环境要求跟 Kuiper 编译环境尽量一致，包括但不限于
    - 相同的 GO 版本
    - 插件与 Kuiper 自身依赖的相同包版本必须完全一致，包括Kuiper自身
    - 插件与 Kuiper 编译环境的GOPATH必须完全一致
    
这些限制较为苛刻，几乎要求插件和 Kuiper 在同一台机器编译运行，经常导致开发环境编译出的插件无法在生产 Kuiper 上使用。本文详细介绍了一种切实可用的插件开发环境设置和流程，推荐给 Kuiper 插件开发者使用。插件的开发和使用一般有如下流程：

- 开发
    - 创建并开发插件项目
    - 编译调试插件
- 部署
    - 编译生产环境可用插件
    - 部署插件到生产环境

## 插件开发

插件开发一般在开发环境中进行。在开发环境调试运行通过后再部署到生产环境中。

### 创建并开发插件项目

Kuiper 项目源代码的 plugins 目录下有一些插件范例。用户自定义的插件也可以在 Kuiper 项目中开发。但是为了便于代码管理，一般应当在 Kuiper 项目之外另建项目开发自定义插件。插件项目建议使用 Go module，典型的项目目录如下图所示：

```
plugin_project
  sources         //源（source）插件源代码目录
    mysource.go
  sinks           //目标（sink）插件源代码目录
    mysink.go
  functions       //函数（function）插件源代码目录
    myfunction.go
  target          //编译结果目录     
  go.mod          //go module文件
```

插件开发需要扩展 Kuiper 内的接口，因此必须依赖于 Kuiper 项目。最简单的 go.mod 也需要包含对 Kuiper 的依赖。典型的 go.mod 如下：
```go
module samplePlugin

go 1.13

require (
	github.com/emqx/kuiper v0.0.0-20200323140757-60d00241372b
)
```

Kuiper 插件有三种类型，源代码可放入对应的目录中。插件开发的详细方法请参看 [EMQ X Kuiper 扩展](https://github.com/emqx/kuiper/blob/master/docs/en_US/extension/overview.md)。本文以目标（sink)为例，介绍插件的开发部署过程。我们将开发一个最基本的 MySql 目标，用于将流输出写入到 MySql 数据库中。

- 新建名为 samplePlugin 的插件项目，采用上文的目录结构
- 在 sinks 目录下，新建 mysql.go 文件
- 编辑 mysql.go 文件以实现插件
    -  实现 [api.Sink](https://github.com/emqx/kuiper/blob/master/xstream/api/stream.go)接口
    - 导出 Symbol：Mysql。它既可以是一个“构造函数”，也可以是结构体本身。当导出构造函数时，使用该插件的规则初始化时会用此函数创建该插件的实例；当导出为结构体时，所有使用该插件的规则将公用该插件同一个单例。如果插件有状态，例如数据库连接，建议使用第一种方法。
- 编辑 go.mod, 添加 mysql 驱动模块

mysql.go 完整代码如下
```go
package main

// 该例子为简化样例，仅建议测试时使用

import (
	"database/sql"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/api"
	_ "github.com/go-sql-driver/mysql"
)

type mysqlConfig struct {
	url           string   `json:"url"`
	table         string   `json:"table"`
}

type mysqlSink struct {
	conf *mysqlConfig
	//数据库连接实例
	db   *sql.DB
}

func (m *mysqlSink) Configure(props map[string]interface{}) error {
	cfg := &mysqlConfig{}
	err := common.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	if cfg.url == ""{
		return fmt.Errorf("property url is required")
	}
	if cfg.table == ""{
		return fmt.Errorf("property table is required")
	}
	return nil
}

func (m *mysqlSink) Open(ctx api.StreamContext) (err error) {
	logger := ctx.GetLogger()
	logger.Debug("Opening mysql sink")
	m.db, err = sql.Open("mysql", m.conf.url)
	return
}

// 该函数为数据处理简化函数。
func (m *mysqlSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	if v, ok := item.([]byte); ok {
		//TODO 生产环境中需要处理item unmarshall后的各种类型。
        // 默认的类型为 []map[string]interface{}
        // 如果sink的`dataTemplate`属性有设置，则可能为各种其他的类型		
		logger.Debugf("mysql sink receive %s", item)
		//TODO 此处列名写死。生产环境中一般可从item中的键值对获取列名
		sql := fmt.Sprintf("INSERT INTO %s (`name`) VALUES ('%s')", m.conf.table, v)
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
 go.mod 完整代码如下
 ```go
module samplePlugin

go 1.13

require (
	github.com/emqx/kuiper v0.0.0-20200323140757-60d00241372b
	github.com/go-sql-driver/mysql v1.5.0
)
```

### 编译调试插件

编译插件应当与编译 Kuiper 的环境一致。在开发环境中，典型的用法是在本地下载并编译 Kuiper 和插件，然后在本地 Kuiper 上调试插件功能；也可以在 Kuiper 的 docker 容器中编译插件，并用 Kuiper 容器运行调试。

#### 本地编译

开发者可以在本地自行编译 Kuiper 和插件进行调试。其步骤如下：
1. 下载 Kuiper 源代码 `git clone https://github.com/emqx/kuiper.git`
2. 编译 Kuiper：在 Kuiper 目录下，运行 `make`
3. 编译插件：
    1. 在插件项目下，运行`go mod edit -replace github.com/emqx/kuiper=$kuiperPath`，使得 Kuiper 依赖指向本地 Kuiper，请替换$kuiperPath 到步骤1下载目录，下同。
   2. 编译插件 so 到 Kuiper 插件目录下
   ```go
    go build --buildmode=plugin -o $kuiperPath/_build/$build/plugins/sinks/Mysql@v1.0.0.so sinks/mysql.go
    ```

### Docker 编译

从0.3.0版本开始，Kuiper 提供了开发版本 docker 镜像。其中， 0.4.0及之后版本的开发镜像为x.x.x，例如``kuiper:0.4.0``；而0.3.x版本的开发镜像名为x.x.x-dev，例如``kuiper:0.3.0-dev``。与运行版本相比，开发版提供了 go 开发环境，使得用户可以在编译出在 Kuiper 正式发布版本中完全兼容的插件。Docker 中编译步骤如下：
1. 运行 Kuiper 开发版本 docker。需要把本地插件目录 mount 到 docker 里的目录中，这样才能在 docker 中访问插件项目并编译。笔者的插件项目位于本地`/var/git`目录。下面的命令中，我们把本地的`/var/git`目录映射到docker内的`/home`目录中。
    ```go
    docker run -d --name kuiper-dev --mount type=bind,source=/var/git,target=/home emqx/kuiper:0.3.0-dev
    ```
2. 在 docker 环境中编译插件，其原理与本地编译一致。编译出的插件置于插件项目的 target 目录中
    ```go
    -- In host
    # docker exec -it kuiper-dev /bin/sh
    
    -- In docker instance
    # cd /home/samplePlugin
    # go mod edit -replace github.com/emqx/kuiper=/go/kuiper
    # go build --buildmode=plugin -o /home/samplePlugin/target/plugins/sinks/Mysql@v1.0.0.so sinks/mysql.go
    ```

### 调试运行插件

在本地或 Docker 中启动 Kuiper，创建流和规则，规则的 action 设置为 mysql 即可对自定义的 mysql sink 插件进行测试。创建流和规则的步骤请参考[ Kuiper 文档](https://github.com/emqx/kuiper/blob/master/docs/zh_CN/getting_started.md)。以下提供一个使用了 mysql 插件的规则供参考。
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

需要注意的是，插件重新编译后需要重启 Kuiper 才能载入新的版本。

## 插件部署

Kuiper 生产环境和开发环境如果不同，开发的插件需要重新编译并部署到生产环境。假设生产环境采用 Kuiper docker 进行部署，本节将描述如何部署插件到生产环境中。

### 插件编译

插件原则上应该与生产环境 Kuiper 采用相同环境进行编译。假设生产环境为 Kuiper docker，则应当采用与生产环境相同版本的 dev docker 环境编译插件。例如，生产环境采用 [emqx/kuiper:0.3.0](https://registry.hub.docker.com/layers/emqx/kuiper/0.3.0/images/sha256-0e3543d33f6f8c56de044d5ff001fd39b9e26f82219ca5fd25605953ed33580e?context=explore)的 docker 镜像，则插件需要在[emqx/kuiper:0.3.0-dev](https://registry.hub.docker.com/layers/emqx/kuiper/0.3.0-dev/images/sha256-a309d3821b55b01dc01c4f4a04e83288bf5526325f0073197387f2ca425260d0?context=explore) 的环境中进行编译。

编译过程请参考[ Docker 编译](#docker编译)。

### 插件部署

可以采用 [REST API](https://github.com/emqx/kuiper/blob/master/docs/en_US/restapi/plugins.md) 或者 [CLI](https://github.com/emqx/kuiper/blob/master/docs/en_US/cli/plugins.md) 进行插件管理。下文以 REST API 为例，将上一节编译的插件部署到生产环境中。

1. 插件打包并放到 http 服务器。将上一节编译好的插件 `.so` 文件及默认配置文件（只有 source 需要） `.yaml` 文件一起打包到一个 `.zip` 文件中，假设为 `mysqlSink.zip`。把该文件放置到生产环境也可访问的 http 服务器中。
   - 某些插件可能依赖Kuiper环境未安装的库。用户可以选择自行到Kuiper服务器安装依赖或者在插件包中放入名为install.sh安装脚本和依赖。插件管理系统会运行插件包中的install.sh文件。详情请参考[ 插件文件格式](../restapi/plugins.md#plugin-file-format)。
2. 使用 REST API 创建插件：
   ```
   POST http://{$production_kuiper_ip}:9081/plugins/sinks
   Content-Type: application/json
   
   {"name":"mysql","file":"http://{$http_server_ip}/plugins/sinks/mysqlSink.zip"}
   ```
3. 验证插件是否创建成功
    ```
    GET http://{$production_kuiper_ip}:9081/plugins/sinks/mysql
    ```
    返回
    ```json
    {
       "name": "mysql",
       "version": "1.0.0"
    }
    ```

至此，插件部署成功。可以创建带有 mysql sink 的规则进行验证。

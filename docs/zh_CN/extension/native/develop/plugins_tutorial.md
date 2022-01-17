# eKuiper 插件开发教程

[LF Edge eKuiper - 基于 SQL 的轻量级流式数据处理软件](https://github.com/lf-edge/ekuiper) 提供了一套插件机制用于实现自定义源（source），目标（sink）以及 SQL 函数（function）以扩展流处理功能。本教程详细介绍了 eKuiper 插件的开发编译和部署过程。

## 概览

eKuiper 插件机制基于 Go 语言的插件机制，使用户可以构建松散耦合的插件程序，在运行时动态加载和绑定。同时，由于 GO 语言插件系统的限制， eKuiper 插件的编译和使用也有相应的限制：
- 插件不支持 windows 系统
- 插件编译环境要求跟 eKuiper 编译环境尽量一致，包括但不限于
    - 相同的 GO 版本
    - 插件与 eKuiper 自身依赖的相同包版本必须完全一致，包括 eKuiper 自身
    - 插件与 eKuiper 编译环境的 GOPATH 必须完全一致
    

这些限制较为苛刻，几乎要求插件和 eKuiper 在同一台机器编译运行，经常导致开发环境编译出的插件无法在生产 eKuiper 上使用。本文详细介绍了一种切实可用的插件开发环境设置和流程，推荐给 eKuiper 插件开发者使用。插件的开发和使用一般有如下流程：

- 开发
    - 创建并开发插件项目
    - 编译调试插件
- 部署
    - 编译生产环境可用插件
    - 部署插件到生产环境

## 插件开发

插件开发一般在开发环境中进行。在开发环境调试运行通过后再部署到生产环境中。由于Go语言插件机制的严格限制，我们在这里提供两种行之有效的构建插件开发环境的办法：在eKuiper工程中创建插件开发环境与在eKuiper工程外创建插件开发环境。eKuiper 插件有三种类型：源，函数和目标，插件开发的详细方法请参看 [LF Edge eKuiper 扩展](../../overview.md) 。本文以目标（sink)为例，介绍插件的开发部署过程。我们将开发一个最基本的 MySql 目标，用于将流输出写入到 MySql 数据库中。涉及到的工作流程大致如下：

- 新建名为 samplePlugin 的插件项目
- 在 sinks 目录下，新建 mysql.go 文件
- 编辑 mysql.go 文件以实现插件
    -  实现 [api.Sink](https://github.com/lf-edge/ekuiper/blob/master/pkg/api/stream.go) 接口
    - 导出 Symbol：Mysql。它既可以是一个“构造函数”，也可以是结构体本身。当导出构造函数时，使用该插件的规则初始化时会用此函数创建该插件的实例；当导出为结构体时，所有使用该插件的规则将公用该插件同一个单例。如果插件有状态，例如数据库连接，建议使用第一种方法。
- 编辑 go.mod, 添加 mysql 驱动模块
- 编译构建eKuiper和目标插件

### 在eKuiper中创建插件项目

当用户以这种方式创建插件项目时，首先必须下载一份eKuiper源码并在项目根目录下执行`make`命令。在项目源码extensions目录中有一些插件事例。以此种方式开发插件项目的好处是官方现存的所有插件均以此种方式开发，新插件开发者可以快速上手而不用重新建立项目，用户可以直接把代码放到extensions目录下，代码结构如下：
```
extensions
  sinks           
    mysink.go
  go.mod         
```
extensions目录用Go module来管理依赖包，用户只需把他们的插件源码放入合适的目录，然后在go.mod中更新依赖即可。

下一步用户需要编辑mysql.go文件，实现插件代码。这里有一份mysql.go源码可供参考：

```go
package main

 // 该例子为简化样例，仅建议测试时使用

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
	//数据库连接实例
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

// 该函数为数据处理简化函数。
func (m *mysqlSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	v, _, err := ctx.TransformOutput(data)
	if err != nil {
		logger.Error(err)
		return err
	}
		
	//TODO 生产环境中需要处理item unmarshall后的各种类型。
	// 默认的类型为 []map[string]interface{}
	// 如果sink的`dataTemplate`属性有设置，则可能为各种其他的类型	
	logger.Debugf("mysql sink receive %s", item)
	//TODO 此处列名写死。生产环境中一般可从item中的键值对获取列名
	sql := fmt.Sprintf("INSERT INTO %s (`name`) VALUES ('%s')", m.conf.Table, v)
	logger.Debugf(sql)
	insert, err := m.db.Query(sql)
	if err != nil {
		return err
	}
	defer insert.Close()
	
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

### 在eKuiper外创建插件项目

为了便于代码管理，一般应当在 eKuiper 项目之外另建项目开发自定义插件。插件项目建议使用 Go module，项目目录如下图所示：

```
samplePlugin
  sinks           //source code directory of the plugin sink
    mysql.go
  target          //directory of compiling results   
  go.mod          //file go module
```
这里的mysql.go文件可以参考上一节的代码。 插件开发需要扩展 eKuiper 内的接口，因此必须依赖于 eKuiper 项目。最简单的 go.mod 也需要包含对 eKuiper 的依赖。典型的 go.mod 如下：
```go
module samplePlugin

go 1.17

require (
	github.com/lf-edge/ekuiper v0.0.0-20200323140757-60d00241372b
)
```
除此之外，mysql.go还有对mysql包的依赖，所以go.mod 完整代码如下
 ```go
module samplePlugin

go 1.17

require (
	github.com/lf-edge/ekuiper v0.0.0-20200323140757-60d00241372b
	github.com/go-sql-driver/mysql v1.5.0
)
 ```

### 编译调试插件

编译插件应当与编译 eKuiper 的环境一致。在开发环境中，典型的用法是在本地下载并编译 eKuiper 和插件，然后在本地 eKuiper 上调试插件功能；也可以在 eKuiper 的 docker 容器中编译插件，并用 eKuiper 容器运行调试。

#### 本地编译

如果用户选择在eKuiper项目中开发插件，那么他可以用以下命令来编译插件：
```shell
   # compile the eKuiper
   go build -trimpath -o ./_build/$build/bin/kuiperd cmd/kuiperd/main.go
    
   # compile the plugin that using the extensions folder within eKuiper project
   go build -trimpath -modfile extensions.mod --buildmode=plugin -o ./_build/$build/plugins/sinks/Mysql@v1.0.0.so extensions/sinks/mysql/mysql.go

```

然后如果开发者选择了自己创建插件项目， 那么他需要以下步骤来编译插件：
1. 下载 eKuiper 源代码 `git clone https://github.com/lf-edge/ekuiper.git`
2. 编译 eKuiper：在 eKuiper 目录下，运行 `make`
3. 设置eKuiper项目目录: 将第一步的下载目录设置为环境变量`eKuiperPath`，例如`export eKuiperPath=/go/ekuiper`   
4. 编译环境设置：
    1. 在插件项目下，运行 `go mod edit -replace github.com/lf-edge/ekuiper=$eKuiperPath`，使得 eKuiper 依赖指向本地 eKuiper，这里$eKuiperPath将被替换为步骤1下载目录，下同。这一步执行后的效果，这里有个例子:
       ```go
          module samplePlugin
          go 1.17

          require (
            github.com/lf-edge/ekuiper v0.0.0-20200323140757-60d00241372b
            github.com/go-sql-driver/mysql v1.5.0
          )
          replace github.com/lf-edge/ekuiper => /go/ekuiper # replace ekuiper with local ekuiper module
       ```
    2. 由于 Go 语言插件系统对依赖的路径有非常严格的要求，为了确保插件可以顺利运行，建议在 eKuiper 主项目里进行编译。在 eKuiper 项目里，添加如下名为 sample.mod 的文件并让它指向真正的插件项目，以便于插件编译。如果你的eKuiper与插件项目在同一目录，这里有个例子：
       ```
       module github.com/lf-edge/ekuiper
       go 1.17
       require samplePlugin v0.0.0
       replace samplePlugin => ../samplePlugin   # replace samplePlugin with local samplePlugin module
       ```
    3. 经过这些配置，你的插件项目与eKuiper项目目录结构应该是这样
       ``` 
        ekuiper
          sample.mod         //new added sample.mod file in step2
          go.mod             //existing ekuiper default mod file
          extensions.mod     //existing extensions mod file for plugins
        samplePlugin
          go.mod             //new plugin project default mod file
       ``
       
5. 在 eKuiper 目录下，编译插件和eKuiper
   ```shell
    # compile the eKuiper
    go build -trimpath -o $eKuiperPath/_build/$build/bin/kuiperd cmd/kuiperd/main.go

    # compile the plugin that using self-managed project within eKuiper project
    go build -trimpath -modfile sample.mod --buildmode=plugin -o $eKuiperPath/_build/$build/plugins/sinks/Mysql@v1.0.0.so ../samplePlugin/sinks/mysql.go
   ```

#### Docker编译

eKuiper 提供了开发版本 docker 镜像。其中， 0.4.0及之后版本的开发镜像为x.x.x，例如`lfedge/ekuiper:0.4.0`；与运行版本相比，开发版提供了 go 开发环境，使得用户可以在编译出在 eKuiper 正式发布版本中完全兼容的插件。Docker 中编译步骤如下：
1. 运行 eKuiper 开发版本 docker。需要把本地插件目录 mount 到 docker 里的目录中，这样才能在 docker 中访问插件项目并编译。笔者的插件项目位于本地 `/var/git` 目录。下面的命令中，我们把本地的 `/var/git`目录映射到 docker 内的 `/home` 目录中。
    ```go
    docker run -d --name kuiper-dev --mount type=bind,source=/var/git,target=/home lfedge/ekuiper:1.3.0
    ```
2. 在 docker 环境中编译插件，其原理与本地编译一致。编译出的插件置于插件项目的 target 目录中
   1. 进入开发版本docker容器中
   ```shell
    # In host
    docker exec -it kuiper-dev /bin/sh
   ``` 
   2. 设置eKuiper工程环境目录：在开发版docker环境中，eKuiper工程位于`/go/kuiper`
    ```shell
        # In docker instance
        export EKUIPER_SOURCE=/go/kuiper
    ```
   3. 参照本地编译环境设置方法，设置编译环境，目录结构如下
    ``` 
      /go/kuiper
        go.mod
        sample.mod
      /home/samplePlugin
        sinks           
          mysql.go     
        go.mod
    ```    
    4. 进入eKuiper主目录，执行下面命令
    ``` shell
    # In docker instance
    # 在 eKuiper 项目中添加 sample.mod 然后运行如下命令进行编译
    go build -trimpath --buildmode=plugin -o /home/samplePlugin/target/plugins/sinks/Mysql@v1.0.0.so /home/samplePlugin/sinks/mysql.go
    ```

在插件项目中可以使用如下 shell 脚本自动编译及打包插件。修改脚本开头的参数以满足不同环境下的开发调试需求。

```shell script
#!/bin/sh
export EKUIPER_SOURCE=/go/kuiper
export PLUGIN_SOURCE=/home/samplePlugin
export PLUGIN_TARGET=$PLUGIN_SOURCE/plugins
export VERSION=0.0.1


mkdir -p $PLUGIN_TARGET/sinks

# replace eKuiper dependency with local eKuiper
go mod edit -replace github.com/lf-edge/ekuiper=$EKUIPER_SOURCE

# go to eKuiper main path
cd $EKUIPER_SOURCE

cat <<EOF >$EKUIPER_SOURCE/sample.mod
module github.com/lf-edge/ekuiper
go 1.17
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

### 调试运行插件

在本地或 **开发** Docker 中启动 eKuiper，创建流和规则，规则的 action 设置为 mysql 即可对自定义的 mysql sink 插件进行测试。创建流和规则的步骤请参考[ eKuiper 文档](https://github.com/lf-edge/ekuiper/blob/master/docs/zh_CN/getting_started.md) 。以下提供一个使用了 mysql 插件的规则供参考。
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

开发调试中，也可以直接把插件 so 文件复制到相应 plugins 目录下，并重启 eKuiper 进行调试。开发环境的Docker 镜像，eKuiper默认在 `/usr/local/kuiper` 目录下。需要注意的是，插件重新编译后需要重启  eKuiper 才能载入新的版本。

## 插件部署

eKuiper 生产环境和开发环境如果不同，开发的插件需要重新编译并部署到生产环境。假设生产环境采用 eKuiper docker 进行部署，本节将描述如何部署插件到生产环境中。

### 插件编译

插件原则上应该与生产环境 eKuiper 采用相同环境进行编译。假设生产环境为 eKuiper docker，则应当采用与生产环境相同版本的 dev docker 环境编译插件。例如，生产环境采用 [lfedge/ekuiper:0.4.0-slim](https://registry.hub.docker.com/layers/lfedge/ekuiper/0.4.0-alpine/images/sha256-f79e9afd020a05f443d1864ee08007fe472e0d15e266d48a1f636fbd0343d507?context=explore) 的docker镜像，则插件需要在[lfedge/ekuiper:0.4.0](https://registry.hub.docker.com/layers/lfedge/ekuiper/0.4.0/images/sha256-dcc1420cbbd501aedd1bfe4093818a69726de1d6365974b69e99e1d5bc671836?context=explore) 的环境中进行编译。

编译过程请参考[ Docker 编译](#Docker编译)。编译完成的插件可以直接在开发 Docker 中进行调试。

### 插件部署

可以采用 [REST API](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/restapi/plugins.md) 或者 [CLI](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/cli/plugins.md) 进行插件管理。下文以 REST API 为例，将上一节编译的插件部署到生产环境中。

1. 插件打包并放到 http 服务器。将上一节编译好的插件 `.so` 文件及默认配置文件（只有 source 需要） `.yaml` 文件一起打包到一个 `.zip` 文件中，假设为 `mysqlSink.zip`。把该文件放置到生产环境也可访问的 http 服务器中。
   
   - 某些插件可能依赖 eKuiper 环境未安装的库。用户可以选择自行到 eKuiper 服务器安装依赖或者在插件包中放入名为 install.sh 安装脚本和依赖。插件管理系统会运行插件包中的 install.sh 文件。详情请参考[ 插件文件格式](../../../operation/restapi/plugins.md#plugin-file-format)。
2. 使用 REST API 创建插件：
   ```
   POST http://{$production_eKuiper_ip}:9081/plugins/sinks
   Content-Type: application/json
   
   {"name":"mysql","file":"http://{$http_server_ip}/plugins/sinks/mysqlSink.zip"}
   ```
3. 验证插件是否创建成功
    ```
    GET http://{$production_eKuiper_ip}:9081/plugins/sinks/mysql
    ```
    返回
    ```json
    {
       "name": "mysql",
       "version": "1.0.0"
    }
    ```

至此，插件部署成功。可以创建带有 mysql sink 的规则进行验证。

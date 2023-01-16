# 序列化

eKuiper 计算过程中使用的是基于 Map 的数据结构，因此 source/sink 连接外部系统的过程中，通常需要进行编解码以转换格式。在 source/sink 中，都可以通过配置参数 `format` 和 `schemaId` 来指定使用的编解码方案。

## 格式

编解码的格式分为两种：有模式和无模式的格式。当前 eKuiper 支持的格式有 `json`, `binary`, `delimiter`, `protobuf`
和 `custom`。其中，`protobuf` 为有模式的格式。
有模式的格式需要先注册模式，然后在设置格式的同时，设置引用的模式。例如，在使用 mqtt sink 时，可配置格式和模式：

```json
{
  "mqtt": {
    "server": "tcp://127.0.0.1:1883",
    "topic": "sample",
    "format": "protobuf",
    "schemaId": "proto1.Book"
  }
}
```

所有格式都提供了编解码的能力，同时也可选地提供了数据结构的定义，即模式。编解码的计算可内置，如 JSON 解析；可动态解析模式进行编解码，如 Protobuf 解析 `*.proto` 文件；也可使用用户自定义的静态插件(`*.so`)进行解析。其中，静态解析的性能最好，但是需要另外编写代码并编译成插件，变更较为困难。动态解析使用更为灵活。

当前所有支持的格式，及其支持的编解码方法和模式如下表所示：

| 格式        | 编解码                    | 自定义编解码 | 模式    |
|-----------|------------------------|--------|-------|
| json      | 内置                     | 不支持    | 不支持   |
| binary    | 内置                     | 不支持    | 不支持   |
| delimiter | 内置，必须配置 `delimiter` 属性 | 不支持    | 不支持   |
| protobuf  | 内置                     | 支持     | 支持且必需 |
| custom    | 无内置                    | 支持且必需  | 支持且可选 |


### 格式扩展

当用户使用 `custom` 格式或者 `protobuf` 格式时，可采用 go 语言插件的形式自定义格式的编解码和模式。其中，`protobuf` 仅支持自定义编解码，模式需要通过 `*.proto` 文件定义。自定义格式的步骤如下：

1. 实现编解码相关接口。其中，Encode 编码函数将传入的数据(当前总是为 map[string]interface{}) 编码为字节数组。而 Decode 解码函数则相反，将字节数组解码为 map[string]interface{}。解码函数在 source 中被调用，而编码函数将在 sink 中调用。
    ```go
    // Converter converts bytes & map or []map according to the schema
    type Converter interface {
        Encode(d interface{}) ([]byte, error)
        Decode(b []byte) (interface{}, error)
    }
    ```
2. 实现数据结构描述接口（格式为 custom 时可选）。若自定义的格式为强类型，则可实现该接口。接口返回一个类 JSON schema 的字符串，供 source 使用。返回的数据结构将作为一个物理 schema 使用，帮助 eKuiper 实现编译解析阶段的 SQL 验证和优化等能力。
    ```go
    type SchemaProvider interface {
	    GetSchemaJson() string
    }
    ```
3. 编译为插件 so 文件。通常格式的扩展无需依赖 eKuiper 的主项目。由于 Go 语言插件系统的限制，插件的编译仍然需要在与 eKuiper 主程序相同的编译环境中进行，包括操作相同，Go 语言版本等。若需要部署到官方 docker 中，则可使用对应的 docker 镜像进行编译。详细步骤，请参考[格式插件编译](#使用-docker-编译格式插件).
    ```shell
    go build -trimpath --buildmode=plugin -o data/test/myFormat.so internal/converter/custom/test/*.go
    ```
4. 通过 REST API 进行模式注册。
    ```shell
    ###
    POST http://{{host}}/schemas/custom
    Content-Type: application/json
    
    {
      "name": "custom1",
       "soFile": "file:///tmp/custom1.so"
    }
    ```
5. 在 source 或者 sink 中，通过 `format` 和 `schemaId` 参数使用自定义格式。

完整的自定义格式可参考 [myFormat.go](https://github.com/lf-edge/ekuiper/blob/master/internal/converter/custom/test/myformat.go)。该文件定义了一个简单的自定义格式，编解码实际上仅调用 JSON 进行序列化。它返回了一个数据结构，可用于 eKuiper source 的数据结构推断。

#### 使用 Docker 编译格式插件

由于go插件的限制，最好在与 eKuiper 构建环境相同的环境中构建格式插件。官方的 eKuiper docker 镜像和二进制文件都是在两个操作系统中构建的：debian 和 alpine。除了默认的 docker镜像如 `1.8.0` 和 `1.8.0-apline` 之外，其他镜像和二进制文件都使用 debian 系统构建。

要在 debian 环境下构建，请使用相应的 dev 镜像来构建。例如，要构建 1.8.0 的格式插件，请使用`1.8.0-dev` 镜像。

要在 alpine 环境下构建，我们可以使用 golang alpine 镜像作为基础环境。具体步骤如下:

1. 在你的插件项目中，创建一个 Makefile，并确保该插件能被 `make` 命令构建。请查看[示例项目](https://github.com/lf-edge/ekuiper/tree/master/internal/converter/custom/test)以获得参考。
2. 检查你的 eKuiper 的版本所使用的 golang 版本。检查相应 eKuiper 版本的[ docker 文件](https://github.com/lf-edge/ekuiper/blob/master/deploy/docker/Dockerfile)中的 `GO_VERSION` 参数。如果版本是 `1.18.5`，则使用 `golang:1.18.5-alpine` docker 镜像进行构建。
3. 切换到你的项目位置，然后在你的项目位置启动 golang docker 容器，安装依赖项，然后执行`make`，确保构建成功。
   ```shell
   cd ${yourProjectLoc}
   docker run --rm -it -v "$PWD":/usr/src/myapp -w /usr/src/myapp golang:1.18.5-alpine sh
   ### inside docker container
   /usr/src/myapp # apk add gcc make libc-dev
   /usr/src/myapp # make
   ```
4. 你应该在你的项目中找到为你的插件建立的 *.so 文件（在这个例子中是 test.so）。用它来注册格式插件。

### 静态 Protobuf

使用 Protobuf 格式时，我们支持动态解析和静态解析两种方式。使用动态解析时，用户仅需要在注册模式时指定 proto 文件。在解析性能要求更高的条件下，用户可采用静态解析的方式。静态解析需要开发解析插件，其步骤如下：

1. 已有 proto 文件 helloworld.proto, 使用官方 protoc 工具生成 go 代码。详情参见[ Protocol Buffer 文档](https://developers.google.com/protocol-buffers/docs/reference/go-generated)。
   ```shell
   protoc --go_opt=Mhelloworld.proto=com.main --go_out=. helloworld.proto
   ```
2. 将生成的代码 helloworld.pb.go 移动到 go 语言项目（此处名为 test）中，包名重命名为 main 。
3. 创建包装类。对于每个消息类型，实现 3 个方法 `Encode`, `Decode`, `GetXXX`。编解码中主要是进行消息的 struct 与 map 类型的转换。需要注意的是，为了保证性能，不要使用反射。
4. 编译为插件 so 文件。通常格式的扩展无需依赖 eKuiper 的主项目。由于 Go 语言插件系统的限制，插件的编译仍然需要在与 eKuiper 主程序相同的编译环境中进行，包括操作相同，Go 语言版本等。若需要部署到官方 docker 中，则可使用对应的 docker 镜像进行编译。
   ```shell
    go build -trimpath --buildmode=plugin -o data/test/helloworld.so internal/converter/protobuf/test/*.go
   ```
5. 通过 REST API 进行模式注册。需要注意的是，proto 文件和 so 文件都需要指定。
    ```shell
    ###
    POST http://{{host}}/schemas/protobuf
    Content-Type: application/json
    
    {
      "name": "helloworld",
      "file": "file:///tmp/helloworld.proto",
       "soFile": "file:///tmp/helloworld.so"
    }
    ```
6. 在 source 或者 sink 中，通过 `format` 和 `schemaId` 参数使用自定义格式。

完整的静态 protobuf 插件可参考 [helloworld protobuf](https://github.com/lf-edge/ekuiper/tree/master/internal/converter/protobuf/test)。


## 模式

模式是一套元数据，用于定义数据结构。例如，Protobuf 格式中使用 .proto 文件作为模式定义传输的数据格式。目前，eKuiper 仅支持 protobuf 和 custom 这两种模式。


### 模式注册

模式采用文件的形式存储。用户可以通过配置文件或者 API 进行模式的注册。模式的存放位置位于 `data/schemas/${type}`。例如，protobuf 格式的模式文件，应该放置于 `data/schemas/protobuf`。

eKuiper 启动时，将会扫描该配置文件夹并自动注册里面的模式。若需要在运行中注册或管理模式，可通过模式注册表 API 来完成。API 的操作会作用到文件系统中。

### 模式注册表 API

用户可使用模式注册表 API 在运行时对模式进行增删改查。详情请参考：

- [模式注册表 REST API](../../api/restapi/schemas.md)
- [模式注册表 CLI](../../api/cli/schemas.md)
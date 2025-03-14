# 原生插件开发

用户可以使用 Go 语言原生插件系统，采用 Go 语言编写 Source，Sink 和函数实现。不管开发哪种类型的插件，都需要经过以下步骤：

1. 创建插件项目。
2. 根据插件扩展的类型，编写插件的实现逻辑。
3. 构建插件 so。
4. 将插件 so 和元数据/配置文件等依赖文件打包成插件 zip 包。

## 插件开发环境设置

需要使用与主项目完全相同版本的依赖项，特别是 (`github.com/lf-edge/ekuiper/contract/v2`) 来构建插件。用户可以自行管理插件项目，确保
go.mod 中的 go 语言版本和与主项目相同的依赖模块的版本一致。

例如，开发对应 eKuiper v2.0.0 版本的插件时，需要先看 eKuiper 对应版本的 go.mod 文件。确保插件项目的 go 版本和 contract mod
版本一致。例如以下的插件 go.mod 中，使用了 contract mod v2.0.0 版本，go 1.24.0 版本。

```go.mod
module mycompany.com/myplugin

require github.com/lf-edge/ekuiper/contract/v2 v2.0.0

go 1.24.0
```

## 插件开发

插件的开发就是根据插件类型实现特定的接口，并导出具有特定名称的实现。导出的符合名称必须与插件名称相同。插件支持两种类型的导出symbol:

1. 导出一个构造函数：eKuiper 将使用构造函数为每次加载创建一个插件实现的新实例。因此，每条规则将有一个插件实例，并且每个实例都将与其他实例隔离。这是推荐的方式。以下示例导出名为
   Random 的 Source 构造函数。

    ```go
    func Random() api.Source {
        return random.GetSource()
    }
    ```

2. 导出一个实例：eKuiper
   将使用该实例作为所有插件加载的单例。因此，所有规则将共享相同的实例。对于这种实现，开发人员需要处理共享状态，以避免任何潜在的多线程问题。在没有共享状态且性能至关重要的情况下，建议使用此模式。函数扩展通常是没有内部状态的函数，适合这种模式。以下示例导出名为
   Random 的 Source 实例。

    ```go
      var Random = random.GetSource()
    ```

扩展实现数据源（source），数据汇（sink）和函数（function）分别需要实现不同的接口。详情请参考：

- [源扩展](./source.md)
- [Sink 扩展](./sink.md)
- [函数扩展](./function.md)

### 状态存储

eKuiper 扩展通过 context 参数暴露了一个基于键值对的状态存储接口，可用于所有类型的扩展，包括 Source，Sink 和 Function 扩展.

状态为键值对，其中键为 string 类型而值为任意数据。键的作用域仅为当前扩展的实例。

用户可通过 context 对象访问状态存储。状态相关方法包括 putState，getState，incrCounter，getCounter and deleteState。

以下代码为函数扩展访问状态的实例。该函数将计算传入的单词数，并将累积数目保存在状态中。

```go
func (f *accumulateWordCountFunc) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
logger := ctx.GetLogger()
err := ctx.IncrCounter("allwordcount", len(strings.Split(args[0], args[1])))
if err != nil {
return err, false
}
if c, err := ctx.GetCounter("allwordcount"); err != nil   {
return err, false
} else {
return c, true
}
}
```

### 运行时依赖

有些插件可能需要访问文件系统中的依赖文件。依赖文件建放置于 <span v-pre>
{{ekuiperPath}}/etc/{{pluginType}}/{{pluginName}}</span>
目录。打包插件时，依赖文件应放置于 [etc 目录](../../../api/restapi/plugins.md#插件文件格式)。安装后，这些文件会自动移动到推荐的位置。

在插件源代码中，开发者可通过 context 获取 eKuiper 根目录，以访问文件系统中的依赖：

```go
ctx.GetRootPath()
```

## 插件编译

插件代码编写完成后，用户需要使用 Go 语言编译工具编译出对应环境的插件 so 文件。**请注意**，插件必须与主项目 eKuiper
使用相同的编译环境进行编译。

- 用户自行编译 eKuiper 主程序：插件可在主程序编译环境进行编译。做插件开发时多为此场景。
- eKuiper 预编译二进制或默认 Docker image: 这些版本的 eKuiper 使用 alpine docker image 编译。具体版本可进入对应版本的
  Dockerfile 源代码 (deploy/docker/Dockerfile) 查看。插件应使用相同版本的 docker image 进行编译。
- eKuiper -slim 或者 -slim-python Docker image: 这些版本的 eKuiper 使用 debian docker image 编译。具体版本可进入对应版本的
  Dockerfile 源代码 (deploy/docker/Dockerfile-slim) 查看。插件应使用相同版本的 docker image 进行编译。

环境准备好之后，可以使用如下编译指令进行编译：

```bash
go build -trimpath --buildmode=plugin -o plugins/sources/MySource.so plugins/sources/my_source.go
```

### 命名

建议插件名使用 camel case 形式。插件命名有一些限制：

1. 插件 Export 的变量必须为**插件名的首字母大写形式**。 例如，插件名为 _file_ ，则其输出变量名必须为 _File_。
2. _.so_ 文件的名字必须与输出变量名或者插件名相同。例如， _MySource.so_ 或 _mySink.so_。

### 版本

用户可以**选择**将版本信息添加到 _.so_ 的名称中，以帮助识别插件的版本。然后可以通过 describe CLI 命令或 REST API
检索版本信息。命名约定是在 _@_ 之后的名称中添加一个版本字符串。版本可以是任何字符串。如果版本字符串以 "v"
开头，则返回结果中将忽略 "v" 。下面是一些典型的例子。

- _MySource@v1.0.0.so_ ：版本是 1.0.0
- _MySource@20200331.so_ ：版本是 20200331

如果有多个具有相同名称的插件版本，则只有最新版本(按版本的字符串排序)将生效。

## 插件打包

插件编译完成后，需要将编译出的 so 文件，默认配置文件 xx.yaml (source 插件必需)，插件描述文件 xx.json 以及插件依赖的文件全部打包到
zip 文件中。zip文件名没有特殊要求，用户可以自行命名。**请注意**：所有文件都必须在 zip 的根目录下，不可有额外的文件夹。

## 进一步阅读

插件的开发打包过程较为繁琐，可跟随[插件教程](./plugins_tutorial.md)一步一步完成插件编写部署。

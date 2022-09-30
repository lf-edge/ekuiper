# Wasm 插件

作为对原生插件的补充，可移植插件旨在提供相同的功能，同时允许在更通用的环境中运行并由更多语言创建。

创建插件的步骤如下：

1. 开发插件
2. 根据编程语言构建或打包插件
3. 通过eKuiper文件/REST/CLI注册插件



# 开发

在 wasm 插件模式下，用选择的语言来实现函数，并将其编译成 wasm 文件。只要是 WebAssembly 支持的语言均可。

# 调试





# 打包发布

开发完成后，我们需要将结果打包成zip进行安装。在 zip 文件中，文件结构必须遵循以下约定并使用正确的命名：

- {pluginName}.json：文件名必须与插件主程序和REST/CLI命令中定义的插件名相同。
- 插件的 wasm 文件

在json文件中，我们需要描述这个插件的元数据。该信息必须与插件主程序中的定义相匹配。下面是一个例子：

```json
{
    "version": "v1.0.0",
    "functions": [
      "test"
    ],
    "wasmEngine": "wasmedge"
  }
```

一个插件可以包含多个函数，在 json 文件中的相应数组中定义它们。



# 管理

通过将内容（json、wasm文件）放在`plugins/portables/${pluginName}`中，可以在启动时自动加载可移植插件。

要在运行时管理可移植插件，我们可以使用 [REST](https://github.com/lf-edge/ekuiper/blob/master/docs/zh_CN/operation/restapi/plugins.md) 或 [CLI](https://github.com/lf-edge/ekuiper/blob/master/docs/zh_CN/operation/cli/plugins.md) 命令。
# Portable 插件 Python SDK 
用户可利用 Python SDK 来开发 portable 插件，这个 SDK 提供了类似原生插件的 API，另外它提供了启动函数，用户只需填充插件信息即可。

为了运行 python 插件，有两个前置条件
To run python plugin, there are two prerequisites in the runtime environment:
1. 安装 Python 3.x 环境.
2. 通过 `pip install nng ekuiper` 安装 nng 和 ekuiper 包.

默认情况下，eKuiper 的 portable 插件运行时会通过 `python` 命令来运行插件。如果您的环境不支持 `python` 命令，请通过[配置文件](../../configuration/global_configurations.md#portable-插件配置)更换为可用的 Python 命令。

## 插件开发

开发插件包括子模块和主程序两部分, Python SDK 提供了 python 语言的源，目标和函数 API。

源接口:
```python
  class Source(object):
    """abstract class for eKuiper source plugin"""

    @abstractmethod
    def configure(self, datasource: str, conf: dict):
        """configure with the string datasource and conf map and raise error if any"""
        pass

    @abstractmethod
    def open(self, ctx: Context):
        """run continuously and send out the data or error with ctx"""
        pass

    @abstractmethod
    def close(self, ctx: Context):
        """stop running and clean up"""
        pass
```

目标接口:
```python
class Sink(object):
    """abstract class for eKuiper sink plugin"""

    @abstractmethod
    def configure(self, conf: dict):
        """configure with conf map and raise error if any"""
        pass

    @abstractmethod
    def open(self, ctx: Context):
        """open connection and wait to receive data"""
        pass

    @abstractmethod
    def collect(self, ctx: Context, data: Any):
        """callback to deal with received data"""
        pass

    @abstractmethod
    def close(self, ctx: Context):
        """stop running and clean up"""
        pass
```

函数接口:
```python
class Function(object):
    """abstract class for eKuiper function plugin"""

    @abstractmethod
    def validate(self, args: List[Any]):
        """callback to validate against ast args, return a string error or empty string"""
        pass

    @abstractmethod
    def exec(self, args: List[Any], ctx: Context) -> Any:
        """callback to do execution, return result"""
        pass

    @abstractmethod
    def is_aggregate(self):
        """callback to check if function is for aggregation, return bool"""
        pass
```
用户通过实现这些抽象接口来创建自己的源，目标和函数，然后在主函数中声明这些自定义插件的实例化方法

```python
if __name__ == '__main__':
    c = PluginConfig("pysam", {"pyjson": lambda: PyJson()}, {"print": lambda: PrintSink()},
                     {"revert": lambda: revertIns})
    plugin.start(c)
```

关于更详细的信息，请参考这篇文章 [python sdk example](https://github.com/lf-edge/ekuiper/tree/master/sdk/python).

## 打包发布

由于 python 是解释性语言，不需要编译出可执行文件，需要确保 json 描述文件中可执行文件名字的准确性即可。详细信息，请[参考](./overview.md#打包发布)

## 部署要求

运行 python 脚本需要有 python 环境。所以，目标系统必须安装 python 3.x 环境。如果使用 docker ，建议使用 `lfedge/ekuiper:<tag>-slim-python` 版本。该版本包含 eKuiper 和 python 环境，无需再手动安装。
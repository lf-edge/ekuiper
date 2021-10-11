# Python SDK for Portable Plugin

By using Python SDK for portable plugins, user can develop portable plugins with python language. The Python SDK provides APIs for the source, sink and function interfaces. Additionally, it provides a plugin start function as the execution entry point to define the plugin and its symbols.

To run python plugin, there are two prerequisites in the runtime environment:
1. Install Python 3.x environment.
2. Install ekuiper package by `pip install ekuiper`.

## Development

The process is the same: develop the symbols and then develop the main program. Python SDK provides the similar source, sink and function interfaces in python language.

Source interface:
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

Sink interface:
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

Function interface:
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

Users need to create their own source, sink and function by implement these abstract classes. Then create the main program and declare the instantiation functions for these extensions like below:

```python
if __name__ == '__main__':
    c = PluginConfig("pysam", {"pyjson": lambda: PyJson()}, {"print": lambda: PrintSink()},
                     {"revert": lambda: revertIns})
    plugin.start(c)
```

For the full example, please check the [python sdk example](https://github.com/lf-edge/ekuiper/tree/master/sdk/python/example/pysam).

## Package

As python is an interpretive language, we don't need to build an executable for it. Just specify the main program python file in the plugin json file is ok. For detail, please check [packaing](./overview.md#package).
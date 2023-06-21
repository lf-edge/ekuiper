# Python SDK for Portable Plugin

By using Python SDK for portable plugins, user can develop portable plugins with python language. The Python SDK provides APIs for the source, sink and function interfaces. Additionally, it provides a plugin start function as the execution entry point to define the plugin and its symbols.

To run python plugin, there are two prerequisites in the runtime environment:

1. Install Python 3.x environment.
2. Install nng and ekuiper package by `pip install nng ekuiper`.

By default, the eKuiper portable plugin runtime will run python script with `python userscript.py`. If users have multiple python instance or an alternative python executable command, they can specify the python command in [the configuration file](../../configuration/global_configurations.md#portable-plugin-configurations).

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

For the full example, please check
the [python sdk example](https://github.com/lf-edge/ekuiper/tree/master/sdk/python/example/pysam).

## Package

As python is an interpretive language, we don't need to build an executable for it. Just specify the main program python
file in the plugin json file is ok. For detail, please check [packaging](./overview.md#package).

## Deployment requirements

Running python script requires the python environment. Make sure python 3.x are installed in the target environment. If
using docker image, we recommend to use tags like `lfedge/ekuiper:<tag>-slim-python` which have both eKuiper and python
environment.

### Virtual Environment

Virtual environments are a common and effective technique used in Python development which is useful for python
dependency management. Anaconda or Miniconda are one of the most popular environment manager for Python.
The [conda](https://conda.io/projects/conda/en/latest/index.html) package and environment manager is included in all
versions of Anaconda®, Miniconda, and Anaconda Repository. eKuiper supports to run the Python plugin with conda
environment.

To use conda environment, the common steps are:

1. Create and set up the conda environment.
2. When packaging the plugin, make sure `virtualEnvType` is set to `conda` and `env` is set to the created virtual
   environment. Below is an example.

    ```json
    {
      "version": "v1.0.0",
      "language": "python",
      "executable": "pysam.py",
      "virtualEnvType": "conda",
      "env": "myenv",
      "sources": [
        "pyjson"
      ],
      "sinks": [
        "print"
      ],
      "functions": [
        "revert"
      ]
    }
    ```

3. If the plugin has installation script, make sure the script install the dependencies to the correct environment.

# Load Wasm Plugin by File

There are 2 ways to install wasm plugins. One is to install by REST/CLI API. Another is to put all the plugin files with specified format into this path 'plugins/wasm'.

## Wasm Plugin Composition

For example, users can define a plugin named `fibonacci` . The definition will be presented as a json file as below:

```json
{
  "version": "v1.0.0",
  "language": "go",
  "functions": [
    "fib"
  ],
  "wasmFile": "/home/erfenjiao/ekuiper/plugins/wasm/fibonacci/fibonacci.wasm",
  "wasmEngine": "wasmedge"
}
```

## File Structure

Each wasm plugin requires the following structure:

- A top-level directory of the name of the plugin.
- A json file inside the directory of the name of the plugin.
- All other dependencies.

Take the `fibonacci` plugin as an example. To load it automatically, uses need to put it in this structure:

```text
plugins
  wasm
    fibonacci.json
    fibonacci.wasm
```

Notice that, the symbol name must be unique for a specific plugin type. By adding the plugin directory to `plugins/wasm`, the plugin will be loaded once eKuiper starts.
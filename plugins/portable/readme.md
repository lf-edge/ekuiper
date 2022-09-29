# Load Portable Plugin by File

There are 2 ways to install portable plugins. One is to install by REST/CLI API. Another is to put all the plugin files with specified format into this path 'plugins/portable'.

## Portable Plugin Composition

There are two levels of a portable plugin. Each plugin has one single executable which will be executed as a separated process in runtime. Uses can define multiple `symbols` inside the one plugin. Each symbol could be a source, sink or function. Thus, when defining a portable plugin, users can get a set of new source, sink and function registered.

For example, users can define a plugin named `car` and export many symbols for source, sink and function. The definition will be presented as a json file as below:

```json
{
  "name": "car",
  "version": "v1.0.0",
  "language": "go",
  "executable": "server",
  "sources": [
    "json","udp","sync"
  ],
  "sinks": [
    "command"
  ],
  "functions": [
    "link", "rank"
  ]
}
```

## File Structure

Each portable plugin requires the following structure:

- A top-level directory of the name of the plugin. 
- A json file inside the directory of the name of the plugin.
- An executable file inside the directory.
- All other dependencies.
- Config files (yaml and json) inside 'etc/$pluginType' for each symbol in that plugin.

Take the `car` plugin as an example. To load it automatically, uses need to put it in this structure:

```text
etc
  sources
    json.yaml
    json.json
    udp.yaml
    udp.json
    sync.yaml
    sync.json
  sinks
    command.json
  functions
    link.json
    rank.json
plugins
  portable
    car
      server
      car.json
```

Notice that, the symbol name must be unique for a specific plugin type. By adding the plugin directory to `plugins/portable`, the plugin will be loaded once eKuiper starts.
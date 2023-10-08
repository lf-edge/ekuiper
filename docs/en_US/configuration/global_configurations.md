# Basic configurations

The configuration file for eKuiper is at `$kuiper/etc/kuiper.yaml`. The configuration file is yaml format.
Application can be configured through environment variables. Environment variables are taking precedence over their counterparts
in the yaml file. In order to use env variable for given config we must use formatting as follows:
`KUIPER__` prefix + config path elements connected by `__`.
Example, in case of config:

```yaml
basic:
  # true|false, with debug level, it prints more debug info
  debug: false
  # true|false, if it's set to true, then the log will be print to console
  consoleLog: false
  # true|false, if it's set to true, then the log will be print to log file
  fileLog: true
  # syslog settings
  syslog:
    # true|false, if it's set to true, then the log will be print to syslog
    enable: false
    # The syslog protocol, tcp or udp; Leave empty if no remote syslog server is used
    network: udp
    # The syslog server address; Leave empty if no remote syslog server is used
    address: localhost:514
    # The syslog level, supports debug, info, warn, error
    level: info
    # The syslog tag; Leave empty if no tag is used
    tag: kuiper
  # How many hours to split the file
  rotateTime: 24
  # Maximum file storage hours
  maxAge: 72
  # Whether to ignore case in SQL processing. Note that, the name of customized function by plugins are case-sensitive.
  ignoreCase: false
  sql:
    # maxConnections indicates the max connections for the certain database instance group by driver and dsn sharing between the sources/sinks
    # 0 indicates unlimited
    maxConnections: 0
  # rulePatrolInterval indicates the patrol interval for the internal checker to reconcile the scheudle rule
  rulePatrolInterval: 10s
  # cfgStorageType indicates the storage type to store the config, support `file` and `kv`. When `cfgStorageType` is file, it will save configuration into File. When `cfgStorageType` is `kv`, it will save configuration into the storage defined in `store`
  cfgStorageType: file
```

for debug option in basic following env is valid `KUIPER__BASIC__DEBUG=true` and if used debug value will be set to true.

The configuration item **ignoreCase** is used to specify whether case is ignored in SQL processing. If it is true, the column name case of the input data can be different from that defined in SQL. If the column name case in SQL statements, stream definitions, and input data can be guaranteed to be exactly the same, it is recommended to set this value to "false" to obtain better performance. Before version 1.10, its default value was true to be compatible with standard SQL; after version 1.10, its default value was changed to false for better performance.

## Log level

```yaml
basic:
  # true|false, with debug level, it prints more debug info
  debug: false
  # true|false, if it's set to true, then the log will be print to console
  consoleLog: false
  # true|false, if it's set to true, then the log will be print to log file
  fileLog: true
  # How many hours to split the file
  rotateTime: 24
  # Maximum file storage hours
  maxAge: 72
```

## System log

When the user sets the value of the environment variable named KuiperSyslogKey to true or set syslog enable to true, the
log will be printed to the syslog. Additional syslog settings are as follows:

```yaml
# syslog settings
syslog:
  # true|false, if it's set to true, then the log will be print to syslog
  enable: false
  # The syslog protocol, tcp or udp; Leave empty if no remote syslog server is used
  network: udp
  # The syslog server address; Leave empty if no remote syslog server is used
  address: localhost:514
  # The syslog level, supports debug, info, warn, error
  level: info
  # The syslog tag; Leave empty if no tag is used
  tag: kuiper
```

All the above settings are optional. If the network and address are not set, the local syslog will be used. If the level
is not set, the default value is info. If the tag is not set, there will be no tag used.

## Timezone

```yaml
# The global time zone from the IANA time zone database, or UTC if not set.
timezone: UTC
```

The global time zone configuration based on the [IANA time zone database](https://www.iana.org/time-zones), if it is left blank, `UTC` will be used as the default time zone, and if it is set to `Local`, the system time zone will be used.

> Note: To use time zone configuration in an alpine-based environment, you need to ensure that the time zone data has been properly installed (e.g. `apk add tzdata`).

## Cli Addr

```yaml
basic:
  # CLI bind IP
  ip: 0.0.0.0
  # CLI port
  port: 20498
```

## Rest Service Configuration

```yaml
basic:
  # REST service bind IP
  restIp: 0.0.0.0
  # REST service port
  restPort: 9081
  restTls:
    certfile: /var/https-server.crt
    keyfile: /var/https-server.key
```

### restPort

The port for the rest api http server to listen to.

### restTls

The tls cert file path and key file path setting. If restTls is not set, the rest api server will listen on http. Otherwise, it will listen on https.

## authentication

eKuiper will check the `Token` for rest api when `authentication` option is true. please check this file for [more info](../api/restapi/authentication.md).

```yaml
basic:
  authentication: false
```

## Rule Patrol Configuration

```yaml
basic:
  rulePatrolInterval: "10s"
```

## Prometheus Configuration

eKuiper can export metrics to prometheus if `prometheus` option is true. The prometheus will be served with the port specified by `prometheusPort` option.

```yaml
basic:
  prometheus: true
  prometheusPort: 20499
```

For such a default configuration, eKuiper will export metrics and serve prometheus at `http://localhost:20499/metrics`.

The prometheus port can be the same as the eKuiper REST API port. If so, both service will be served on the same server.

## Pluginhosts Configuration

The URL where hosts all of pre-build [native plugins](../extension/native/overview.md). By default, it's at `packages.emqx.net`.

All plugins list as follows:

| plugin types | pre-build plugins                                              |
|--------------|----------------------------------------------------------------|
| source       | random zmq                                                     |
| sink         | file image influx redis tdengine zmq                           |
| function     | accumulateWordCount countPlusOne echo geohash image labelImage |

User can get all pre-build plugins names and address by below Rest-APIs:

```shell
GET http://localhost:9081/plugins/sources/prebuild
GET http://localhost:9081/plugins/sinks/prebuild
GET http://localhost:9081/plugins/functions/prebuild
```

After get the plugin info, users can try these plugins, [more info](../api/restapi/plugins.md)

**Note: only the official released debian based docker images support these operations**

## Rule configurations

Configure the default properties of the rule option. All the configuration can be overridden in rule level. Check [rule options](../guide/rules/overview.md#options) for detail.

## Sink configurations

Configure the default properties of sink, currently mainly used to configure [cache policy](../guide/sinks/overview.md#Caching). The same configuration options are available at the rules level to override these default configurations.

```yaml
  sink:
  # Control to disable cache or not. If it's set to true, then the cache will be disabled, otherwise, it will be enabled.
  enableCache: false

  # The maximum number of messages to be cached in memory.
  memoryCacheThreshold: 1024

  # The maximum number of messages to be cached in the disk.
  maxDiskCache: 1024000

  # The number of messages for a buffer page which is the unit to read/write to disk batchly to prevent frequent IO
  bufferPageSize: 256

  # The interval in millisecond to resend the cached messages
  resendInterval: 0

  # Whether to clean the cache when the rule stops
  cleanCacheAtStop: false
```

## Store configurations

### Configuration Storage

```yaml
basic:
  cfgStorageType: kv
```

When `basic.cfgStorageType` is kv, the underlying storage used by it will become `store.type`, and the contents of configurations will be stored in the specified storage in the form of key-value pairs.

There is possibility to configure storage of state for application. Default storage layer is sqlite database. There is option to set redis as storage.
In order to use redis as store type property must be changed into redis value.

### Sqlite

It has properties

* name - name of database file - if left empty it will be `sqliteKV.db`

### Redis

It has properties

* host     - host of redis
* port     - port of redis
* password - password used for auth in redis, if left empty auth won't be used
* timeout  - timeout fo connection
* connectionSelector - reuse the connection info defined in etc/connections/connection.yaml, mainly used for edgeX redis in secure mode
  * only applicable to redis connection information
  * the server, port and password in connection info will overwrite the host port and password above
  * [more info](../guide/sources/builtin/edgex.md#connectionselector)

### External State

There is also a configuration item named `extStateType`.
The configuration's usage is user can store some information in database in advance, when stream processing rules need
these information,
they can get them easily by [get_keyed_state](../sqls/functions/other_functions.md#getkeyedstate) function in SQL.

*Note*: `type` and `extStateType` can be configured differently.

### Config

```yaml
    store:
      #Type of store that will be used for keeping state of the application
      type: sqlite
      extStateType: redis
      redis:
        host: localhost
        port: 6379
        password: kuiper
        #Timeout in ms
        timeout: 1000
      sqlite:
        #Sqlite file name, if left empty name of db will be sqliteKV.db
        name:
```

## Portable plugin configurations

This section configures the portable plugin runtime.

```yaml
  portable:
      # The executable of python. Specify this if you have multiple python instances in your system
      # or other circumstance where the python executable cannot be successfully invoked through the default command.
      pythonBin: python
      # control init timeout in ms. If the init time is longer than this value, the plugin will be terminated.
      initTimeout: 5000
```

## Ruleset Provision

Support file based stream and rule provisioning on startup. Users can put a [ruleset](../api/restapi/ruleset.md#ruleset-format) file named `init.json` into `data` directory to initialize the ruleset. The ruleset will only be import on the first startup of eKuiper.

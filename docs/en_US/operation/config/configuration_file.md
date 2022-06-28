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
  # How many hours to split the file
  rotateTime: 24
  # Maximum file storage hours
  maxAge: 72
  # Whether to ignore case in SQL processing. Note that, the name of customized function by plugins are case-sensitive.
  ignoreCase: true
```
for debug option in basic following env is valid `KUIPER_BASIC_DEBUG=true` and if used debug value will be set to true.

Configuration **ignoreCase** is used to ignore case in SQL processing. By default, it's set to true to comply with standard SQL. In this case, data ingested can be case-insensitive. If the column names in the SQL, stream definition and the ingested data can be unified as a case-sensitive name, it is recommended to set to false to gain a better performance.

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
## system log
When the user sets the value of the environment variable named KuiperSyslogKey to true, the log will be printed to the syslog.
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
eKuiper will check the `Token` for rest api when `authentication` option is true. please check this file for [more info](authentication.md).

```yaml
basic:
  authentication: false
```

## Prometheus Configuration

eKuiper can export metrics to prometheus if `prometheus` option is true. The prometheus will be served with the port specified by `prometheusPort` option.

```yaml
basic:
  prometheus: true
  prometheusPort: 20499
```
For such a default configuration, eKuiper will export metrics and serve prometheus at `http://localhost:20499/metrics`

## Pluginhosts Configuration

The URL where hosts all of pre-build [native plugins](../../extension/native/overview.md). By default, it's at `packages.emqx.net`. 

All plugins list as follows:

| plugin types | pre-build plugins                                              |
|--------------|----------------------------------------------------------------|
| source       | random zmq                                                     |
| sink         | file image influx redis tdengine zmq                           |
| function     | accumulateWordCount countPlusOne echo geohash image labelImage |

User can get all pre-build plugins names and address by below Rest-APIs:

```
GET http://localhost:9081/plugins/sources/prebuild
GET http://localhost:9081/plugins/sinks/prebuild
GET http://localhost:9081/plugins/functions/prebuild
``` 

After get the plugin info, users can try these plugins, [more info](../restapi/plugins.md) 

**Note: only the official released debian based docker images support these operations**

## Sink configurations

Configure the default properties of sink, currently mainly used to configure [cache policy](../../rules/sinks/overview.md#Caching). The same configuration options are available at the rules level to override these default configurations.

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
  * [more info](../../rules/sources/builtin/edgex.md#connectionselector)
    

### Config
```yaml
    store:
      #Type of store that will be used for keeping state of the application
      type: sqlite
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
```
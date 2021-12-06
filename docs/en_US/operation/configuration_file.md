# Basic configurations

The configuration file for eKuiper is at ``$kuiper/etc/kuiper.yaml``. The configuration file is yaml format.
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

Configuration **ignoreCase** is used to ignore case in SQL processing. By default, it's set to true to comply with standard SQL. In this case, data ingested can be case-insensitive. If the column names in the SQL, stream definition and the ingested data can be unified as a case-sensitive name, it is recommended to set to true to gain a better performance.

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
eKuiper will check the ``Token`` for rest api when ``authentication`` option is true. please check this file for [more info](./authentication.md).

```yaml
basic:
  authentication: false
```

## Prometheus Configuration

eKuiper can export metrics to prometheus if ``prometheus`` option is true. The prometheus will be served with the port specified by ``prometheusPort`` option.

```yaml
basic:
  prometheus: true
  prometheusPort: 20499
```
For such a default configuration, eKuiper will export metrics and serve prometheus at `http://localhost:20499/metrics`

## Pluginhosts Configuration

The URL where hosts all of pre-build plugins. By default it's at `packages.emqx.io`. There could be several hosts (host can be separated with comma), if same package could be found in the several hosts, then the package in the 1st host will have the highest priority.

Please notice that only the plugins that can be installed to the current eKuiper instance will be listed through below Rest-APIs.  

```
GET http://localhost:9081/plugins/sources/prebuild
GET http://localhost:9081/plugins/sinks/prebuild
GET http://localhost:9081/plugins/functions/prebuild
```
It has following conditions to make the plugins listed through previous APIs,

- eKuiper version: The plugins must be built for the eKuiper instance version. If the plugins cannot be found  for a specific version, no plugins will be returned.
- Operating system: Now only Linux system is supported, so if eKuiper is running at other operating systems,  no plugins will be returned.
- CPU architecture: Only with correct CPU architecture built plugins are found in the plugin repository can the plugins be returned.
- EMQ official released Docker images: Only when the eKuiper is running at EMQ official released Docker images can the plugins be returned.


```yaml
pluginHosts: https://packages.emqx.io
```

It could be also as following, you can specify a local repository, and the plugin in that repository will have higher priorities.

```yaml
pluginHosts: https://local.repo.net, https://packages.emqx.io
```

The directory structure of the plugins should be similar as following.

```
http://host:port/kuiper-plugins/0.9.1/alpine/sinks
http://host:port/kuiper-plugins/0.9.1/alpine/sources
http://host:port/kuiper-plugins/0.9.1/alpine/functions
```

The content of the page should be similar as below.

```html
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
<title>Directory listing for enterprise: /4.1.1/</title>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<meta name="robots" content="noindex,nofollow">
<body>
	<h2>Directory listing for enterprise: /4.1.1/</h2>
	<hr>
	<ul>
		<li><a href="file_386.zip">file_386.zip</a>
		<li><a href="file_amd64.zip">file_amd64.zip</a>
		<li><a href="file_arm.zip">file_arm.zip</a>
		<li><a href="file_arm64.zip">file_arm64.zip</a>
		<li><a href="file_ppc64le.zip">file_ppc64le.zip</a>

		<li><a href="influx_386.zip">influx_386.zip</a>
		<li><a href="influx_amd64.zip">influx_amd64.zip</a>
		<li><a href="influx_arm.zip">influx_arm.zip</a>
		<li><a href="influx_arm64.zip">influx_arm64.zip</a>
		<li><a href="influx_ppc64le.zip">influx_ppc64le.zip</a>
	</ul>
	<hr>
</body>
</html>
```



## Sink configurations

```yaml
  #The cache persistence threshold size. If the message in sink cache is larger than 10, then it triggers persistence. If you find the remote system is slow to response, or sink throughput is small, then it's recommend to increase below 2 configurations.More memory is required with the increase of below 2 configurations.

  # If the message count reaches below value, then it triggers persistence.
  cacheThreshold: 10
  # The message persistence is triggered by a ticker, and cacheTriggerCount is for using configure the count to trigger the persistence procedure regardless if the message number reaches cacheThreshold or not. This is to prevent the data won't be saved as the cache never pass the threshold.
  cacheTriggerCount: 15

  # Control to disable cache or not. If it's set to true, then the cache will be disabled, otherwise, it will be enabled.
  disableCache: false
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
  * [more info](../rules/sources/edgex.md#connectionselector)
    

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
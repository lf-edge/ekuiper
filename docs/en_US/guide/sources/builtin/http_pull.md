# HTTP Pull Source Connector

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

The HTTP Pull source connector allows eKuiper to retrieve data from external HTTP servers, providing a flexible way to pull data on demand or based on a schedule. This section focuses on how to configure and use the HTTP Pull as a source connector.

The HTTP Pull source connector is designed to fetch data by making HTTP requests to external servers. It can be set to pull data based on a specified interval or triggered by certain conditions.

## Configurations

The connector in eKuiper can be configured with [environment variables](../../../configuration/configuration.md#environment-variable-syntax), [rest API](../../../api/restapi/configKey.md), or configuration file. This section focuses on configuring eKuiper connectors with the configuration file.

eKuiper's default HTTP Pull source configuration resides at `$ekuiper/etc/sources/http_pull.yaml`. This configuration file provides a set of default settings, which you can override as needed.

See below for a demo configuration with the global configuration and a customized `application_conf` section.

```yaml
#Global httppull configurations
default:
  # url of the request server address
  url: http://localhost
  # post, get, put, delete
  method: post
  # The interval between the requests, time unit is ms
  interval: 10000
  # The timeout for http request, time unit is ms
  timeout: 5000
  # If it's set to true, then will compare with last result; If response of two requests are the same, then will skip sending out the result.
  # The possible setting could be: true/false
  incremental: false
  # The body of request, such as '{"data": "data", "method": 1}'
  body: '{}'
  # Body type, none|text|json|html|xml|javascript|form
  bodyType: json
  # HTTP headers required for the request
  insecureSkipVerify: true
  headers:
    Accept: application/json
  # how to check the response status, by status code or by body
  responseType: code
  #  # Get token
#  oAuth:
#    # Access token fetch method
#    access:
#      # Url to fetch access token, always use POST method
#      url: https://127.0.0.1/api/token
#      # Body of the request
#      body: '{"username": "admin","password": "123456"}'
#      # Expire time of the token in string, time unit is second, allow template
#      expire: '3600'
#    # Refresh token fetch method
#    refresh:
#      # Url to refresh the token, always use POST method
#      url: https://127.0.0.1/api/refresh
#      # HTTP headers required for the request, allow template from the access token
#      headers:
#        identityId: '{{.data.identityId}}'
#        token: '{{.data.token}}'
#      # Request body
#      body: ''

#Override the global configurations
application_conf: #Conf_key
  incremental: true
  url: http://localhost:9090/pull
```

## Global Configurations

Use can specify the global HTTP pull settings here. The configuration items specified in `default` section will be taken as default settings for all HTTP connections.

### **HTTP Request Configurations**

- `url`: The URL where to get the result.
- `method`: HTTP method, it could be post, get, put & delete.
- `interval`: The interval between the requests, time unit is ms.
- `timeout`: The timeout for http request, time unit is ms.
- `body`: The body of request, such as `'{"data": "data", "method": 1}'`
- `bodyType`: Body type, it could be none|text|json|html|xml|javascript|format.
- `headers`: The HTTP request headers that you want to send along with the HTTP request.
- `responseType`: Define how to parse the HTTP response. There are two types defined:
  - `code`: To check the response status from the HTTP status code.
  - `body`: To check the response status from the response body. The body must be "application/json" content type and contains a "code" field.

### Security Configurations

#### Certificate Paths

- `certificationPath`:  Specifies the path to the certificate, example: `d3807d9fa5-certificate.pem`. This can be an absolute or relative path. The base path for a relative address depends on where the `kuiperd` command is executed.
  - If executed as `bin/kuiperd` from `/var/kuiper`, the base is `/var/kuiper`.
  - If executed as `./kuiperd` from `/var/kuiper/bin`, the base is `/var/kuiper/bin`.

- `privateKeyPath`: Path to the private key, example `d3807d9fa5-private.pem.key`. Can be an absolute or a relative path. For relative paths, refer to the behavior described under `certificationPath`.
- `rootCaPath`: Path to the root CA. Can be an absolute or a relative path.
- `insecureSkipVerify`: Control if to skip the certification verification. If set to `true`, then skip certification verification; Otherwise, verify the certification.

#### OAuth Authentication

OAuth 2.0 allows an API client limited access to user data on a web server. The most common OAuth flow is the authorization code, prevalent in server-side and mobile web apps. In this flow, users authenticate with a web app using their account, receiving an authentication code. This code allows the app to request an access token, which may be refreshed after expiration.

The following configurations are designed under the assumption that the authentication code is already known. It allows the user to define the token retrieval process.

`OAuth`: Defines the authentication flow that follows OAuth standards. For other authentication methods like API keys, the key can be set directly in the header, eliminating the need for this configuration.

- `access`

  - `url`: The url to fetch access token, will always use POST method.

  - `body`: The request body to fetch access token. Usually, the authorization code is needed here.

  - `expire`: Expire time of the token, time unit is second, allow to use template, so it must be a string.

- `refresh`

  - `url`: The url to refresh the token, always use POST method.

  - `headers`: The request header to refresh the token. Usually put the tokens here for authorization.

  - `body`: The request body to refresh the token. May not need when using header to pass the refresh token.

### Data Processing Configurations

#### Incremental Data Processing

`incremental`: If it's set to `true`, then will compare with the last result; If the responses of two requests are the same, then will skip sending out the result.

#### Dynamic Properties

Dynamic properties adapt in real time and can be employed to customize the HTTP request's URL, body, and header. The format for these properties is based on the [data template](../../sinks/data_template.md) syntax.

Key dynamic properties include:

- `PullTime`: The timestamp of the current pull time in int64 format.
- `LastPullTime`: The timestamp of the last pull time in int64 format.

For HTTP services that allow time-based filtering, `PullTime` and `LastPullTime` can be harnessed for incremental data pulls. Depending on how the service accepts time parameters:

::: v-pre

- From URL parameters: `http://localhost:9090/pull?start={{.LastPullTime}}&end={{.PullTime}}`.
- From body parameters: `{"start": {{.LastPullTime}}, "end": {{.PullTime}}`.

:::

## Custom Configurations

For scenarios where you need to customize certain connection parameters, eKuiper allows the creation of custom configuration profiles. By doing this, you can have multiple sets of configurations, each tailored for a specific use case.

Here's how to set up a custom configuration:

```yaml
#Override the global configurations
application_conf: #Conf_key
  incremental: true
  url: http://localhost:9090/pull
```

In the above example, a custom configuration named `application_conf` is created. To utilize this configuration when creating a stream, use the `CONF_KEY` option and specify the configuration name. More details can be found at [Stream Statements](../../../sqls/streams.md)).

**Usage Example**

```json
demo (
    ...
  ) WITH (DATASOURCE="test/", FORMAT="JSON", TYPE="httppull", KEY="USERID", CONF_KEY="application_conf");
```

Parameters defined in a custom configuration will override the corresponding parameters in the `default` configuration. Make sure to set values carefully to ensure the desired behavior.

## Create a Stream Source

Once the connector is defined, the next step is integrating it into eKuiper rules for data processing.

::: tip

HTTP Pull Source connector can function as a [stream source](../../streams/overview.md) or a [scan table](../../tables/scan.md) source. This section illustrates the integration using the HTTP Pull Source connector as a stream source example.

:::

You can define the HTTP Pull source as the data source either by REST API or CLI tool.

### Use REST API

The REST API offers a programmatic way to interact with eKuiper, making it suitable for those who aim to automate tasks or integrate eKuiper operations into other systems.

Example

```sql
{"sql":"create stream http_stream () WITH (FORMAT="json", TYPE="http_pull"}
```

For a comprehensive guide, refer to [Streams Management with REST API](../../../api/restapi/streams.md).

### Use CLI

If you favor a more hands-on approach, the Command Line Interface (CLI) offers direct access to eKuiper's functionalities.

1. Navigate to the eKuiper binary directory:

   ```bash
   cd path_to_eKuiper_directory/bin
   ```

2. Use the `create` command to create a rule, specifying the HTTP Pull connector as its source, for example:

   ```bash
   bin/kuiper create stream http_stream '() WITH (FORMAT="json", TYPE="http_pull")'
   ```

For a step-by-step guide, check [Streams Management with CLI](../../../api/cli/streams.md).

## Lookup Table

httppull also supports being a lookup table. We can use the create table statement to create an httppull lookup table. It will be tied to the entity relational database and queried on demand:

```text
CREATE TABLE httppullTable() WITH (DATASOURCE="/url", CONF_KEY="default", TYPE="httppull", KIND="lookup")
```

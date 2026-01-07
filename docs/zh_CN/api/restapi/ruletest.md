# 规则试运行

规则编写时，用户需要验证规则是否正确，并结合数据验证规则是否能够正常运行得到预期的结果。本节的一系列 API
用于支持规则的试运行，从而无需繁琐地创建规则输入数据即可对规则做初步的验证。

测试规则为临时的规则，不会被保存到服务器中，仅用于规则的试运行。测试规则仅能使用本节的 API 进行管理。规则运行时间固定（当前为
10 分钟），超过时间后会自动停止并清除。使用规则试运行的一般步骤如下：

1. [创建测试规则](#创建测试规则)，获得测试规则的 id 和端口。
2. 使用测试规则的 id 和端口，连接并监听 WebSocket 服务。其服务地址为 `http://locahost:10081/test/myid` 其中 `10081` 为步骤1
   返回的端口值，myid 为测试规则的 id。
3. [启动测试规则](#启动测试规则)，等待测试规则运行。规则运行结果将通过 WebSocket 服务返回。
4. 规则试运行结束后，[删除测试规则](#删除测试规则)，关闭 WebSocket 服务。

::: tip

WebSocket 服务默认采用 10081 端口，可通过配置文件 `kuiper.yaml` 中的 `httpServerPort` 字段修改。使用测试规则前，请确保该端口可访问。

:::

## 创建测试规则

```shell
POST /ruletest
```

创建试运行规则，等待运行。该 API 可检查语法，确保创建出可执行的试运行规则。请求体必需，请求体格式为 `application/json`,示例如下：

```json
{
  "id": "uuid",
  "sql": "select * from demo",
  "mockSource": {
    "demo": {
      "data": [
        {
          "a": 2
        },
        {
          "b": 3
        }
      ],
      "interval": 100,
      "loop": true
    },
    "demo1": {
      "data": [
        {
          "n": 2
        },
        {
          "n": 3
        }
      ],
      "interval": 200,
      "loop": true
    }
  },
  "sinkProps": {
    "dataTemplate": "xxx",
    "fields": ["abc", "test"]
  }
}
```

请求体参数包含 4 个部分：

- id: 测试规则的 id，必需，用于后续测试规则管理。确保唯一性，不可与其余测试规则重复，否则原测试规则会被覆盖。该 id 与普通规则的
  id 没有关联。
- sql: 测试规则的 sql 语句，必需，用于定义测试规则的语法。
- mockSource: 测试规则的数据源的模拟规则定义，可选，用于定义测试规则的输入数据。若不定义，则使用 SQL 中的真实数据源。
- sinkProps: 测试规则的 sink 参数的定义，可选。大部分 sink 的通用参数可以使用，例如 `dataTemplate` 和 `fields`。若不定义，则使用默认的
  sink 参数。

若创建成功，返回示例如下：

```json
{
  "id": "uuid",
  "port": 10081
}
```

规则创建成功后，websocket endpoint 启动。用户可通过监听 websocket 地址 `http://locahost:10081/test/uuid` 获取结果输出。其中，端口和
id 为上述返回值。

若创建失败，状态码为 400，返回错误信息，示例如下：

```json
{
  "msg": "error message here"
}
```

## 启动测试规则

```shell
POST /ruletest/{id}/start
```

启动试运行规则，WebSocket 将可接收到规则运行后输出的数据。

## 删除测试规则

```shell
DELETE /ruletest/{id}
```

删除试运行规则，WebSocket 将停止服务。

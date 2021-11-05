## 配置

- [eKuiper 基本配置](configuration_file.md)
- [MQTT  源配置](../rules/sources/mqtt.md)

## Restful APIs

eKuiper 提供了一些 RESTful 管理 API。eKuiper 提供了一些 RESTful 管理 APIs。请参考 [Rest-API 文档](../restapi/overview.md)以获取更详细信息。

## Authentication

如果使能的话， eKuiper 从 1.4.0 起将为 RESTful API 提供基于 ``JWT RSA256`` 的身份验证。用户需要将他们的公钥放在 ``etc/mgmt`` 文件夹中，并使用相应的私钥来签署 JWT 令牌。
当用户请求 RESTful API 时，将 ``Token`` 放在 http 请求头中，格式如下：
```
Authorization：XXXXXXXXXXXXXXX
```
如果token正确，eKuiper会响应结果；否则，它将返回 http ``401`` 代码。


### JWT Header

```json
{
  "typ": "JWT",
  "alg": "RS256"
}
```


### JWT Payload
JWT Payload 应使用以下格式

|  字段   | 是否可选 |  意义  |
|  ----  | ----  | ----  |
| iss  | 否| 颁发者 ,  此字段必须与``etc/mgmt``目录中的相应公钥文件名字一致|
| aud  | 否 |颁发对象 , 此字段必须是 ``eKuiper`` |
| exp  | 是 |过期时间 |
| jti  | 是 |JWT ID |
| iat  | 是 |颁发时间 |
| nbf  | 是 |Not Before |
| sub  | 是 |主题 |

这里有一个 json 格式的例子
```json
{
  "iss": "sample_key.pub",
  "adu": "eKuiper"
}
```
使用此格式时，用户必须确保正确的公钥文件 ``sample_key.pub`` 位于 ``etc/mgmt`` 下。

### JWT Signature

需要使用私钥对令牌进行签名，并将相应的公钥放在 ``etc/mgmt`` 中。

# 规则集管理

eKuiper REST api 允许您导入导出当前的所有流和规则配置。

## 规则集格式

导入导出规则集的文件格式为 JSON，其中可包含三个部分：流 `streams`，表 `tables` 和规则 `rules`。每种类型保存名字和创建语句的键值对。在以下示例文件中，我们定义了一个流和两条规则。

```json
{
    "streams": {
        "demo": "CREATE STREAM demo () WITH (DATASOURCE=\"users\", FORMAT=\"JSON\")"
    },
    "tables": {},
    "rules": {
        "rule1": "{\"id\": \"rule1\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{\"log\": {}}]}",
        "rule2": "{\"id\": \"rule2\",\"sql\": \"SELECT * FROM demo\",\"actions\": [{  \"log\": {}}]}"
    }
}
```

## 导入规则集

该 API 接受规则集并将其导入系统中。若规则集中的流或规则已存在，则不再创建。导入的规则将立刻启动。API 返回文本告知创建的流和规则的数目。 API 支持通过文本内容或者文件 URI 的方式指定规则集。

示例1：通过文本内容导入

```shell
POST http://{{host}}/ruleset/import
Content-Type: application/json

{
  "content": "$规则集 json 内容"
}
```

示例2：通过文件 URI 导入

```shell
POST http://{{host}}/ruleset/import
Content-Type: application/json

{
  "file": "file:///tmp/a.json"
}
```

## 导出规则集

导出 API 返回二进制流，在浏览器使用时，可选择下载保存的文件路径。

```shell
POST http://{{host}}/ruleset/export
```

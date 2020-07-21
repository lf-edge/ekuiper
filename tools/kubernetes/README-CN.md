## 1 程序说明及其配置：

### 1.1 程序说明：

​    本程序用于监控并处理命令文件夹中的文件。当程序发现命令文件夹下存在新创建的文件或已更新的文件时，程序将加载这些文件并执行文件中的命令，之后程序将处理过的文件名记录在命令文件夹同级目录的 .history 文件中。 .history 的数据格式如下：

```json
[
  {
        "name":"sample.json",	//已处理过文件的文件名
        "loadTime":1594362344 //处理文件时的时间戳
    }]
```

### 1.2 命令文件格式及含义：

| 字段        | 是否必填   | 类型     | 释义         |
| ----------- | ---------- | -------- | ------------ |
| commands    | 必填       | array    | 命令集合     |
| url         | 必填       | string   | http请求路径 |
| method      | 必填       | string   | http请求方法 |
| description | 选填       | string   | 操作描述     |
| data        | 创建时必填 | json obj | 创建内容     |
|             |            |          |              |

### 1.3 配置文件格式及含义：
```yaml
port: 9081  //kuiper 端口
timeout: 500  //执行一条命令超时时间（单位：毫秒）
intervalTime: 60  //隔多久检查一次命令文件夹（单位：秒）
ip: "127.0.0.1" //kuiper ip地址
logPath: "./log/kubernetes.log" //日志保存路径
commandDir: "./sample/" //命令文件夹路径
```
### 1.4 编译程序：

执行 `go build -o tools/kubernetes/kubernetes tools/kubernetes/main.go` 命令即可生成 kubernetes 程序。

## 2 流的操作示例

### 2.1 创建流 stream1

```json
{
    "commands":[
        {
            "url":"/streams",
            "description":"create stream1",
            "method":"post",
            "data":{
                "sql":"create stream stream1 (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\");"
            }
        }]
}
```

### 2.2 显示流列表
```json
{
    "commands":[
        {
            "url":"/streams",
            "description":"list stream",
            "method":"get"
        }]
}
```

### 2.3 获取流 stream1
```json
{
    "commands":[
        {
            "url":"/streams/stream1",
            "description":"get stream1",
            "method":"get"
        }]
}
```

### 2.4 删除流 stream1

```json
{
    "commands":[
        {
            "url":"/streams/stream1",
            "description":"del stream1",
            "method":"delete"
        }]
}
```

## 3 规则的操作示例

### 3.1 创建规则 rule1

```json
{
    "commands":[
        {
            "url":"/rules",
            "description":"create rule1",
            "method":"post",
            "data":{
                "id":"rule1",
                "sql":"SELECT * FROM stream1",
                "actions":[
                    {
                        "log":{
                        }
                    }]
            }
        }]
}
```

### 3.2 显示规则列表

```json
{
    "commands":[
        {
            "url":"/rules",
            "description":"list rule",
            "method":"get"
        }]
}
```

### 3.3 获取规则 rule1

```json
{
    "commands":[
        {
            "url":"/rules/rule1",
            "description":"get rule1",
            "method":"get"
        }]
}
```

### 3.4 删除规则 rule1

```json
{
    "commands":[
        {
            "url":"/rules/rule1",
            "description":"del rule1",
            "method":"delete"
        }]
}

```
### 3.5 停止规则 rule1

```json
{
    "commands":[
        {
            "url":"/rules/rule1/stop",
            "description":"stop rule1",
            "method":"post"
        }]
}
```

### 3.6 启动规则 rule1

```json
{
    "commands":[
        {
            "url":"/rules/rule1/start",
            "description":"start rule1",
            "method":"post"
        }]
}
```

### 3.7 重启规则 rule1

```json
{
    "commands":[
        {
            "url":"/rules/rule1/restart",
            "description":"restart rule1",
            "method":"post"
        }]
}
```

### 3.8 显示规则 rule1 的状态

```json
{
    "commands":[
        {
            "url":"/rules/rule1/status",
            "description":"get rule1 status",
            "method":"get"
        }]
}
```

## 4 多命令集合示例：

```json
{
    "commands":[
        {
            "url":"/streams",
            "description":"create stream1",
            "method":"post",
            "data":{
                "sql":"create stream stream1 (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\");"
            }
        },
        {
            "url":"/streams",
            "description":"create stream2",
            "method":"post",
            "data":{
                "sql":"create stream stream2 (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\");"
            }
        },
        {
            "url":"/streams",
            "description":"list stream",
            "method":"get"
        },
        {
            "url":"/streams/stream1",
            "description":"get stream1",
            "method":"get"
        },
        {
            "url":"/streams/stream2",
            "description":"del stream2",
            "method":"delete"
        },
        {
            "url":"/rules",
            "description":"create rule1",
            "method":"post",
            "data":{
                "id":"rule1",
                "sql":"SELECT * FROM stream1",
                "actions":[
                    {
                        "log":{
                        }
                    }]
            }
        },
        {
            "url":"/rules",
            "description":"create rule2",
            "method":"post",
            "data":{
                "id":"rule2",
                "sql":"SELECT * FROM stream1",
                "actions":[
                    {
                        "log":{
                        }
                    }]
            }
        },
        {
            "url":"/rules",
            "description":"list rule",
            "method":"get"
        },
        {
            "url":"/rules/rule1",
            "description":"get rule1",
            "method":"get"
        },
        {
            "url":"/rules/rule2",
            "description":"del rule2",
            "method":"delete"
        },
        {
            "url":"/rules/rule1/stop",
            "description":"stop rule1",
            "method":"post"
        },
        {
            "url":"/rules/rule1/start",
            "description":"start rule1",
            "method":"post"
        },
        {
            "url":"/rules/rule1/restart",
            "description":"restart rule1",
            "method":"post"
        },
        {
            "url":"/rules/rule1/status",
            "description":"get rule1 status",
            "method":"get"
        }]
}
```

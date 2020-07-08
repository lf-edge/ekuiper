## 1.程序说明及其配置：

### 1.1程序说明：

​    本程序用于处理命令文件夹中的文件，处理完毕后将文件移除。之后程序监控命令文件夹，当命令文件夹下出现文件时重复上述步骤，直到命令文件夹下没有文件为止。程序启动时，用户需要在配置文件中指定命令文件夹的路径，之后用户只需要将编辑好的命令文件放在命令文件夹中即可。

### 1.2命令文件格式及含义：

| 字段        | 是否必填   | 类型     | 释义         |
| ----------- | ---------- | -------- | ------------ |
| commands    | 必填       | array    | 命令集合     |
| url         | 必填       | string   | http请求路径 |
| method      | 必填       | string   | http请求方法 |
| description | 选填       | string   | 操作描述     |
| data        | 创建时必填 | json obj | 创建内容     |
|             |            |          |              |

### 1.3配置文件格式及含义：

{
    "ip":"127.0.0.1",	//kuiper ip地址
    "port":9081,	//kuiper 端口
    "logPath":"./log/kubeedge.log",	//日志保存路径
    "commandDir":"./sample/",	//命令文件夹路径
    "timeout":500,	//执行一条命令超时时间（单位：毫秒）
    "intervalTime":60	//隔多久检查一次命令文件夹（单位：秒）
}

### 1.4编译程序：

i.在main.go文件所在文件夹下运行命令：go mod init，生产go.mod文件。

ii.执行go build命令，生成可执行程序。

## 2.流的操作示例

### 2.1.创建流stream1

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

### 2.2.显示流列表

{
    "commands":[
        {
            "url":"/streams",
            "description":"list stream",
            "method":"get"
        }]
}

### 2.3.获取流stream1

{
    "commands":[
        {
            "url":"/streams/stream1",
            "description":"get stream1",
            "method":"get"
        }]
}

### 2.4.删除流stream1

{
    "commands":[
        {
            "url":"/streams/stream1",
            "description":"del stream1",
            "method":"delete"
        }]
}

## 3.规则的操作示例

### 3.1.创建规则rule1

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

### 3.2.显示规则列表

{
    "commands":[
        {
            "url":"/rules",
            "description":"list rule",
            "method":"get"
        }]
}

### 3.3.获取规则rule1

{
    "commands":[
        {
            "url":"/rules/rule1",
            "description":"get rule1",
            "method":"get"
        }]
}

### 3.4.删除规则rule1

{
    "commands":[
        {
            "url":"/rules/rule1",
            "description":"del rule1",
            "method":"delete"
        }]
}

### 3.5.停止规则rule1

{
    "commands":[
        {
            "url":"/rules/rule1/stop",
            "description":"stop rule1",
            "method":"post"
        }]
}

### 3.6.启动规则rule1

{
    "commands":[
        {
            "url":"/rules/rule1/start",
            "description":"start rule1",
            "method":"post"
        }]
}

### 3.7.重启规则rule1

{
    "commands":[
        {
            "url":"/rules/rule1/restart",
            "description":"restart rule1",
            "method":"post"
        }]
}

### 3.8.显示规则rule1的状态

{
    "commands":[
        {
            "url":"/rules/rule1/status",
            "description":"get rule1 status",
            "method":"get"
        }]
}

## 4.多命令集合示例：

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

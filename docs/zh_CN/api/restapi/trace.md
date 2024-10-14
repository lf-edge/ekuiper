# 数据追踪管理

eKuiper 支持通过 API 查看规则最近的追踪数据。

## 开启特定规则的数据追踪

开启规则的数据追踪，strategy 支持 `always` 与 `head`. `always` 代表总是对每条消息进行追踪，`head` 代表只有上游消息含有 trace context 才会进行追踪。

```shell
POST http://localhost:9081/rules/{ruleID}/trace/start

{
    "strategy": "head"
}
```

## 关闭特定规则的数据追踪

```shell
POST http://localhost:9081/rules/{ruleID}/trace/stop
```

## 根据规则 ID 查看最近的 Trace ID

```shell
GET http://localhost:9081/trace/rule/{ruleID}"

["747743cbf1fc6d10f732d17e5626021a"]
```

## 根据 Trace ID 查看详细追踪数据

```shell
GET http://localhost:9081/trace/{id}

{
    "Name": "demo",
    "TraceID": "747743cbf1fc6d10f732d17e5626021a",
    "SpanID": "f560f34e0d12a0aa",
    "ParentSpanID": "0000000000000000",
    "Attribute": null,
    "Links": null,
    "StartTime": "2024-08-28T10:01:38.362706+08:00",
    "EndTime": "2024-08-28T10:01:38.362745751+08:00",
    "ChildSpan": [
        {
            "Name": "2_decoder",
            "TraceID": "747743cbf1fc6d10f732d17e5626021a",
            "SpanID": "fe1cde747e6cc4ba",
            "ParentSpanID": "f560f34e0d12a0aa",
            "Attribute": {
                "data": "{\"a\":1}"
            },
            "Links": null,
            "StartTime": "2024-08-28T10:01:38.362842+08:00",
            "EndTime": "2024-08-28T10:01:38.362865821+08:00",
            "ChildSpan": [
                {
                    "Name": "3_project",
                    "TraceID": "747743cbf1fc6d10f732d17e5626021a",
                    "SpanID": "377ee05e98e7f00b",
                    "ParentSpanID": "fe1cde747e6cc4ba",
                    "Attribute": {
                        "data": "{\"a\":1,\"meta\":\"747743cbf1fc6d10f732d17e5626021a\"}"
                    },
                    "Links": null,
                    "StartTime": "2024-08-28T10:01:38.362926+08:00",
                    "EndTime": "2024-08-28T10:01:38.362977943+08:00",
                    "ChildSpan": [
                        {
                            "Name": "transform_op",
                            "TraceID": "747743cbf1fc6d10f732d17e5626021a",
                            "SpanID": "7816e87f397b8ecc",
                            "ParentSpanID": "377ee05e98e7f00b",
                            "Attribute": {
                                "data": "{\"a\":1,\"meta\":\"747743cbf1fc6d10f732d17e5626021a\"}"
                            },
                            "Links": null,
                            "StartTime": "2024-08-28T10:01:38.363005+08:00",
                            "EndTime": "2024-08-28T10:01:38.363016309+08:00",
                            "ChildSpan": [
                                {
                                    "Name": "transform_op_split",
                                    "TraceID": "747743cbf1fc6d10f732d17e5626021a",
                                    "SpanID": "a0a6786f7905f9ba",
                                    "ParentSpanID": "7816e87f397b8ecc",
                                    "Attribute": null,
                                    "Links": null,
                                    "StartTime": "2024-08-28T10:01:38.363021+08:00",
                                    "EndTime": "2024-08-28T10:01:38.363023415+08:00",
                                    "ChildSpan": [
                                        {
                                            "Name": "log_0_1_encode",
                                            "TraceID": "747743cbf1fc6d10f732d17e5626021a",
                                            "SpanID": "fecc8a2b92b72560",
                                            "ParentSpanID": "a0a6786f7905f9ba",
                                            "Attribute": {
                                                "data": "[{\"a\":1,\"meta\":\"747743cbf1fc6d10f732d17e5626021a\"}]"
                                            },
                                            "Links": null,
                                            "StartTime": "2024-08-28T10:01:38.363053+08:00",
                                            "EndTime": "2024-08-28T10:01:38.363063262+08:00",
                                            "ChildSpan": [
                                                {
                                                    "Name": "log_0",
                                                    "TraceID": "747743cbf1fc6d10f732d17e5626021a",
                                                    "SpanID": "c544ab89716781f6",
                                                    "ParentSpanID": "fecc8a2b92b72560",
                                                    "Attribute": {
                                                        "data": "[{\"a\":1,\"meta\":\"747743cbf1fc6d10f732d17e5626021a\"}]"
                                                    },
                                                    "Links": null,
                                                    "StartTime": "2024-08-28T10:01:38.363082+08:00",
                                                    "EndTime": "2024-08-28T10:01:38.363083833+08:00",
                                                    "ChildSpan": []
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]
}
```

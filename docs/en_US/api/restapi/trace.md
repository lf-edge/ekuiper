# Data tracing management

eKuiper supports viewing recent tracing data of rules through API.

## Start trace the data of specific rule

Turn on the data tracing of the rules. The strategy supports `always` and `head`. `always` means that each message is always traced, and `head` means that only upstream messages containing trace context will be traced.

```shell
POST http://localhost:9081/rules/{ruleID}/trace/start

{
    "strategy": "head"
}
```

## Stop trace the data of specific rule

```shell
POST http://localhost:9081/rules/{ruleID}/trace/stop
```

## View the latest Trace ID based on the rule ID

```shell
GET http://localhost:9081/trace/rule/{ruleID}"

["747743cbf1fc6d10f732d17e5626021a"]
```

## View detailed tracing data based on Trace ID

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

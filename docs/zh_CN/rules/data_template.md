# eKuiper 中使用 Golang 模版 (template) 定制分析结果

## 简介

用户通过 eKuiper 进行数据分析处理后，使用各种 sink 可以往不同的系统发送数据分析结果。针对同样的分析结果，不同的 sink 需要的格式可能未必一样。比如，在某物联网场景中，当发现某设备温度过高的时候，需要向云端某 rest 服务发送一个请求，同时在本地需要通过 MQTT 协议往设备发送一个控制命令，这两者需要的数据格式可能并不一样，因此，需要对来自于分析的结果进行「二次处理」后，才可以往不同的目标发送针对数据。本文将介绍如何利用 sink 中的数据模版（data  template ）来实现对分析结果的「二次处理」。

## Golang 模版介绍

Golang  模版将一段逻辑应用到数据上，然后按照用户指定的逻辑对数据进行格式化输出，Golang 模版常见的使用场景为在网页开发中，比如将 Golang 中的某数据结构进行转换和控制后，将其转换为 HTML 标签输出到浏览器。在eKuiper 使用了 [Golang 的 template（模版）](https://golang.org/pkg/text/template/)对分析结果实现「二次处理」，请参考以下来自于 Golang 的官方介绍。

> 模版是通过将其应用到一个数据结构上来执行的。模版中的注释 (Annotations) 指的是数据结构中的元素（典型的为结构体中的一个字段，或者 map 中的一个 key），注释用于控制执行、并获取用于显示的值。模版的执行会迭代数据结构并设置游标，通过符号「.」 来表示，称之为「dot」，在执行过程中指向数据结构中的当前位置。
>
> 模版的输入文本可以为 UTF-8 编码的任意文本。「`动作` (Actions)」 -- 数据求值或者控制结构  - 是通过  "{{" 和 "}}" 来界定的；所有在`动作`之外的文本会被保持原样到输出，除了 raw strings，`动作`不可跨行（注释除外）。

### 动作 (Actions)

Golang 模版提供了一些[内置的动作](https://golang.org/pkg/text/template/#hdr-Actions)，可以让用户写各种控制语句，用于提取内容。比如，

- 根据判断条件来输出不同的内容

```
{{if pipeline}} T1 {{else}} T0 {{end}}
```

- 循环遍历数据，并进行处理

```
{{range pipeline}} T1 {{else}} T0 {{end}}
```

读者可以看到，动作是用 `{{}}` 界定的，在 eKuiper 的数据模版使用过程中，由于输出一般也是 JSON 格式， 而 JSON 格式是用 `{}` 来界定，因此读者在不太熟悉使用的时候，在使用 eKuiper 的数据模版的功能会觉得比较难以理解。比如以下的例子中，

```
{{if pipeline}} {"field1": true} {{else}}  {"field1": false} {{end}}
```

上述表达式的意思如下（请注意动作的界定符和 JSON 的界定符）：

- 如果满足了条件 pipeline，则输出 JSON 字符串 `{"field1": true}`
- 否则输出 JSON 字符串 `{"field1": false}`

### eKuiper sink 数据格式

Golang 的模版可以作用于各种数据结构，比如 map、切片 (slice)，通道等，而 eKuiper 的 sink 中的数据模版得到的数据类型是固定的，是一个包含了 Golang `map` 切片的数据类型，如下所示。

```go
[]map[string]interface{}
```

### 切片 (slice) 数据按条发送

流入 sink 的数据是一个 `map[string]interface{}` 切片的数据结构，但是用户往目标 sink 发送数据的时候，可能是需要单条的数据，而不是所有的数据。比如在这篇 [eKuiper 与 AWS IoT Hub 集成的文章](https://www.emqx.cn/blog/lightweight-edge-computing-emqx-kuiper-and-aws-iot-hub-integration-solution)中所介绍的，规则产生的样例数据如下所示。

```json
[
  {"device_id":"1","t_av":36.25,"t_count":4,"t_max":80,"t_min":10},
  {"device_id":"2","t_av":27,"t_count":4,"t_max":45,"t_min":12}
]
```

::: v-pre
在发送到 sink 的时候，希望每条数据分开发送，首先需要将 sink 的 ``sendSingle`` 设置为 `true`，然后使用数据模版：`{{json .}}`，完整配置如下，用户可以将其拷贝到某 sink 配置的最后。
:::

```json
 ...
 "sendSingle": true,
 "dataTemplate": "{{toJson .}}"
```

- 将 ``sendSingle`` 设置为 `true`后，eKuiper 把传递给 sink 的 `[]map[string]interface{}` 数据类型进行遍历处理，对于遍历过程中的每一条数据都会应用用户指定的数据模版
- `toJson` 是 eKuiper 提供的函数（用户可以参考 [eKuiper 扩展模版函数](./overview.md#模版中支持的函数)来了解更多的 eKuiper 扩展），可以将传入的参数转化为 JSON 字符串输出，对于遍历到的每一条数据，将 map 中的内容转换为 JSON 字符串

Golang 还内置提供了一些函数，用户可以参考[更多 Golang 内置提供的函数](https://golang.org/pkg/text/template/#hdr-Functions)来获取更多函数信息。

### 数据内容转换

还是针对上述例子，需要对返回的 `t_av`（平均温度）做一些转换，转换的基本要求就是根据不同的平均温度，加入不同的描述文字，用于目标 sink 中的处理。规则如下，

- 当温度小于 30，描述字段为「Current temperature is`$t_av`,  it's normal.」
- 当温度大于 30，描述字段为「Current temperature is`$t_av`, it's high.」 

假设目标 sink 还是需要 JSON 数据，该数据模版的内容如下，

```json
...
"dataTemplate": "{\"device_id\": {{.device_id}}, \"description\": \"{{if lt .t_av 30.0}}Current temperature is {{.t_av}}, it's normal.\"{{else if ge .t_av 30.0}}Current temperature is {{.t_av}}, it's high.\"{{end}}}"
"sendSingle": true,
```

::: v-pre
在上述的数据模版中，使用了 `{{if pipeline}} T1 {{else if pipeline}} T0 {{end}}` 的内置动作，看上去比较复杂，稍微调整一下，去掉转义并加入缩进后排版如下（注意：在生成 eKuiper 规则的时候，不能传入以下优化后排版的规则）。
:::

```
{"device_id": {{.device_id}}, "description": "
  {{if lt .t_av 30.0}}
    Current temperature is {{.t_av}}, it's normal."
  {{else if ge .t_av 30.0}}
    Current temperature is {{.t_av}}, it's high."
  {{end}}
}
```

使用了 Golang 内置的二元比较函数，

- `lt`： 小于
- `ge`：大于等于

值得注意的是，在 `lt`  和 `ge` 函数中，第二个参数值的类型应该与 map 中的数据实际的数据类型一致，否则会出错。如在上述的例子中，温度大于 `30` 的情况，因为 map 中实际平均数的类型为 float，因此第二个参数的值需传入 `30.0`，而不是 `30`。

另外，模版还是应用到切片中每条记录上，所以还是需要将 `sendSingle` 属性设置为 `true`。最终该数据模版针对上述数据产生的内容如下，

```json
{"device_id": 1, "description": "Current temperature is 36.25, it's high."}
{"device_id": 2, "description": "Current temperature is 27, it's normal."}
```

### 数据遍历

通过给 sink 的 `sendSingle` 属性设置为 `true` ，可以实现把传递给 sink 的切片数据进行遍历。在此处，我们将介绍一些更为复杂的例子，比如在 sink 的结果中，包含了嵌套的数组类型的数据，如何通过在数据模版中提供的遍历功能，自己来实现遍历。

假设流入 sink 中的数据内容如下所示，

```json
{"device_id":"1", 
 "values": [
  {"temperature": 10.5},
  {"temperature": 20.3},
  {"temperature": 30.3}
 ]
}
```

需求为，

- 当发现 "values" 数组中某个 `temperature` 值小于等于 `25` 的时候，增加一个名为 `description` 的属性，将其值设置为 `fine`。
- 当发现 "values" 数组中某个 `temperature` 值大于 `25` 的时候，增加一个名为 `description` 的属性，将其值设置为 `high`。

```json
"sendSingle": true,
"dataTemplate": "{{$len := len .values}} {{$loopsize := add $len -1}} {\"device_id\": \"{{.device_id}}\", \"description\": [{{range $index, $ele := .values}} {{if le .temperature 25.0}}\"fine\"{{else if gt .temperature 25.0}}\"high\"{{end}} {{if eq $loopsize $index}}]{{else}},{{end}}{{end}}}"
```

该数据模板比较复杂，解释如下，

::: v-pre
- `{{$len := len .values}} {{$loopsize := add $len -1}}`，这一段执行了两个表达式，第一个 `len` 函数取得数据中 `values` 的长度，第二个 `add` 将其值减 1 并赋值到变量 `loopsize`：由于 Golang 的表达式中目前还不支持直接将数值减 1 的操作， `add` 是 eKuiper 为实现该功能而扩展的函数。
:::

::: v-pre
- `{\"device_id\": \"{{.device_id}}\", \"description\": [` 这一段模版在作用到样例数据后，生成了 JSON 串 `{"device_id": "1", "description": [ `
:::

::: v-pre
- `{{range $index, $ele := .values}} {{if le .temperature 25.0}}\"fine\"{{else if gt .temperature 25.0}}\"high\"{{end}} {{if eq $loopsize $index}}]{{else}},{{end}}{{end}}` ，这一段模版看起来比较复杂，但是如果把它调整一下，去掉转义并加入缩进后排版如下，看起来可能会更加清晰（注意：在生成 eKuiper 规则的时候，不能传入以下优化后排版的规则）。
:::

```
{{range $index, $ele := .values}} 
  {{if le .temperature 25.0}}
    "fine"
  {{else if gt .temperature 25.0}}
    "high"
  {{end}} 
  {{if eq $loopsize $index}}
    ]
  {{else}}
    ,
  {{end}}
{{end}}
```

  第一个条件判断生成是 `fine`  或者 `high`；第二个条件判断是生成分隔数组的 `,` 还是数组结尾的 `]`。

另外，模版还是应用到切片中每条记录上，所以还是需要将 `sendSingle` 属性设置为 `true`。最终该数据模版针对上述数据产生的内容如下，

```json
  {"device_id": "1", "description": [ "fine" , "fine" , "high" ]}
```

## 总结

通过 eKuiper 提供的数据模版功能可以实现对分析结果的二次处理，以满足不同的 sink 目标的需求。但是读者也可以看到，由于 Golang 模版本身的限制，实现比较复杂的数据转换的时候会比较笨拙，希望将来 Golang 模版的功能可以做得更加强大和灵活，这样可以支持处理更加复杂的需求。目前建议用户可以通过数据模版来实现一些较为简单的数据的转换；如果用户需要对数据进行比较复杂的处理，并且自己扩展了 sink 的情况下，可以在 sink 的实现中直接进行处理。

另外，eKuiper 团队在规划将来支持自定义扩展 sink 中的模版函数，这样一些比较复杂的逻辑可以在函数内部实现，用户调用的时候只需一个简单的模版函数调用即可实现。


Kuiper 实现了下面的插件，目前这些插件有的是用于描述插件开发过程的样例，有的是来自于社区开发者贡献的插件，在使用插件前，请仔细阅读相关文档。

Kuiper 插件开发者在开发过程中，可以指定元数据文件，这些元数据主要应用于以下方面：

- 插件编译：对于在目录 `plugins/sinks` 和 `plugins/sources` 中的插件，如果开发者提供了相关的元数据文件，那么 Kuiper 在版本发布的时候会自动编译该插件，然后自动上传这些插件到 EMQ 的插件下载网站上： www.emqx.io/downloads/kuiper/vx.x.x/plugins，其中 `x.x.x` 为版本号。

  **<u>请注意：由于 Golang 插件的局限性，这些自动编译出来的插件能运行在 Kuiper 官方发布的对应版本的容器中；但是对于直接下载的二进制安装包，或者用户自己编译出来的二进制包，这些下载的插件不保证可以正常运行。</u>**

- 可视化展示：从 0.9.1 版本开始，Kuiper 会随版本同步发布管理控制台，该控制台可以用于管理 Kuiper 节点、流、规则和插件等。开发者提供的插件元数据可以让用户在使用插件的时候更加方便，因此强烈建议插件开发者在提交插件的时候同时提供对应的元数据文件。元数据文件为json格式，文件名与插件一致，与插件一起放在压缩包的根目录下。

## 源 (Sources)

| 名称                  | 描述                                                  | 备注                                                |
| --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| [zmq](sources/zmq.md)| 该插件监听 Zero Mq 消息并发送到 Kuiper 流中 | 插件样例，不可用于生产环境 |
| [random](sources/random.md) | 该插件按照指定模式生成消息   | 插件样例，不可用于生产环境 |

### source 元数据文件格式

source 的大部分属性用户通过对应的配置文件指定，用户无法在创建流的过程中对其进行配置。插件开发者提供的元数据文件中，只需指定以下部分的内容。

#### libs

该部分内容定义了插件用到了哪些库依赖 (格式为 `github.com/x/y@version`)，在插件的编译过程中，会读取该信息，将相关的库依赖加入到项目的 `go.mod` 中，该配置项内容为字符串数组。

#### about

* trial:表示插件是否可用

* author

  这部分包含了插件的作者信息，插件开发者可以视情况提供这些信息，这部分信息会被展示在管理控制台的插件信息列表上。

      * name：名字
      *  email：电子邮件地址
      * company：公司名称
      * website：公司网站地址

* helpUrl

  该插件的帮助文件地址，控制台会根据语言的支持情况，链接到对应的帮助文档中。

      * en_US：英文文档帮助地址
      * zh_CN：中文文档帮助地址

* description

  该插件的简单描述，控制台支持多种语言。

  * en_US：英文描述
  * zh_CN：中文描述

#### properties

该插件所支持的属性列表，以及每个属性相关的配置。

- name：属性名称；**该字段必须提供；**
- default：缺省值，用于设定该属性的缺省值，类型可以为数字、字符、布尔等；该字段可选(可嵌套)；
- optional：设定该属性是否是必须提供；该字段可选，如果不提供则为 `true`, 表示用户可以提供该字段的值；
- control：控件类型，控制在界面中显示的控件类型；**该字段必须提供；**
  - text：文本输入框
  - text-area：文字编辑区域
  - list-box：列表框
  - radio：单选框
- helpUrl：如果有关于该属性更详细的帮助，可以在此指定；该字段可选；
  - en_US：英文文档帮助地址
  - zh_CN：中文文档帮助地址
- hint：控件的提示信息；该字段可选；
  - en_US
  - zh_CN
- label：控件针对的标签控件；**该字段必须提供；**
  - en_US
  - zh_CN
- values：如果控件类型为 `list-box` 或者 `radio`，**该字段必须提供；**
  - 数组：数据类型可以为数字、字符、布尔等

#### 样例文件

以下为样例元数据文件。

```json
{
	"libs": [""],
	"about": {
		"trial": false,
		"author": {
			"name": "",
			"email": "",
			"company": "",
			"website": ""
		},
		"helpUrl": {
			"en_US": "",
			"zh_CN": ""
		},
		"description": {
			"en_US": "",
			"zh_CN": ""
		}
	},
	"properties": {
		"default": [{
			"name": "",
			"default": "",
			"optional": false,
			"control": "",
			"type": "",
			"hint": {
				"en_US": "",
				"zh_CN": ""
			},
			"label": {
				"en_US": "",
				"zh_CN": ""
			}
		}, {
			"name": "",
			"default": [{
				"name": "",
				"default": "",
				"optional": false,
				"control": "",
				"type": "",
				"hint": {
					"en_US": "",
					"zh_CN": ""
				},
				"label": {
					"en_US": "",
					"zh_CN": ""
				}
			}],
			"optional": false,
			"control": "",
			"type": "",
			"hint": {
				"en_US": "",
				"zh_CN": ""
			},
			"label": {
				"en_US": "",
				"zh_CN": ""
			}
		}]
	}
}
```



## 动作 (Sinks/Actions)

| 名称                  | 描述                                                  | 备注                                                |
| --------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| [file](sinks/file.md) | 该插件将分析结果保存到某个指定到文件系统中 | 插件样例，不可用于生产环境 |
| [zmq](sinks/zmq.md)   | 该插件将分析结果发送到 Zero Mq 的主题中  | 插件样例，不能用于生产环境 |
| [Influxdb](sinks/influxdb.md)   | 该插件将分析结果发送到 InfluxDB 中  | 由 [@smart33690](https://github.com/smart33690) 提供 |
| [TDengine](sinks/taos.md) | 该插件将分析结果发送到 TDengine 中 |  |

### sink 元数据文件格式

元数据文件格式为 JSON，主要分成以下部分：

#### libs

该部分内容定义了插件用到了哪些库依赖 (格式为 `github.com/x/y@version`)，在插件的编译过程中，会读取该信息，将相关的库依赖加入到项目的 `go.mod` 中，该配置项内容为字符串数组。

#### about

* trial:表示插件是否可用

* author

  这部分包含了插件的作者信息，插件开发者可以视情况提供这些信息，这部分信息会被展示在管理控制台的插件信息列表上。

     * name：名字
     *  email：电子邮件地址
     * company：公司名称
     * website：公司网站地址

* helpUrl

  该插件的帮助文件地址，控制台会根据语言的支持情况，链接到对应的帮助文档中。

     * en_US：英文文档帮助地址
     * zh_CN：中文文档帮助地址

* description

  该插件的简单描述，控制台支持多种语言。

  * en_US：英文描述
  * zh_CN：中文描述

#### properties

该插件所支持的属性列表，以及每个属性相关的配置。

- name：属性名称；**该字段必须提供；**
- default：缺省值，用于设定该属性的缺省值，类型可以为数字、字符、布尔等；该字段可选；
- optional：设定该属性是否是必须提供；该字段可选，如果不提供则为 `true`, 表示用户可以提供该字段的值；
- control：控件类型，控制在界面中显示的控件类型；**该字段必须提供；**
  - text：文本输入框
  - text-area：文字编辑区域
  - list-box：列表框
  - radio-button：单选框
- helpUrl：如果有关于该属性更详细的帮助，可以在此指定；该字段可选；
  - en_US：英文文档帮助地址
  - zh_CN：中文文档帮助地址
- hint：控件的提示信息；该字段可选；
  - en_US
  - zh_CN
- label：控件针对的标签控件；**该字段必须提供；**
  - en_US
  - zh_CN
- values：如果控件类型为 `list-box` 或者 `radio-button`，**该字段必须提供；**
  - 数组：数据类型可以为数字、字符、布尔等

#### 样例文件

以下为样例元数据文件。

```json
{
	"about": {
		"trial": false,
		"author": {
			"name": "",
			"email": "",
			"company": "",
			"website": ""
		},
		"helpUrl": {
			"en_US": "",
			"zh_CN": ""
		},
		"description": {
			"en_US": "",
			"zh_CN": ""
		}
	},
	"libs": [""],
	"properties": [{
		"name": "",
		"default": "",
		"optional": false,
		"control": "",
		"type": "",
		"hint": {
			"en_US": "",
			"zh_CN": ""
		},
		"label": {
			"en_US": "",
			"zh_CN": ""
		}
	}]
}
```

## 函数 (Functions)

Kuiper 具有许多内置函数，可以对数据执行计算。(具体文档参考 https://github.com/emqx/kuiper/blob/master/docs/zh_CN/sqls/built-in_functions.md)

### functions 元数据文件格式

元数据文件格式为 JSON，主要分成以下部分：

- name：属性名称；**该字段必须提供；**
- control：控件类型，控制在界面中显示的控件类型；**该字段必须提供；**
  - text：文本输入框
- example：样例
- hint：控件的提示信息；该字段可选；
  - en_US
  - zh_CN

#### 样例文件

以下为样例元数据文件。

```json
[{
	"name": "avg",
	"control": "text",
	"example": "avg(col1)",
	"hint": {
		"en_US": "The file path for saving the result",
		"zh_CN": "组中的平均值。空值不参与计算。"
	}
}]
```

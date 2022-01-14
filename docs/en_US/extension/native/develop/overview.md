eKuiper has implemented the following plugins. At present, some of these plug-ins are examples used to describe the plug-in development process, and some are contributed by community developers. Please read the relevant documents carefully before using the plugins.

Developers of eKuiper plugin can specify metadata files during the development process. These metadata files are mainly used in the following aspects:

- Plugin compilation: For plugins in the directories `plugins/sinks` and `plugins/sources`, if the developer provides related metadata files, eKuiper will automatically compile the plugins when the version is released, and then automatically upload these plugins to the EMQ plugin download website: www.emqx.io/downloads/kuiper/vx.x.x/plugins, where `x.x.x` is the version number.

  **<u>Note: Due to the limitations of Golang plugins, these automatically compiled plugins can run in the container of the corresponding version released by eKuiper. However, for the directly downloaded binary installation package, or the binary package compiled by the user , these downloaded plugins are not guaranteed to work properly. </u>**

- Visualization: from version 0.9.1, eKuiper will release the management console synchronously with the version, which can be used to manage eKuiper nodes, streams, rules, and plugins. Plugin metadata provided by developers can make it more convenient for users to use plugins. Therefore, it is strongly recommended that plugin developers provide corresponding metadata files when submitting plugins. The metadata file is in JSON format. The file name is consistent with that of the plugin and is placed in the root directory of the compressed package together with the plugin.

## Sources

| Name                        | Descriptiom                                                  | Remarks                                                   |
| --------------------------- | ------------------------------------------------------------ | --------------------------------------------------------- |
| [zmq](../sources/zmq.md)       | The plugin listens to Zero Mq messages and sends them to the eKuiper stream | Sample of plugin, not available in production environment |
| [random](../sources/random.md) | The plugin generates messages according to the specified pattern | Sample of plugin, not available in production environment |

### source metadata file format

Most attributes of source are specified by the user through the corresponding configuration file, and the user cannot configure it during the creation of the stream. In the metadata file provided by the plugin developer, only the following parts need to be specified.

#### libs

This part of the content defines which library dependencies are used by the plugin (the format is `github.com/x/y@version`). During the compilation of the plugin, this information is read and the relevant library dependencies are added to the `go.mod` of the project. The content of this configuration item is a string array.

#### about

* trial: indicates whether the plugin is under beta test stage 

* author

  This part contains the author information of the plugin. The plugin developer can provide this information as appropriate. The information of this part will be displayed in the plugin information list of the management console.

  * name
  * email
  * company
  * website

* helpUrl

  The help file address of the plug-in. The console will link to the corresponding help file according to the language support.

  * en_US: English document help address
  * zh_CN: Chinese document help address

* description

  A simple description of the plugin. The console supports multiple languages.

  * en_US: English description
  * zh_CN: Chinese description

#### properties

The list of attributes supported by the plugin and the configuration related to each attribute.

- name: attribute name; **This field must be provided;**
- default: default value, which is used to set the default value of the attribute. The type can be numbers, characters, boolean, and so on. This field is optional (can be nested);
- optional: set whether the attribute must be provided; the field is optional, if not provided, it is `true`, indicating that the user can provide the value of the field;
- control: control type, which control the type of control displayed in the interface; **This field must be provided;**
  - text: text input box
  - text-area: text editing area
  - list: list box
  - radio: radio box
- Helpurl: if you have more detailed help on this property, you can specify it here; this field is optional;
  - en_US: English document help address
  - zh_CN: Chinese document help address
- Hint: prompt information of the control; this field is optional;
  - en_US
  - zh_CN
- label: The label control targeted by the control; **This field must be provided;**
  - en_US
  - zh_CN
- type: field type; **This field must be provided;**

  * string
  * float
  * int
  * list_object: list, element is structure
  * list_string: list, elements is string
- values: If the control type is `list-box` or `radio`, **this field must be provided;**
- Array: The data type can be number, character, boolean, etc.

#### Sample file

The following is a sample of metadata file.

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



## Sinks/Actions

| Name                        | Description                                                  | Remarks                                                   |
| --------------------------- | ------------------------------------------------------------ | --------------------------------------------------------- |
| [file](../sinks/file.md)       | The plugin saves the analysis results to a specified file system | Sample of plugin, not available in production environment |
| [zmq](../sinks/zmq.md)         | The plugin sends the analysis results to the topic of Zero Mq | Sample of plugin, not available in production environment |
| [Influxdb](../sinks/influx.md) | The plugin sends the analysis results to InfluxDB            | Provided by [@smart33690](https://github.com/smart33690)  |
| [TDengine](../sinks/tdengine.md)   | The plugin sends the analysis results to TDengine            |                                                           |

### sink metadata file format

The metadata file format is JSON and is mainly divided into the following parts:

#### libs

The content of this part defines which library dependencies are used by the plugin (the format is `github.com/x/y@version`). During the compilation of the plugin, this information is read and the relevant library dependencies are added to the `go.mod` of the project. The content of this configuration item is a string array.

#### about

* trial: indicates whether the plugin is under beta test stage

* author

  This part contains the author information of the plugin. The plugin developer can provide this information as appropriate. The information of this part will be displayed in the plugin information list of the management console.

  * name
     * email
     * company
     * website

* helpUrl

  The help file address of the plugin. The console will link to the corresponding help file according to the language support.

     * en_US: English document help address
  * zh_CN: Chinese document help address

* description

  A simple description of the plugin. The console supports multiple languages.

  * en_US: English description
  * zh_CN: Chinese description

#### properties

The list of attributes supported by the plugin and the configuration related to each attribute.

- name: attribute name; **This field must be provided;**
- default: default value, which is used to set the default value of the attribute. The type can be numbers, characters, boolean, and so on. This field is optional (can be nested);
- optional: set whether the attribute must be provided; the field is optional, if not provided, it is `true`, indicating that the user can provide the value of the field;
- control: control type, which control the type of control displayed in the interface; **This field must be provided;**
  - text: text input box
  - text-area: text editing area
  - list: list box
  - radio: radio box
- Helpurl: if you have more detailed help on this property, you can specify it here; this field is optional;
  - en_US: English document help address
  - zh_CN: Chinese document help address
- Hint: prompt information of the control; this field is optional;
  - en_US
  - zh_CN
- label: The label control targeted by the control; **This field must be provided;**
  - en_US
  - zh_CN
- type: field type; **This field must be provided;**

  * string
  * float
  * int
  * list_object: list, element is structure
  * list_string: list, elements is string
  * list_float: list, elements is float
   * list_int: list, elements is int
- values: If the control type is `list-box` or `radio`, **this field must be provided;**
- Array: The data type can be number, character, boolean, etc.

#### Sample file

The following is a sample of metadata file.

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

## Functions

| Name                                          | Description                                                  | Remarks                                                 |
| --------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------- |
| [echo](../functions/functions.md)                | Output parameter value as it is                              | Plugin sample, not available for production environment |
| [countPlusOne](../functions/functions.md)        | Output the value of the parameter length plus one            | Plugin sample, not available for production environment |
| [accumulateWordCount](../functions/functions.md) | The function counts how many words there are                 | Plugin sample, not available for production environment |
| [resize](../functions/functions.md)              | Create a scaled image with new dimensions (width, height). If width or height is set to 0, it is set to the reserved value of aspect ratio | Plugin sample, not available for production environment |
| [thumbnail](../functions/functions.md)           | Reduce the image that retains the aspect ratio to the maximum size (maxWidth, maxHeight). | Plugin sample, not available for production environment |

eKuiper has many built-in functions that can perform calculations on data. (Refer to https://github.com/lf-edge/ekuiper/blob/master/docs/zh_CN/sqls/built-in_functions.md for specific documentation)

### functions metadata file format

The metadata file format is JSON and is mainly divided into the following parts:

#### about

* trial: indicates whether the plugin is under beta test stage

* author

  This part contains the author information of the plugin. The plugin developer can provide this information as appropriate. The information of this part will be displayed in the plugin information list of the management console.

  * name
     * email
     * company
     * website

* helpUrl

  The help file address of the plugin. The console will link to the corresponding help file according to the language support.

     * en_US: English document help address
  * zh_CN: Chinese document help address

* description

  A simple description of the plugin. The console supports multiple languages.

  * en_US: English description
  * zh_CN: Chinese description

#### functions

- name: attribute name; **This field must be provided;**
- example
- hint: hint information of the function; this field is optional;
- - en_US
  - zh_CN

#### Sample file

The following is a sample of metadata file.

```json
{
	"about": {
		"trial":false,
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
	"functions": [{
		"name": "",
		"example": "",
		"hint": {
			"en_US": "",
			"zh_CN": ""
		}
	}]
}
```



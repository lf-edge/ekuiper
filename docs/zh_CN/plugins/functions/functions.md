# 定制函数

Kuiper 可以定制函数，函数的开发、编译及使用请[参见这里](../../extension/function.md)。

### echo 插件

| 函数 | 示例      | 说明           |
| ---- | --------- | -------------- |
| echo | echo(avg) | 原样输出参数值 |

echo(avg) 示例

- 假设 avg 类型为 int ，值为30， 则结果为: `[{"r1":30}]`

  ```
  SELECT echo(avg) as r1 FROM test;
  ```

### countPlusOne 插件

| 函数         | 示例              | 说明                 |
| ------------ | ----------------- | -------------------- |
| countPlusOne | countPlusOne(avg) | 输出参数长度加一的值 |

countPlusOne(avg) 示例

- 假设 avg 类型为 []int ，值为`[1,2,3]`， 则结果为: `[{"r1":4}]`

  ```
  SELECT countPlusOne(avg) as r1 FROM test;
  ```

### accumulateWordCount 插件

| 函数                | 示例                         | 说明                     |
| ------------------- | ---------------------------- | ------------------------ |
| accumulateWordCount | accumulateWordCount(avg,sep) | 函数统计一共有多少个单词 |

accumulateWordCount(avg,sep) 示例

- 假设 avg 类型为 string ，值为`My name is Bob`；sep  类型为 string ，值为空格,则结果为: `[{"r1":4}]`

  ```
  SELECT accumulateWordCount(avg,sep) as r1 FROM test;
  ```

### 图像处理插件

图像处理目前暂时只支持`png`和`jpeg`格式

| 函数      | 示例                               | 说明                                                         |
| --------- | ---------------------------------- | ------------------------------------------------------------ |
| resize    | resize(avg,width, height)          | 创建具有新尺寸（宽度，高度）的缩放图像。如果 width 或 height 设置为0，则将其设置为长宽比保留值 |
| thumbnail | thumbnail(avg,maxWidth, maxHeight) | 将保留宽高比的图像缩小到最大尺寸( maxWidth，maxHeight)。     |

resize(avg,width, height)示例

- 其中 avg 类型为 []byte 。

  ```
  SELECT resize(avg,width,height) as r1 FROM test;
  ```

thumbnail(avg,maxWidth, maxHeight)示例

- 其中 avg 类型为 []byte。

  ```
  SELECT countPlusOne(avg,maxWidth, maxHeight) as r1 FROM test;
  ```

### Geohash 插件

| 函数                  | 示例                                                     | 说明                                                         |
| --------------------- | -------------------------------------------------------- | ------------------------------------------------------------ |
| geohashEncode         | geohashEncode(la,lo float64)(string)                     | 将经纬度编码为字符串                                         |
| geohashEncodeInt      | geohashEncodeInt(la,lo float64)(uint64)                  | 将经纬度编码为无类型整数                                     |
| geohashDecode         | geohashDecode(hash string)(la,lo float64)                | 将字符串解码为经纬度                                         |
| geohashDecodeInt      | geohashDecodeInt(hash uint64)(la,lo float64)             | 将无类型整数解码为经纬度                                     |
| geohashBoundingBox    | geohashBoundingBox(hash string)(string)                  | 返回字符串编码的区域                                         |
| geohashBoundingBoxInt | geohashBoundingBoxInt(hash uint64)(string)               | 返回无类型整数编码的区域                                     |
| geohashNeighbor       | geohashNeighbor(hash string,direction string)(string)    | 返回一个字符串对应方向上的邻居（方向列表：North NorthEast East SouthEast South SouthWest West NorthWest） |
| geohashNeighborInt    | geohashNeighborInt(hash uint64,direction string)(uint64) | 返回一个无类型整数对应方向上的邻居（方向列表：North NorthEast East SouthEast South SouthWest West NorthWest） |
| geohashNeighbors      | geohashNeighbors(hash string)([]string)                  | 返回一个字符串的所有邻居                                     |
| geohashNeighborsInt   | geohashNeighborsInt(hash uint64)([]uint64)               | 返回一个无类型整数的所有邻居                                 |

 geohashEncode 示例

- 输入：`{"lo" :131.036192,"la":-25.345457}` 
- 输出：`{"geohashEncode":"qgmpvf18h86e"}`

```sql
SELECT geohashEncode(la,lo) FROM test
```

 geohashEncodeInt 示例

- 输入：`{"lo" :131.036192,"la":-25.345457}` 
- 输出：`{"geohashEncodeInt":12963433097944239317}`

```sql
SELECT geohashEncodeInt(la,lo) FROM test
```

 geohashDecode 示例

- 输入：`{"hash" :"qgmpvf18h86e"} ` 
- 输出：`{"geohashDecode":{"Longitude":131.036192,"Latitude":-25.345457099999997}}`

```sql
SELECT geohashDecode(hash) FROM test
```

geohashDecodeInt 示例

- 输入：`{"hash" :12963433097944239317}`
- 输出：`{"geohashDecodeInt":{"Longitude":131.03618861,"Latitude":-25.345456300000002}}`

```sql
SELECT geohashDecodeInt(hash) FROM test
```

 geohashBoundingBox  示例

- 输入：`{"hash" :"qgmpvf18h86e"} `
- 输出：`{"geohashBoundingBox":{"MinLat":-25.345457140356302,"MaxLat":-25.34545697271824,"MinLng":131.03619195520878,"MaxLng":131.0361922904849}}`

```sql
SELECT geohashBoundingBox(hash) FROM test
```

 geohashBoundingBoxInt  示例

- 输入：`{"hash" :12963433097944239317}`
- 输出：`{"geohashBoundingBoxInt":{"MinLat":-25.345456302165985,"MaxLat":-25.34545626025647,"MinLng":131.0361886024475,"MaxLng":131.03618868626654}}`

```sql
SELECT geohashBoundingBoxInt(hash) FROM test
```

geohashNeighbor 示例

- 输入：`{"hash" :"qgmpvf18h86e","direction":"North"} `
- 输出：`{"geohashNeighbor":"qgmpvf18h86s"}`

```sql
SELECT geohashNeighbor(hash,direction) FROM test
```

geohashNeighborInt 示例

- 输入：`{"hash" :12963433097944239317,"direction":"North"}`
- 输出：`{"geohashNeighborInt":12963433097944240129}`

```sql
SELECT geohashNeighborInt(hash,direction) FROM test
```

geohashNeighbors 示例

- 输入：`{"hash" :12963433097944239317}`
- 输出：`{"geohashNeighbors":["qgmpvf18h86s","qgmpvf18h86u","qgmpvf18h86g","qgmpvf18h86f","qgmpvf18h86d","qgmpvf18h866","qgmpvf18h867","qgmpvf18h86k"]}`

```sql
SELECT geohashNeighbors(hash) FROM test
```

geohashNeighborsInt 示例

- 输入： `{"hash" :"qgmpvf18h86e","neber":"North"}` 
- 输出：`{"geohashNeighborsInt":[12963433097944240129,12963433097944240131,12963433097944240130,12963433097944237399,12963433097944237397,12963433097944150015,12963433097944152746,12963433097944152747]}`

```sql
SELECT geohashNeighborsInt(hash) FROM test
```

### LabelImage plugin

该插件为展示使用 TensorFlowLite 模型的示例插件。此函数接收一个以 bytea 类型表示的图像的输入，输出该图像的根据 tflite 模型计算的标示。

如下 SQL 中，假设输入为 peacock.jpg 文件的二进制流，则输出为字符串 “peacock”。

```sql
SELECT labelImage(self) FROM tfdemo
```
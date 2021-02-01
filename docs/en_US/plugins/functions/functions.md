# Custom function

Kuiper can customize functions. For the development, compilation and use of functions, please [see here](../../extension/function.md).

### echo plugin

| Function | Example   | Description                     |
| -------- | --------- | ------------------------------- |
| echo     | echo(avg) | Output parameter value as it is |

echo(avg) example

- Assuming the type of avg is int and the value is 30, the result is: `[{"r1":30}]`

  ```
  SELECT echo(avg) as r1 FROM test;
  ```

### countPlusOne plugin

| Function     | Example           | Description                                       |
| ------------ | ----------------- | ------------------------------------------------- |
| countPlusOne | countPlusOne(avg) | Output the value of the parameter length plus one |

countPlusOne(avg) example

- Assuming the type of avg is []int and the value is `[1,2,3]`, the result is: `[{"r1":4}]`

  ```
  SELECT countPlusOne(avg) as r1 FROM test;
  ```

### accumulateWordCount plugin

| Function            | Example                      | Description                                  |
| ------------------- | ---------------------------- | -------------------------------------------- |
| accumulateWordCount | accumulateWordCount(avg,sep) | The function counts how many words there are |

accumulateWordCount(avg,sep) example

- Assuming that the avg type is string and the value is `My name is Bob`, the sep type is string and the value is a space, the result is: `[{"r1":4}]`

  ```
  SELECT accumulateWordCount(avg,sep) as r1 FROM test;
  ```

### Image processing plugin

Image processing currently only supports the formats of `png` and `jpeg` 

| Function  | Example                            | Description                                                  |
| --------- | ---------------------------------- | ------------------------------------------------------------ |
| resize    | resize(avg,width, height)          | Create a scaled image with new dimensions (width, height). If width or height is set to 0, it is set to the reserved value of aspect ratio |
| thumbnail | thumbnail(avg,maxWidth, maxHeight) | Reduce the image that retains the aspect ratio to the maximum size (maxWidth, maxHeight). |

resize(avg,width, height) example

- The avg type is []byte.

  ```
  SELECT resize(avg,width,height) as r1 FROM test;
  ```

thumbnail(avg,maxWidth, maxHeight) example

- The avg type is []byte.

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
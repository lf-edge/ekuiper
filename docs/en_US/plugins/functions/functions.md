# Custom function

Kuiper can customize functions. For the development, compilation and use of functions, please [see here](../../extension/function.md).

## echo plugin

| Function | Example   | Description                     |
| -------- | --------- | ------------------------------- |
| echo     | echo(avg) | Output parameter value as it is |

echo(avg) example

- Assuming the type of avg is int and the value is 30, the result is: `[{"r1":30}]`

  ```
  SELECT echo(avg) as r1 FROM test;
  ```

## countPlusOne plugin

| Function     | Example           | Description                                       |
| ------------ | ----------------- | ------------------------------------------------- |
| countPlusOne | countPlusOne(avg) | Output the value of the parameter length plus one |

countPlusOne(avg) example

- Assuming the type of avg is []int and the value is `[1,2,3]`, the result is: `[{"r1":4}]`

  ```
  SELECT countPlusOne(avg) as r1 FROM test;
  ```

## accumulateWordCount plugin

| Function            | Example                      | Description                                  |
| ------------------- | ---------------------------- | -------------------------------------------- |
| accumulateWordCount | accumulateWordCount(avg,sep) | The function counts how many words there are |

accumulateWordCount(avg,sep) example

- Assuming that the avg type is string and the value is `My name is Bob`, the sep type is string and the value is a space, the result is: `[{"r1":4}]`

  ```
  SELECT accumulateWordCount(avg,sep) as r1 FROM test;
  ```

## Image processing plugin

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

### Geohash plugin

| Function              | Example                                                  | Description                                                  |
| --------------------- | -------------------------------------------------------- | ------------------------------------------------------------ |
| geohashEncode         | geohashEncode(la,lo float64)(string)                     | Encode latitude and longitude as a string                    |
| geohashEncodeInt      | geohashEncodeInt(la,lo float64)(uint64)                  | Encode latitude and longitude as an unsigned integer         |
| geohashDecode         | geohashDecode(hash string)(la,lo float64)                | Decode a string into latitude and longitude                  |
| geohashDecodeInt      | geohashDecodeInt(hash uint64)(la,lo float64)             | Decode an unsigned integers into latitude and longitude      |
| geohashBoundingBox    | geohashBoundingBox(hash string)(string)                  | Returns the area encoded by a string                         |
| geohashBoundingBoxInt | geohashBoundingBoxInt(hash uint64)(string)               | Returns the area encoded by an unsigned integer              |
| geohashNeighbor       | geohashNeighbor(hash string,direction string)(string)    | Returns the neighbor in the corresponding direction of a string (Direction list: North NorthEast East SouthEast South SouthWest West NorthWest) |
| geohashNeighborInt    | geohashNeighborInt(hash uint64,direction string)(uint64) | Returns the neighbor in the corresponding direction of an unsigned integer (Direction list: North NorthEast East SouthEast South SouthWest West NorthWest) |
| geohashNeighbors      | geohashNeighbors(hash string)([]string)                  | Return all neighbors of a string                             |
| geohashNeighborsInt   | geohashNeighborsInt(hash uint64)([]uint64)               | Return all neighbors of an unsigned integer                  |

 geohashEncode example

- Input: `{"lo" :131.036192,"la":-25.345457}` 
- Output: `{"geohashEncode":"qgmpvf18h86e"}`

```sql
SELECT geohashEncode(la,lo) FROM test
```

 geohashEncodeInt example

- Input: `{"lo" :131.036192,"la":-25.345457}` 
- Output: `{"geohashEncodeInt":12963433097944239317}`

```sql
SELECT geohashEncodeInt(la,lo) FROM test
```

 geohashDecode example

- Input: `{"hash" :"qgmpvf18h86e"} ` 
- Output: `{"geohashDecode":{"Longitude":131.036192,"Latitude":-25.345457099999997}}`

```sql
SELECT geohashDecode(hash) FROM test
```

geohashDecodeInt example

- Input: `{"hash" :12963433097944239317}`
- Output: `{"geohashDecodeInt":{"Longitude":131.03618861,"Latitude":-25.345456300000002}}`

```sql
SELECT geohashDecodeInt(hash) FROM test
```

 geohashBoundingBox  example

- Input: `{"hash" :"qgmpvf18h86e"} `
- Output: `{"geohashBoundingBox":{"MinLat":-25.345457140356302,"MaxLat":-25.34545697271824,"MinLng":131.03619195520878,"MaxLng":131.0361922904849}}`

```sql
SELECT geohashBoundingBox(hash) FROM test
```

 geohashBoundingBoxInt  example

- Input: `{"hash" :12963433097944239317}`
- Output: `{"geohashBoundingBoxInt":{"MinLat":-25.345456302165985,"MaxLat":-25.34545626025647,"MinLng":131.0361886024475,"MaxLng":131.03618868626654}}`

```sql
SELECT geohashBoundingBoxInt(hash) FROM test
```

geohashNeighbor example

- Input: `{"hash" :"qgmpvf18h86e","direction":"North"} `
- Output: `{"geohashNeighbor":"qgmpvf18h86s"}`

```sql
SELECT geohashNeighbor(hash,direction) FROM test
```

geohashNeighborInt example

- Input:`{"hash" :12963433097944239317,"direction":"North"}`
- Output:`{"geohashNeighborInt":12963433097944240129}`

```sql
SELECT geohashNeighborInt(hash,direction) FROM test
```

geohashNeighbors example

- Input: `{"hash" :12963433097944239317}`
- Output: `{"geohashNeighbors":["qgmpvf18h86s","qgmpvf18h86u","qgmpvf18h86g","qgmpvf18h86f","qgmpvf18h86d","qgmpvf18h866","qgmpvf18h867","qgmpvf18h86k"]}`

```sql
SELECT geohashNeighbors(hash) FROM test
```

geohashNeighborsInt example

- Input:  `{"hash" :"qgmpvf18h86e","neber":"North"}` 
- Output: `{"geohashNeighborsInt":[12963433097944240129,12963433097944240131,12963433097944240130,12963433097944237399,12963433097944237397,12963433097944150015,12963433097944152746,12963433097944152747]}`

```sql
SELECT geohashNeighborsInt(hash) FROM test
```

### LabelImage plugin

This is a sample plugin to demonstrate the usage of TensorFlowLite(tflite) model interpreter. The function receives a bytea input representing an image and produce the AI label of the image by running the tflite model.

Assuming the input is the byte array of peacock.jpg, the output will be "peacock".

```sql
SELECT labelImage(self) FROM tfdemo
```
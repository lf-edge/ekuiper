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

 
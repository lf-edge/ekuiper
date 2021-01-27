# 定制函数

Kuiper 可以定制函数，函数的开发、编译及使用请[参见这里](../../extension/function.md)。

## echo 插件

| 函数 | 示例      | 说明           |
| ---- | --------- | -------------- |
| echo | echo(avg) | 原样输出参数值 |

echo(avg) 示例

- 假设 avg 类型为 int ，值为30， 则结果为: `[{"r1":30}]`

  ```
  SELECT echo(avg) as r1 FROM test;
  ```

## countPlusOne 插件

| 函数         | 示例              | 说明                 |
| ------------ | ----------------- | -------------------- |
| countPlusOne | countPlusOne(avg) | 输出参数长度加一的值 |

countPlusOne(avg) 示例

- 假设 avg 类型为 []int ，值为`[1,2,3]`， 则结果为: `[{"r1":4}]`

  ```
  SELECT countPlusOne(avg) as r1 FROM test;
  ```

## accumulateWordCount 插件

| 函数                | 示例                         | 说明                     |
| ------------------- | ---------------------------- | ------------------------ |
| accumulateWordCount | accumulateWordCount(avg,sep) | 函数统计一共有多少个单词 |

accumulateWordCount(avg,sep) 示例

- 假设 avg 类型为 string ，值为`My name is Bob`；sep  类型为 string ，值为空格,则结果为: `[{"r1":4}]`

  ```
  SELECT accumulateWordCount(avg,sep) as r1 FROM test;
  ```

## 图像处理插件

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

 
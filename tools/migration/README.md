## 1 程序说明及应用：

### 1.1 程序说明：

​    本程序用于历史数据迁移，将版本号小于1.0.2的 kuiper 数据迁移到1.0.2版本中。程序运行时将 `dada` 文件夹及其子目录下所有名为 `stores.data` 的文件数据迁移到 data 目录下的 `sqliteKV.db` 数据库中。若原数据存储路径为 `data/rule/store.data`,则迁移后数据位于`data/sqliteKV.db `路径下名为`rule`的表中。

### 1.2 编译程序：

执行 `go build -o tools/migration/migration tools/migration/main.go` 命令即可生成 migration 程序。

### 1.3 操作示例

用户需要提供 kuiper 的 data 文件夹的路径

```shell
./migration $(kuiper/data)
```

## 



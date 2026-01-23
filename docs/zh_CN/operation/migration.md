# 从 eKuiper 1.x 迁移到 2.x

本指南介绍从 eKuiper 1.x 升级到 2.x 时的重要破坏性变更和迁移步骤。

## 破坏性变更

### SQLite 数据库格式

eKuiper 2.x 在 SQLite 数据库（`sqliteKV.db`）中使用了不同的存储格式。这意味着：

- **1.x 创建的流和表无法被 2.x 读取**
- 尝试描述或使用旧的流会导致错误：`error unmarshall <name>, the data in db may be corrupted`

#### 格式变更

| 资源 | eKuiper 1.x | eKuiper 2.x |
|------|-------------|-------------|
| 流 | 纯 SQL 语句 | 包含 `streamType`, `streamKind`, `statement` 的 JSON |
| 表 | 纯 SQL 语句 | 包含 `streamType`, `streamKind`, `statement` 的 JSON |
| 规则 | 包含 `triggered` 字段的 JSON | 不包含 `triggered` 字段的 JSON |

## 迁移选项

### 选项 1：全新安装（推荐）

删除旧数据库，重新开始：

```bash
# 停止 eKuiper
docker stop ekuiper

# 删除旧数据库
rm -rf /kuiper/data/sqliteKV.db

# 启动 eKuiper 2.x（创建新数据库）
docker start ekuiper

# 通过 REST API 或 CLI 重新创建所有流和规则
```

### 选项 2：使用独立数据库文件

使用不同的文件名以保留旧数据库用于回滚：

在升级前编辑 `etc/kuiper.yaml`：

```yaml
store:
  sqlite:
    name: sqliteKV-v2.db
```

### 选项 3：删除损坏的条目

如果您已经升级并且有损坏的条目，可以通过 REST API 删除：

```bash
# 删除损坏的流
curl -X DELETE http://localhost:9081/streams/<stream_name>

# 删除损坏的表
curl -X DELETE http://localhost:9081/tables/<table_name>

# 然后重新创建
curl -X POST http://localhost:9081/streams \
  -d '{"sql": "CREATE STREAM my_stream () WITH (DATASOURCE=\"topic\", FORMAT=\"JSON\", TYPE=\"mqtt\")"}'
```

### 选项 4：直接操作数据库

对于批量清理，可以直接使用 SQLite：

```bash
# 列出所有流
sqlite3 /kuiper/data/sqliteKV.db "SELECT key FROM stream;"

# 删除指定流
sqlite3 /kuiper/data/sqliteKV.db "DELETE FROM stream WHERE key = 'my_stream';"

# 重启 eKuiper
docker restart ekuiper
```

## 其他说明

- 全新安装的 eKuiper 2.x 不受影响
- 升级前请务必备份数据库
- 建议在升级前使用 REST API 导出规则定义：
  ```bash
  curl http://localhost:9081/data/export > backup.json
  ```

## 另请参阅

- [安装指南](../installation.md)
- [REST API 参考](../api/restapi.md)

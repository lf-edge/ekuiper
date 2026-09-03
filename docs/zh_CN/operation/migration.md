# 从 eKuiper 1.x 迁移到 2.x

本指南介绍从 eKuiper 1.x 升级到 2.x 时的重要破坏性变更和迁移步骤。

## 破坏性变更

### v2.5 Checkpoint 格式

eKuiper v2.5 使用带版本号的快照格式写入 checkpoint。v2.5 可以恢复旧版本
写入的 checkpoint；但 v2.5 成功生成新 checkpoint 后，v2.4 及更早版本不能
读取该 checkpoint。

如果回滚时需要保留规则处理进度，请在升级前备份数据目录。若 v2.5 已写入
checkpoint，回滚时应恢复该备份；也可以删除并重新创建受影响的规则，使其从
新规则配置的 source 位置开始。

自定义 rewindable source 的 `GetOffset` 必须返回可被 gob 序列化的 offset。
该 offset 必须表示 ingest callback 最近接收的 tuple，并且在 callback 返回前
保持不变。若 `GetOffset` 返回错误，eKuiper 会拒绝 checkpoint，直到后续
ingest callback 成功取得有效 offset。

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

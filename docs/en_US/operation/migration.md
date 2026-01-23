# Migrating from eKuiper 1.x to 2.x

This guide covers important breaking changes and migration steps when upgrading from eKuiper 1.x to 2.x.

## Breaking Changes

### SQLite Database Format

eKuiper 2.x uses a different storage format for streams and tables in the SQLite database (`sqliteKV.db`). This means:

- **Streams and tables created in 1.x cannot be read by 2.x**
- Attempting to describe or use old streams results in: `error unmarshall <name>, the data in db may be corrupted`

#### Format Changes

| Resource | eKuiper 1.x | eKuiper 2.x |
|----------|-------------|-------------|
| Streams | Plain SQL statement | JSON with `streamType`, `streamKind`, `statement` |
| Tables | Plain SQL statement | JSON with `streamType`, `streamKind`, `statement` |
| Rules | JSON with `triggered` field | JSON without `triggered` field |

## Migration Options

### Option 1: Clean Installation (Recommended)

Start fresh by removing the old database:

```bash
# Stop eKuiper
docker stop ekuiper

# Remove old database
rm -rf /kuiper/data/sqliteKV.db

# Start eKuiper 2.x (creates new database)
docker start ekuiper

# Re-create all streams and rules via REST API or CLI
```

### Option 2: Separate Database File

Keep the old database for rollback capability by using a different filename:

Edit `etc/kuiper.yaml` before upgrading:

```yaml
store:
  sqlite:
    name: sqliteKV-v2.db
```

### Option 3: Delete Corrupted Entries

If you've already upgraded and have corrupted entries, delete them via REST API:

```bash
# Delete corrupted stream
curl -X DELETE http://localhost:9081/streams/<stream_name>

# Delete corrupted table  
curl -X DELETE http://localhost:9081/tables/<table_name>

# Then recreate
curl -X POST http://localhost:9081/streams \
  -d '{"sql": "CREATE STREAM my_stream () WITH (DATASOURCE=\"topic\", FORMAT=\"JSON\", TYPE=\"mqtt\")"}'
```

### Option 4: Direct Database Manipulation

For bulk cleanup, use SQLite directly:

```bash
# List all streams
sqlite3 /kuiper/data/sqliteKV.db "SELECT key FROM stream;"

# Delete specific stream
sqlite3 /kuiper/data/sqliteKV.db "DELETE FROM stream WHERE key = 'my_stream';"

# Restart eKuiper
docker restart ekuiper
```

## Additional Notes

- Fresh installations of eKuiper 2.x are not affected
- Always backup your database before upgrading
- Consider exporting your rule definitions using the REST API before upgrading:
  ```bash
  curl http://localhost:9081/data/export > backup.json
  ```

## See Also

- [Installation Guide](../installation.md)
- [REST API Reference](../api/restapi.md)

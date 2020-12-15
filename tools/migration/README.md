## 1 Program description and application:

### 1.1 Program description:

This program is used for historical data migration, which migrates Kuiper data whose version number is less than 1.0.2 to 1.0.2 version. When the program is running, all file data named `stores.data` in the `dada` folder and its subdirectories will be migrated to the `sqliteKV.db` database under the data directory. If the original data storage path is `data/rule/store.data`, the migrated data will be located in the table named `rule` under the path `data/sqliteKV.db`.

### 1.2 Compile the program:

Execute the `go build -o tools/migration/migration tools/migration/main.go` command to generate the migration program.

### 1.3 Operation example

The user needs to provide the path of Kuiper's data folder

```shell
./migration $(kuiper/data)
```

## 



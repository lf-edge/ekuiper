# 安全配置

eKuiper 支持加密配置，用于保护敏感数据如 AES 密钥、密码和 TLS 证书。

## 概述

配置分为两个文件：

| 文件            | 内容                           | 是否加密 |
|-----------------|--------------------------------|----------|
| `kuiper.yaml`   | 普通配置（日志级别、端口等）   | 否       |
| `kuiper.dat`    | 敏感配置（密钥、密码）         | 是       |

## 设置

### 1. 创建敏感配置文件

复制示例并编辑：

```bash
cp etc/kuiper.priv.yaml.example etc/kuiper.priv.yaml
```

编辑 `etc/kuiper.priv.yaml`：

```yaml
basic:
  aesKey: "your-base64-encoded-key"

store:
  redis:
    password: "your-redis-password"
```

### 2. 加密配置

```bash
go run tools/locker/main.go -i etc/kuiper.priv.yaml -o etc/kuiper.dat
```

### 3. 部署

一起部署两个文件：
- `etc/kuiper.yaml` - 用户可编辑配置
- `etc/kuiper.dat` - 加密的敏感配置

### 4. 保护源文件

添加到 `.gitignore`：

```text
etc/kuiper.priv.yaml
```

## 工作原理

启动时，eKuiper：
1. 加载 `kuiper.yaml`
2. 如果 `kuiper.dat` 存在，解密并合并
3. 敏感值覆盖普通配置

### 向后兼容性

是的，eKuiper 完全向后兼容。
- 如果 `kuiper.dat` 不存在，eKuiper 将仅使用 `kuiper.yaml` 正常运行。
- 如果不需要加密，您可以继续将敏感值放在 `kuiper.yaml` 中。

## Locker 工具

```bash
# 加密
go run tools/locker/main.go -i <input.yaml> -o <output.dat>

# 解密（用于调试）
go run tools/locker/main.go -d -i <input.dat> -o <output.yaml>
```

## 安全说明

- 加密密钥嵌入在二进制文件中
- 这可以防止随意检查，但无法阻止有决心的攻击者
- 生产环境请考虑额外的安全措施

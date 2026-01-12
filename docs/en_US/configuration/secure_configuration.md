# Secure Configuration

eKuiper supports encrypted configuration for sensitive data like AES keys, passwords, and TLS certificates.

## Overview

Configuration is split into two files:

| File            | Content                                | Encrypted |
|-----------------|----------------------------------------|-----------|
| `kuiper.yaml`   | Normal config (log level, ports, etc.) | No        |
| `kuiper.dat`    | Sensitive config (keys, passwords)     | Yes       |

## Setup

### 1. Create sensitive config file

Copy the example and edit:

```bash
cp etc/kuiper.priv.yaml.example etc/kuiper.priv.yaml
```

Edit `etc/kuiper.priv.yaml`:

```yaml
basic:
  aesKey: "your-base64-encoded-key"

store:
  redis:
    password: "your-redis-password"
```

### 2. Encrypt the config

```bash
go run tools/locker/main.go -i etc/kuiper.priv.yaml -o etc/kuiper.dat
```

### 3. Deploy

Deploy both files together:
- `etc/kuiper.yaml` - User-editable config
- `etc/kuiper.dat` - Encrypted sensitive config

### 4. Secure the source file

Add to `.gitignore`:

```text
etc/kuiper.priv.yaml
```

## How It Works

At startup, eKuiper:
1. Loads `kuiper.yaml`
2. If `kuiper.dat` exists, decrypts and merges it
3. Sensitive values override normal config

### Backward Compatibility

Yes, eKuiper is fully backward compatible.
- If `kuiper.dat` does not exist, eKuiper runs normally using only `kuiper.yaml`.
- You can continue putting sensitive values in `kuiper.yaml` if encryption is not required.

## Locker Tool

```bash
# Encrypt
go run tools/locker/main.go -i <input.yaml> -o <output.dat>

# Decrypt (for debugging)
go run tools/locker/main.go -d -i <input.dat> -o <output.yaml>
```

## Security Notes

- The encryption key is embedded in the binary
- This protects against casual inspection, not determined attackers
- For production, consider additional security measures

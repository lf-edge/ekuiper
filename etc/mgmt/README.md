# Management Keys

This directory is for RSA key pairs used by eKuiper JWT authentication.

## How to Generate Keys

```bash
# Generate private key
openssl genrsa -out mykey 2048

# Extract public key
openssl rsa -in mykey -pubout -out mykey.pub
```

## Usage

Set the issuer name in `kuiper.yaml`:

```yaml
basic:
  authentication: true
```

Then use the key name (filename) as the issuer when creating tokens.

## Security Notice

- Never commit private keys to version control
- Use different keys for development, testing, and production
- Rotate keys regularly

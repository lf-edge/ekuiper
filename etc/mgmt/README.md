# Management Keys

This directory contains only the RSA public keys used by eKuiper to verify JWTs.
Keep the corresponding private keys outside the eKuiper host and use them only
in the trusted service that issues tokens.

## How to Generate Keys

```bash
# Generate the private key in a secure location, not under etc/mgmt
umask 077
openssl genrsa -out /secure/path/ekuiper-jwt.key 2048

# Export only the public key to eKuiper
openssl rsa -in /secure/path/ekuiper-jwt.key -pubout \
  -out etc/mgmt/ekuiper-jwt.pub
```

## Usage

Enable JWT authentication in `kuiper.yaml`:

```yaml
basic:
  authentication: true
```

Sign tokens outside eKuiper with the private key. Set the JWT `iss` claim to
the exact public-key filename, for example `ekuiper-jwt.pub`. For containers
and Kubernetes deployments, mount or provision the public key into
`etc/mgmt`; do not include the private key in the image or volume.

## Security Notice

- Never copy private keys to the eKuiper host, image, or configuration volume
- Never commit private keys to version control
- Use different keys for development, testing, and production
- Rotate keys regularly

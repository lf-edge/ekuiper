## Authentication

eKuiper support `JWT RSA256` authentication for the RESTful management APIs since `1.4.0` if enabled . Users need put their Public Key in `etc/mgmt` folder and use the corresponding Private key to sign the JWT Tokens.
When user request the RESTful apis, put the `Token` in http request headers in the following format:

```go
Authorization: XXXXXXXXXXXXXXX
```

If the token is correct, eKuiper will respond the result; otherwise, it will return http `401`code.

### JWT Header

```json
{
  "typ": "JWT",
  "alg": "RS256"
}
```

### JWT payload

The JWT Payload should use the following format

| field | optional | meaning                                                               |
|-------|----------|-----------------------------------------------------------------------|
| iss   | false    | Issuer , must use the same name with the public key put in `etc/mgmt` |
| aud   | false    | Audience , must be `eKuiper`                                          |
| exp   | true     | Expiration Time                                                       |
| jti   | true     | JWT ID                                                                |
| iat   | true     | Issued At                                                             |
| nbf   | true     | Not Before                                                            |
| sub   | true     | Subject                                                               |

There is an example in json format

```json
{
  "iss": "ekuiper-jwt.pub",
  "aud": "eKuiper"
}
```

Generate your own RSA key pair and place only the public key, `ekuiper-jwt.pub`,
in `etc/mgmt`. No sample key pair is shipped. Keep the private key in the external
service that signs tokens. See the [management key setup instructions](../../../../etc/mgmt/README.md).

### JWT Signature

need use the Private key to sign the Tokens and put the corresponding Public Key in `etc/mgmt` .

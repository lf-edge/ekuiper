# Release Notes

## Security hardening

- REST API now binds to localhost by default.
- CLI service now binds to localhost by default.
- HTTP data ingestion server now binds to localhost by default.
- RPC can be disabled with `basic.disableRpc: true` (default: `false`).
- Prometheus and pprof also default to localhost, including omitted or empty bind addresses.
- External file access is disabled by default.
- Production deployments exposing REST API should enable authentication.

> **Behavior change:** By default the REST management API, the CLI service,
> and the HTTP data ingestion server now listen on `127.0.0.1` only.
> Remote devices pushing data over HTTP, and remote CLI/REST clients, will no
> longer be able to connect unless operators explicitly set the relevant bind
> address to a trusted interface IP. These defaults also apply to Helm deployments.
> Enable REST authentication and TLS for external access; isolate RPC port 20498
> with a firewall/VPN or disable RPC. REST security settings do not protect RPC.

See the [deployment trust boundary](docs/en_US/configuration/global_configurations.md#deployment-trust-boundary)
for administrator, source, downstream account and file sink responsibilities.

# eKuiper
A lightweight IoT edge analytic software

![Version: 1.3.0](https://img.shields.io/badge/Version-1.3.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 1.5.0](https://img.shields.io/badge/AppVersion-1.5.0-informational?style=flat-square)

## Install the Chart

- From GitHub
```
git clone https://github.com/lf-edge/ekuiper.git
cd deploy/chart/ekuiper
helm install my-ekuiper .
```

- From Helm repo
```
helm install my-ekuiper oci://ghcr.io/lf-edge/ekuiper-charts/ekuiper
```


## Uninstall Chart
```
helm uninstall my-ekuiper
```

## Image compatibility and release requirements

The checked-in `appVersion` still selects `lfedge/ekuiper:2.3.0-beta.5`. That image
predates `basic.disableRpc` and `basic.prometheusIp` and ignores those options.
Do not rely on them with the default image. Until a release containing these changes
is available, explicitly set `image.repository` and `image.tag` to an image built
from the updated source. The updated slim-python image also includes `wget` for the
loopback readiness probe; older slim-python images need a custom probe.

Before publishing a chart with these features:

1. Publish and validate the matching eKuiper images, including the slim-python variant.
2. Set `appVersion` to that published image version and increment the chart `version`.
3. Verify default loopback readiness and explicit Pod IP Service access with that image.

No future release number is assigned here. `basic.disableRpc` belongs under
`kuiperConfig["kuiper.yaml"].basic`; there is no RPC disable switch under `service.ports`.

## Listener configuration

Listeners default to `127.0.0.1`. Kubernetes Services cannot reach these listeners
through the Pod IP. For remote access, explicitly configure the appropriate trusted
interface IP under `kuiperConfig["kuiper.yaml"]` and enforce network access controls.
Enable REST authentication and TLS for external access. Isolate RPC port 20498 with
a firewall/VPN or set `basic.disableRpc: true`; this also disables the CLI.

The default readiness probe runs inside the container for the loopback REST listener
and requires `wget`. When configuring REST TLS or authentication, provide a suitable
`readinessProbe` override for your deployment.

For a dynamically assigned Pod IP, set `extraEnv` using the Kubernetes Downward API
and override the probe to use the Pod listener:

```yaml
extraEnv:
  - name: KUIPER__BASIC__RESTIP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
readinessProbe:
  httpGet:
    path: /ping
    port: 9081
  initialDelaySeconds: 5
  periodSeconds: 5
```

This opts REST into access through the Pod network and Service. Apply the access
controls above and adapt the probe for REST TLS or authentication. Without this
override, the default bind address remains `127.0.0.1`.

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` |  |
| clusterDomain | string | `"cluster.local"` | clusterDomain Kubernetes Cluster Domain |
| ekuiperEnv | object | `{"enabled":true,"key":{"mqttDefaultServer":"MQTT_SOURCE__DEFAULT__SERVER"},"value":{"mqttDefaultServer":"tcp://broker.emqx.io:1883"}}` | remove this when the mqtt_source configmap is available |
| extraEnv | list | `[]` | Additional environment variables, including Downward API references. |
| readinessProbe | object | `{}` | Override the default REST readiness probe. |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `"lfedge/ekuiper"` |  |
| image.tag | string | `""` | Overrides the image tag whose default is the chart appVersion. |
| nodeSelector | object | `{}` |  |
| persistence.enabled | bool | `false` |  |
| persistence.storageClass | string | ""  | Storage class of backing PVC|
| persistence.existingClaim| string | "" | EMQX data Persistent Volume existing claim name, evaluated as a template |
| persistence.accessMode   | string | ReadWriteOnce | PVC Access Mode for volume |
| persistence.size` | string | 20Mi | PVC Storage Request for EMQX volume |
| persistence.existingClaim | string | `""` | Existing PersistentVolumeClaims The value is evaluated as a template So, for example, the name can depend on .Release or .Chart |
| resources | object | `{}` |  |
| service.annotations | object | `{}` | Provide any additional annotations which may be required. Evaluated as a template |
| service.nodePorts | object | `{"ekuiper":null,"restapi":null}` | Specify the nodePort(s) value for the LoadBalancer and NodePort service types. ref: https://kubernetes.io/docs/concepts/services-networking/service/#type-nodeport |
| service.ports | object | `{"ekuiper":{"name":"ekuiper","port":20498},"restapi":{"name":"restapi","port":9081}}` | Service ports |
| service.ports.ekuiper.name | string | `"ekuiper"` | eKuiper port name |
| service.ports.ekuiper.port | int | `20498` | eKuiper port |
| service.ports.restapi.name | string | `"restapi"` | eKuiper restapi port name |
| service.ports.restapi.port | int | `9081` | eKuiper restapi port |
| service.type | string | `"ClusterIP"` | service type |
| serviceAccount.annotations | object | `{}` |  |
| serviceAccount.create | bool | `true` |  |
| serviceAccount.name | string | `""` |  |
| tls | object | `{"autoGenerated":false,"caCertificate":"","enabled":false,"existingSecret":"","existingSecretFullChain":false,"serverCertificate":"","serverKey":""}` | Enable encryption to eKuiper |
| tls.autoGenerated | bool | `false` | tls.autoGenerated Generate automatically self-signed TLS certificates |
| tls.caCertificate | string | `""` | tls.caCertificate Certificate Authority (CA) bundle content |
| tls.enabled | bool | `false` | tls.enabled Enable TLS support on eKuiper |
| tolerations | list | `[]` |  |

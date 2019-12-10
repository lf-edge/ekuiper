Kuiper 可以通过 Helm chart 部署在 k3s / k8s 集群上。
下面以 k3s 为例演示如何部署 Kuiper：

## Prepare：

+ 安装 K3S: 
  ```
  $ curl -sfL https://get.k3s.io | sh -
  $ sudo chmod 755 /etc/rancher/k3s/k3s.yaml
  $ kubectl get nodes
  NAME               STATUS   ROLES    AGE     VERSION
  ip-172-31-16-120   Ready    master   4m31s   v1.16.3-k3s.2
  ```

+ 安装 helm3
  ```
  $ curl -sfL https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash -
  Downloading https://get.helm.sh/helm-v3.0.1-linux-amd64.tar.gz
  Preparing to install helm into /usr/local/bin
  helm installed into /usr/local/bin/helm
  
  ## K8S 可以跳过这一步
  $ export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
  ```

## 获取 Kuiper Helm Chart

+ 可以通过一下两种方法的任意一种获取 Kuiper Helm Chart，本文以使用 `git clone` 拉取代码的方式为例讲解。

  + Git clone

    ```
    $ git clone https://github.com/emqx/kuiper
    $ cd kuiper/deploy/chart/kuiper
    ```

  + Helm repo (TODO)

    + 添加Helm repo

      ```
      $ helm repo add emqx https://repos.emqx.io/charts
      ```

    + 查询 Kuiper

      ```
      helm search kuiper
      NAME     		CHART VERSION	APP VERSION  	DESCRIPTION
      emqx/kuiper	0.0.3	        0.0.3	        A lightweight IoT edge analytic software
      ```

+ 可以通过编辑 `values.yaml` 文件或使用 `helm install --set` 命令编辑 Kuiper Helm Chart 的配置

  ##### Kuiper Helm Chart 配置项

  | 参数                           | 描述                                | Default Value            |
  | ------------------------------ | ----------------------------------- | ------------------------ |
  | `replicaCount`                 | 部署kuiper数量                      | 1                        |
  | `image.repository`             | 拉取镜像名称                        | emqx/kuiper              |
  | `image.pullPolicy`             | 拉取镜像策略                        | IfNotPresent             |
  | `persistence.enabled`          | 是否启用 PVC                        | false                    |
  | `persistence.storageClass`     | Storage class 名称                  | `nil`                    |
  | `persistence.existingClaim`    | PV 名称                             | ""                       |
  | `persistence.accessMode`       | PVC 访问模式                        | ReadWriteOnce            |
  | `persistence.size`             | PVC 容量                            | 20Mi                     |
  | `resources`                    | CPU/内存资源                        | {}                       |
  | `nodeSelector`                 | 节点选择                            | {}                       |
  | `tolerations`                  | 污点容忍                            | []                       |
  | `affinity`                     | 节点亲和性                          | {}                       |
  | `mqtt.servers`                 | mqtt服务器的代理地址                | `[tcp://127.0.0.1:1883]` |
  | `mqtt.qos`                     | 消息转发的服务质量                  | 1                        |
  | `mqtt.sharedSubscription`      | 是否使用共享订阅                    | true                     |
  | `mqtt.username`                | 连接用户名                          |                          |
  | `mqtt.password`                | 连接密码                            |                          |
  | `mqtt.certificationSecretName` | 通过证书文件创建的 Secre 资源的名字 |                          |
  | `mqtt.privateKeySecretName`    | 通过私钥文件创建的 Secre 资源的名字 |                          |
  | `mqtt.certificationPath`       | 证书路径。必须是绝对路径。          |                          |
  | `mqtt.privateKeyPath`          | 私钥路径。必须绝对路径。            |                          |

## 通过 Helm 部署 Kuiper

#### 快速部署Kuiper

+ 使用 Helm 部署 Kuiper

  ```
  $ helm install my-kuiper .
  NAME: my-kuiper
  LAST DEPLOYED: Mon Dec  9 09:56:32 2019
  NAMESPACE: default
  STATUS: deployed
  REVISION: 1
  TEST SUITE: None
  ```

+ 部署成功

  ```
  $ kubectl get pods
  NAME       READY   STATUS    RESTARTS   AGE
  my-kuiper-0   1/1     Running   0          19s

  $ kubectl exec -it  my-kuiper-0 sh
  /kuiper # ./bin/cli
  Connecting to 127.0.0.1:20498...
  ```

#### 部署持久化的 Kuiper

+ Kuiper 通过 创建 PVC 资源挂载 `/kuiper/data` 目录实现持久化 `pods`，**在部署 Kuiper 之前，用户需要自行在 Kubernetes 中创建 PVC 资源或 Storage Classes 资源**

+ 编辑 `values.yaml` 文件，设置 `persistence.enabled=true`

  + 如果用户部署了 PVC 资源，那么设置 `persistence.existingClaim=your_pv_name`
+ 如果用户部署了 Storage Classes 资源，那么设置`persistence.storageClass=your_storageClass_name`
  
+ 使用 Helm 部署 Kuiper

  ```
  $ helm install my-kuiper .
  NAME: my-kuiper
  LAST DEPLOYED: Mon Dec  9 09:56:32 2019
  NAMESPACE: default
  STATUS: deployed
  REVISION: 1
  TEST SUITE: None
  ```

+ 部署成功

  ```
  $ kubectl get pods
  NAME       READY   STATUS    RESTARTS   AGE
  my-kuiper-0   1/1     Running   0          19s

  $ kubectl exec -it  my-kuiper-0 sh
  /kuiper # ./bin/cli
  Connecting to 127.0.0.1:20498...
  ```

#### 部署Kuiper并使用证书

+ 使用 `kubectl create secret` 将证书文件和私钥创建成 Secret 资源，`kubectl create secret` 命令的语法如下：

  ```
  $ kubectl create secret generic your-secret-name --from-file=/path/to/file
  ```

  创建证书文件 Secret 资源：

  ```
  $ kubectl create secret generic client-cert --from-file=certs/client-cert.pem
  ```

  创建私钥文件 Secret 资源：

  ```
  $ kubectl create secret generic client-key --from-file=certs/client-key.pem
  ```

  查看 Secret 资源：

  ```
  $ kubectl get secret
  NAME                                         TYPE                                  DATA   AGE
  client-cert                                  Opaque                                1      25m
  client-key                                   Opaque                                1      24m
  ```

+ 编辑 `values.yaml` 文件

  + 设置 `mqtt.certificationSecretName` 为证书文件 Secret 资源： `mqtt.certificationSecretName: client-cert`
  + 设置 `mqtt.privateKeySecretName` 为私钥文件 Secret 资源：`mqtt.privateKeySecretName: client-key`
  + 设置证书文件部署路径：`mqtt.certificationPath: /var/kuiper/certificate.pem`
  + 设置私钥文件部署路径：`mqtt.privateKeyPath: /var/kuiper/private.pem.key`

+ 使用 Helm 部署 Kuiper

  ```
  $ helm install my-kuiper .
  NAME: my-kuiper
  LAST DEPLOYED: Mon Dec  9 09:56:32 2019
  NAMESPACE: default
  STATUS: deployed
  REVISION: 1
  TEST SUITE: None
  ```

+ 部署成功

  ```
  $ kubectl get pods
  NAME       READY   STATUS    RESTARTS   AGE
  my-kuiper-0   1/1     Running   0          19s

  $ kubectl exec -it my-kuiper-0 -- ls -al /var/kuiper
  total 8
  drwxr-xr-x    4 root     root          4096 Dec 10 02:23 .
  drwxr-xr-x    1 root     root          4096 Dec 10 02:23 ..
  drwxrwxrwt    3 root     root           100 Dec 10 02:23 certificate.pem
  drwxrwxrwt    3 root     root           100 Dec 10 02:23 private.pem.key

  $ kubectl exec -it  my-kuiper-0 sh
  /kuiper # ./bin/cli
  Connecting to 127.0.0.1:20498...
  ```
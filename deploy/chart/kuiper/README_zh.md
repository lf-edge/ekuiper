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

## 通过 Helm 部署 Kuiper

+ 获取项目代码

  ```
  $ git clone https://github.com/emqx/kuiper
  $ cd kuiper/deploy/chart/kuiper
  ```

+ 可以通过编辑 `values.yaml` 文件修改部署配置

  ##### Kuiper Helm Chart 配置项

  | 参数                        | 描述                                                         | Default Value            |
  | --------------------------- | ------------------------------------------------------------ | ------------------------ |
  | `replicaCount`              | 部署kuiper数量                                               | 1                        |
  | `image.repository`          | 拉取镜像名称                                                 | emqx/kuiper              |
  | `image.pullPolicy`          | 拉取镜像策略                                                 | IfNotPresent             |
  | `persistence.enabled`       | 是否启用 PVC                                                 | false                    |
  | `persistence.storageClass`  | Storage class 名称                                           | `nil`                    |
  | `persistence.existingClaim` | PV 名称                                                      | ""                       |
  | `persistence.accessMode`    | PVC 访问模式                                                 | ReadWriteOnce            |
  | `persistence.size`          | PVC 容量                                                     | 20Mi                     |
  | `resources`                 | CPU/内存资源                                                 | {}                       |
  | `nodeSelector`              | 节点选择                                                     | {}                       |
  | `tolerations`               | 污点容忍                                                     | []                       |
  | `affinity`                  | 节点亲和性                                                   | {}                       |
  | `mqtt.servers`              | mqtt服务器的代理地址                                         | `[tcp://127.0.0.1:1883]` |
  | `mqtt.qos`                  | 消息转发的服务质量                                           | 1                        |
  | `mqtt.sharedSubscription`   | 是否使用共享订阅                                             | true                     |
  | `mqtt.username`             | 连接用户名                                                   |                          |
  | `mqtt.password`             | 连接密码                                                     |                          |
  | `mqtt.certificationPath`    | 证书路径。可以为绝对路径，也可以为相对路径。如果指定的是相对路径，那么父目录为执行`server`命令的路径。比如，如果你在`/var/kuiper` 中运行 `bin/server` ，那么父目录为 `/var/kuiper`; 如果运行从`/var/kuiper/bin`中运行`./server`，那么父目录为 `/var/kuiper/bin`。 |                          |
  | `mqtt.privateKeyPath`       | 私钥路径。可以为绝对路径，也可以为相对路径。更详细的信息，请参考 `certificationPath`. |                          |

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
  ```

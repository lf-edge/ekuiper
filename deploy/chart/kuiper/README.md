eKuiper can be deployed at k3s/k8s cluster through Helm chart. Below takes k3s as an example for demonstrating how to deploy at k3s.

## Prepare

+ Install K3S
  ```shell
  $ curl -sfL https://get.k3s.io | sh -
  $ sudo chmod 755 /etc/rancher/k3s/k3s.yaml
  $ kubectl get nodes
  NAME               STATUS   ROLES    AGE     VERSION
  ip-172-31-16-120   Ready    master   4m31s   v1.16.3-k3s.2
  ```

+ Install helm3
  ```shell
  $ curl -sfL https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash -
  Downloading https://get.helm.sh/helm-v3.0.1-linux-amd64.tar.gz
  Preparing to install helm into /usr/local/bin
  helm installed into /usr/local/bin/helm
  
  ## K8S can skip this step
  $ export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
  ```

## Get eKuiper Helm Chart

+ With any approaches in below to get eKuiper Helm Chart, here uses `git clone` to get Helm chart.

  + Git clone

    ```shell
    $ git clone https://github.com/lf-edge/ekuiper
    $ cd ekuiper/deploy/chart/Kuiper
    ```

  + Helm repo (TODO)

    + Add Helm repo

      ```shell
      $ helm repo add emqx https://repos.emqx.io/charts
      ```

    + Search Kuiper

      ```shell
      helm search kuiper
      NAME     		CHART VERSION	APP VERSION  	DESCRIPTION
      lfedge/ekuiper	0.0.3	        0.0.3	        A lightweight IoT edge analytic software
      ```

+ By edit  `values.yaml` file or use command `helm install --set` to edit ``eKuiper Helm Chart`` configurations.

  ##### eKuiper Helm Chart Configurations 

  | Parameters                     | Descriptions                                         | Default Value            |
  | ------------------------------ | ---------------------------------------------------- | ------------------------ |
  | `replicaCount`                 | Deployed eKuiper instance number                      | 1                        |
  | `image.repository`             | Docker image name                                    | lfedge/ekuiper              |
  | `image.pullPolicy`             | Pull policy                                          | IfNotPresent             |
  | `service.type`  | Kubernetes Service type. |ClusterIP|
  | `service.kuiper`  | Port for eKuiper. |20498|
  | `service.restapi`  | Port for ReseApi. |9081|
  | `service.nodePorts.kuiper`  | Kubernetes node port for eKuiper. |nil|
  | `service.nodePorts.restapi` | Kubernetes node port for RestAPi. |nil|
  | `service.loadBalancerIP`  | loadBalancerIP for Service |	nil |
  | `service.loadBalancerSourceRanges` |	Address(es) that are allowed when service is LoadBalancer |	[] |
  | `service.annotations` |	Service annotations |	{}(evaluated as a template)|
  | `persistence.enabled`          | Enable PVC                                           | false                    |
  | `persistence.storageClass`     | Storage class name                                   | `nil`                    |
  | `persistence.existingClaim`    | PV name                                              | ""                       |
  | `persistence.accessMode`       | PVC access mode                                      | ReadWriteOnce            |
  | `persistence.size`             | PVC size                                             | 20Mi                     |
  | `resources`                    | CPU/Memory                                           | {}                       |
  | `nodeSelector`                 | Node selector                                        | {}                       |
  | `tolerations`                  | Tolerations                                          | []                       |
  | `affinity`                     | Affinity                                             | {}                       |
  | `kuiperConfig`                 | Configuration file in the eKuiper `etc` directory     |                          |

## Deploy eKuiper through Helm

#### Deploy eKuiper quickly

+ Deploy eKuiper through Helm

  ```shell
  $ helm install my-kuiper .
  NAME: my-kuiper
  LAST DEPLOYED: Mon Dec  9 09:56:32 2019
  NAMESPACE: default
  STATUS: deployed
  REVISION: 1
  TEST SUITE: None
  ```

+ Deployment is successful

  ```shell
  $ kubectl get pods
  NAME       READY   STATUS    RESTARTS   AGE
  my-kuiper-0   1/1     Running   0          19s
  
  $ kubectl exec -it  my-kuiper-0 sh
  /kuiper # ./bin/kuiper
  Connecting to 127.0.0.1:20498...
  ```

#### Deploy persisted eKuiper

+ eKuiper realized persisted  `pods` through creating PVC resources and mount `/ekuiper/data` directory. **Before deploying Kuiper, user need to create PVC or Storage Classes resource in Kubernetes.**

+ Open and edit `values.yaml` file, set  `persistence.enabled=true`

  + If user deploys PVC resource, , then set`persistence.existingClaim=your_pv_name`
  + If user deploys Storage Classes resource, then set `persistence.storageClass=your_storageClass_name`

+ Deploy eKuiper through Helm 

  ```
  $ helm install my-kuiper .
  NAME: my-kuiper
  LAST DEPLOYED: Mon Dec  9 09:56:32 2019
  NAMESPACE: default
  STATUS: deployed
  REVISION: 1
  TEST SUITE: None
  ```

+ Deployment is successful

  ```shell
  $ kubectl get pods
  NAME       READY   STATUS    RESTARTS   AGE
  my-kuiper-0   1/1     Running   0          19s
  
  $ kubectl exec -it  my-kuiper-0 sh
  /kuiper # ./bin/kuiper
  Connecting to 127.0.0.1:20498...
  ```

#### Deploy eKuiper and using SSL certification and key

+ Use command `kubectl create secret` , create certification & private keys to ``Secret resources``, the usage of command `kubectl create secret`  is listed as in below：

  ```shell
  $ kubectl create secret generic your-secret-name --from-file=/path/to/file
  ```

  Create Secret resource for certification file: 

  ```shell
  $ kubectl create secret generic client-cert --from-file=certs/client-cert.pem
  ```

  Create Secret for private key file: 

  ```shell
  $ kubectl create secret generic client-key --from-file=certs/client-key.pem
  ```

  Review Secret resources

  ```shell
  $ kubectl get secret
  NAME                                         TYPE                                  DATA   AGE
  client-cert                                  Opaque                                1      25m
  client-key                                   Opaque                                1      24m
  ```

+ Open and edit `values.yaml` file

  ```shell
  $ vim value.yaml
  kuiperConfig:
  ...
    "mqtt_source.yaml":
      #Global MQTT configurations
      default:
        qos: 1
        sharedSubscription: true
        server: tcp://127.0.0.1:1883
        concurrency: 1
        #username: user1
        #password: password
        certificationSecretName: client-cert  # Set certification Secret resource name
        certificationPath: /var/kuiper/certificate.pem # Set certification file path
        privateKeySecretName: client-key  # Set private key Secret resource name
        privateKeyPath: /var/kuiper/xyz-private.pem.key # Set private key file path
  ...
  ```

+ Deploy eKuiper through Helm 

  ```shell
  $ helm install my-kuiper .
  NAME: my-kuiper
  LAST DEPLOYED: Mon Dec  9 09:56:32 2019
  NAMESPACE: default
  STATUS: deployed
  REVISION: 1
  TEST SUITE: None
  ```

+ Deployment is successful

  ```shell
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
  /kuiper # ./bin/kuiper
  Connecting to 127.0.0.1:20498...
  ```
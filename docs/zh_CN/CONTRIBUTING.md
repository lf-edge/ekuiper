# 如何贡献

很高兴你能读到这篇文章，欢迎加入项目社区，帮助项目成长。

## 发现 bug ？

- **通过在 GitHub 的[问题](https://github.com/lf-edge/ekuiper/issues)下搜索，确保该错误尚未被报告**。
- 如果你找不到解决该问题的公开问题，[开一个新问题](https://github.com/lf-edge/ekuiper/issues/new)。请确保**标题和清晰的描述**，尽可能多的相关信息，以及**代码样本**或**可执行的测试案例**，以明确问题。

## 代码和文档贡献

欢迎贡献代码以提供功能或修复错误。

### 一次性设置

我们使用 GitHub pull request 来审查提议的代码修改。所以你需要在做出代码贡献之前拥有一个 GitHub 账户。

1. **Fork** eKuiper到你的私人仓库。点击 eKuiper 仓库右上角的 "Fork "按钮。
2. 从你的个人分叉中**克隆**版本库。 `git clone https://github.com/<Github_user>/ekuiper.git` 。
3. 添加 eKuiper repo 作为额外的 Git 远程仓库，这样你就可以在本地 repo 和eKuiper 之间进行同步。
   ```shell
      git remote add upstream https://github.com/lf-edge/ekuiper.git
   ```

你可以使用你喜欢的IDE或编辑器来开发。你可以在 [Editors and IDEs for GO](https://github.com/golang/go/wiki/IDEsAndTextEditorPlugins) 中找到编辑器对Go工具的支持信息。

### 创建一个分支

你将在你自己的 repo 中的一个分支中进行你的代码开发。创建一个本地分支，初始化为你希望合并到的分支的状态。`master`分支是活跃的开发分支，所以建议将`master`设为基础分支。

```shell
$ git fetch upstream
$ git checkout -b <my-branch> upstream/master
```

### 代码惯例

- 在提交代码变更之前，使用 `go fmt` 来格式化你的代码。
- 配置文件中的配置键使用 camel 大小写格式。

### 调试你的代码

以 GoLand 为例，开发者可以对代码进行调试。

1. 调试整个程序。确保 [Makefile](https://github.com/lf-edge/ekuiper/blob/master/Makefile) build_prepare 部分提到的所有目录都在你的eKuiper根路径中创建。添加你的断点。打开 `cmd/kuiperd/main.go` 。在主函数中，你会发现标尺上有一个绿色的三角形，点击它并选择调试。然后创建你的流/规则，让它运行到你的断点，调试器会在那里暂停。
2. 要调试一小部分代码，我们建议写一个单元测试并调试它。你可以到任何一个测试文件中，找到同样的绿色三角形，在调试模式下运行。例如，`pkg/cast/cast_test.go` TestMapConvert_Funcs 可以作为调试运行。

#### 调试 edgex 代码

用户可以修改 edgex 源/目标代码以满足他们的要求。在这种情况下，最佳实践是让其他服务在 docker 模式下运行，但 eKuiper 在本地运行。
用户可以按照以下步骤设置环境

#### 暴露消息总线

eKuiper 按主题订阅消息，默认情况下，edgex 使用 redis 作为消息总线。本指南将使用 redis 作为示例来展示如何公开消息总线。
在 docker-compose 文件中，找到 redis 服务并在端口部分更改 127.0.0.1:6379
到 0.0.0.0:6379，然后重启所有服务。

```yaml
 database:
    container_name: edgex-redis
    environment:
      CLIENTS_CORE_COMMAND_HOST: edgex-core-command
      CLIENTS_CORE_DATA_HOST: edgex-core-data
      CLIENTS_CORE_METADATA_HOST: edgex-core-metadata
      CLIENTS_SUPPORT_NOTIFICATIONS_HOST: edgex-support-notifications
      CLIENTS_SUPPORT_SCHEDULER_HOST: edgex-support-scheduler
      DATABASES_PRIMARY_HOST: edgex-redis
      EDGEX_SECURITY_SECRET_STORE: "false"
      REGISTRY_HOST: edgex-core-consul
    hostname: edgex-redis
    image: redis:6.2-alpine
    networks:
      edgex-network: {}
    ports:
    - 0.0.0.0:6379:6379/tcp
    read_only: true
    restart: always
    security_opt:
    - no-new-privileges:true
    user: root:root
    volumes:
    - db-data:/data:z

```

#### 修改本地 edgex 的配置

根据消息总线类型更改 edgex 源配置，下表为消息总线配置
该文件位于 `etc/sources/edgex.yaml` 中。

| message bus   | type  | protocol | server       | port |
|---------------|-------|----------|--------------|------|
| redis  server | redis | redis    | 10.65.38.224 | 6379 |
| mqtt  broker  | mqtt  | tcp      | 10.65.38.224 | 1883 |
| zemo mq       | zero  | tcp      | 10.65.38.224 | 5566 |

以redis为例，下面的配置会让eKuiper连接到10.65.38.224的6379端口。

```yaml
default:
  protocol: redis
  server: 10.65.38.224
  port: 6379
  topic: rules-events
  type: redis
  # Could be 'event' or 'request'.
  # If the message is from app service, the message type is an event;
  # Otherwise, if it is from the message bus directly, it should be a request
  messageType: event
```

修改后 redis 会监听主机的6379端口，开发者可以通过服务器地址连接到edgex远程运行的机器。
例如机器 ip 地址为 10.65.38.224 ，用户可以通过该ip地址连接。

#### 打开 eKuiper 的控制台日志，修改 rest 端口

更改 `etc/kuiper.yaml` 中的配置文件，将控制台日志设置为 true 并将 eKuiper rest api 端口设置为 59720
```yaml
basic:
  # true|false, with debug level, it prints more debug info
  debug: false
  # true|false, if it's set to true, then the log will be print to console
  consoleLog: true
  # true|false, if it's set to true, then the log will be print to log file
  fileLog: true
  # How many hours to split the file
  rotateTime: 24
  # Maximum file storage hours
  maxAge: 72
  # CLI ip
  ip: 0.0.0.0
  # CLI port
  port: 20498
  # REST service ip
  restIp: 0.0.0.0
  # REST service port
  restPort: 59720
  # true|false, when true, will check the RSA jwt token for rest api
  authentication: false
  #  restTls:
  #    certfile: /var/https-server.crt
  #    keyfile: /var/https-server.key
  # Prometheus settings
  prometheus: false
  prometheusPort: 20499
  # The URL where hosts all of pre-build plugins. By default it's at packages.emqx.net
  pluginHosts: https://packages.emqx.net
  # Whether to ignore case in SQL processing. Note that, the name of customized function by plugins are case-sensitive.
  ignoreCase: true
```

#### run locally

使用前面提到的[启动方法](./CONTRIBUTING.md#调试你的代码) 运行 eKuiper

### 测试

eKuiper 项目利用 Github actions 来运行单元测试和 FVT（功能验证测试），所以请看一下 PR 状态的运行结果，并确保所有的测试用例都能成功运行。

- 如果有必要，请编写 Golang 单元测试案例来测试你的代码。
- [FVT测试案例](../../test/README.md) 将随着任何PR提交而被触发，所以请确保这些测试案例能够成功运行。

### 许可

所有贡献给eKuiper的代码都将在Apache License V2下授权。你需要确保你添加的每个新文件都有正确的许可证头。

### Signoff

Signoff 是为了证明提交的来源。它是提交给这个项目的必要条件。如果你设置了
你的`user.name`和`user.email`的 git 配置，你可以用`git commit -s`自动签署你的提交。每次提交都必须签名。

### 同步

在提交 PR 之前，你应该用目标分支的最新改动来更新你的分支。我们倾向于使用 rebase 而不是 merge，以避免不必要的合并提交。

```shell
git fetch upstream
git rebase upstream/master
```

假设你 forked repo 名称是默认的`origin`， 使用如下指令推送改动到你的 forked repo。假设你 forked repo 名称是默认的`origin`。如果你在最后一次推送前重新建立了 git 历史，请添加 `-f` 来强制推送这些变化。

```shell
git push origin -f
```

### 提交修改

`master` 分支是活跃的开发分支，所以建议将 `master` 设为基础分支，并在 `master` 分支下创建PR

组织你的提交，使提交者在审查时更容易。提交者通常喜欢多个小的拉动请求，而不是一个大的拉动请求。在一个拉动请求中，最好有相对较少的提交，将问题分解成合理的步骤。对于大多数 PR ，你可以将你的修改压缩到一个提交。你可以使用下面的命令来重新排序、合并、编辑或改变单个提交的描述。

```shell
git rebase -i upstream/master
```

确保你的所有提交都符合[提交信息指南](#提交信息指南)。

然后你会推送到你 forked 的 repo 上的分支，然后导航到 eKuiper repo 创建一个 PR 。我们的 GitHub repo 提供了基于 GitHub actions的自动化测试。请确保这些测试通过。我们将在所有测试通过后审查代码。

### 提交信息指南

每条提交信息都由一个 **header** ，一个 **body** 和一个 **footer** 组成。header 包含三个部分：**类型**，**范围**和**主题**。

```
<类型>(<范围>): <主题>
<空行>
<body>
<空行>
<footer>
```

**header** 的**类型**为必填项。header 的**范围**是可选的。没有预定义的范围选项，可以使用一个自定义的范围。

提交信息的任何一行都不能超过100个字符，这样可以使信息在 GitHub 以及各种 git 工具中更容易阅读。

如果有的话，footer 应该包含一个 [对问题的关闭引用](https://help.github.com/articles/closing-issues-via-commit-messages/)。

例子1:

```
feat: 添加编译文件
```

```
fix(script): 纠正运行脚本以使用正确的端口

以前的设备服务使用了错误的端口号。这个提交修正了端口号，使用最新的端口号。

关闭。#123, #245, #992
```

#### Revert

如果该提交是为了恢复之前的提交，它应该以 `revert: `开头，然后是被恢复的提交的标题。在正文中，应该说："这是对提交 hash 的恢复"，其中的 hash 是被恢复的提交的 SHA 值。

#### 类型

必须是以下类型之一:

- **feat**。为用户提供的新功能，而不是构建脚本的新功能
- **fix**: 为用户提供的错误修复，而不是对构建脚本的修复
- **docs**: 只对文档进行修改
- **style**: 格式化，缺少分号，等等；没有生产代码的变化
- **refactor**: 重构生产代码，例如重命名一个变量。
- **chore**: 更新脚本任务等；不改变生产代码
- **perf**: 提高性能的代码变化
- **test**: 添加缺失的测试，重构测试；不改变生产代码
- **build**: 影响 CI/CD 管道或构建系统或外部依赖的变化（例如 makefile）。
- **ci**: 由 DevOps 提供的用于 CI 目的的改变。
- **revert**: 恢复先前的提交。

#### 范围

这个版本库没有预定义的范围。为了清晰起见，可以提供一个自定义的范围。

#### 主题

主题包含对修改的简洁描述。

- 使用祈使句、现在时："改变 "而不是 "改变 "或 "变化"
- 不要把第一个字母大写
- 结尾不加点（...）。

#### body

与主题一样，使用祈使句、现在时："改变 "而不是 "改变 "或 "变化"。主体应该包括改变的动机，并与以前的行为进行对比。

#### footer

页脚应该包含任何关于**突破性变化的信息，同时也是引用此提交**关闭的 GitHub 问题的地方。

**Breaking Changes** 应该以 "BREAKING CHANGE: "开头，并加上一个空格或两个换行。提交信息的其余部分就用于此了。

## 社区推广

除了编码，其他类型的贡献也是参与的好方法。欢迎通过以下方式为这个项目做出贡献
向开源社区和世界推广它。

推广贡献包括但不限于。

- 将 eKuiepr 整合到你的开源项目中。
- 组织关于本项目的研讨会或聚会
- 在 issues、slack 或 maillist 上回答关于本项目的问题
- 撰写项目的使用教程
- 为其他贡献者提供指导

感谢你的贡献!
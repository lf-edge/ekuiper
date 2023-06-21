# How to contribute

We're really glad you're reading this, because we need volunteer developers to help this project come to fruition.

## Did you find a bug?

- **Ensure the bug was not already reported** by searching on GitHub under [Issues](https://github.com/lf-edge/ekuiper/issues).
- If you're unable to find an open issue addressing the problem, [open a new one](https://github.com/lf-edge/ekuiper/issues/new). Be sure to include a **title and clear description**, as much relevant information as possible, and a **code sample** or an **executable test case** demonstrating the expected behavior that is not occurring.

## Code and doc contribution

Welcome to contribute code to provide features or fix bugs.

### One time setup

We use GitHub pull request to review proposed code changes. So you'll need to obtain a GitHub account before making code contribution.

1. **Fork** eKuiper to your private repository. Click the `Fork` button in the top right corner of eKuiper repository.
2. **Clone** the repository locally from your personal fork. `git clone https://github.com/<Github_user>/ekuiper.git`.
3. Add eKuiper repo as additional Git remote so that you can sync between local repo and eKuiper.

  ```shell
  git remote add upstream https://github.com/lf-edge/ekuiper.git
  ```

You can use your favorite IDE or editor to develop. You can find information in editor support for Go tools in [Editors and IDEs for GO](https://github.com/golang/go/wiki/IDEsAndTextEditorPlugins).

### Create a branch in your fork

You’ll work on your contribution in a branch in your own (forked) repository. Create a local branch, initialized with the state of the branch you expect your changes to be merged into. The `master` branch is active development branch, so it's recommended to set `master` as base branch.

```shell
$ git fetch upstream
$ git checkout -b <my-branch> upstream/master
```

### Package Import Specification

Reasonable package import order can enhance the cleanliness and standardization of the code.
This project uses gci to automatically check the order of package imports, with priority given
to `standard packages` > `third-party external packages` > `local project packages`, as follows:

```go
import (
    "fmt"

    "github.com/sirupsen/logrus"

    "github.com/lf-edge/ekuiper/pkg/api"
)
```

In the project root directory, you can run the command `gci write --skip-generated -s standard -s default -s "prefix(github.com/lf-edge/ekuiper)" .` to
automatically reorder package imports.

Alternatively, if you use GoLand, you can check `Group` and `Group stdlib imports` as well as their sub-options under
`Settings > Editor > Code Style > Go > Imports` to enable automatic import sorting.

### Code conventions

- Use `go fmt` to format your code before commit code change. eKuiper Github Action CI pipeline reports error if it's
  not format by `go fmt`.
- Run static code analysis with `make lint` to make sure there are no stylistic errors and common programming issues.
  - If you encounter lint errors related to `gofumpt`, run `gofumpt -w .` in the project root directory to solve it.
  - Check [golangci-lint](https://golangci-lint.run/) for more information on the corresponding lint errors.
- Configuration key in config files uses camel case format.

### Debug your code

Take GoLand as an example, developers can debug the code:

1. Debug the whole program. Make sure all directories mentioned in [Makefile](https://github.com/lf-edge/ekuiper/blob/master/Makefile) build_prepare sections are created in your eKuiper root path. Add your breakpoints. Open `cmd/kuiperd/main.go`. In the main function, you'll find a green triangle in the ruler, click it and select debug. Then create your stream/rule that would run through your breakpoint, the debugger will pause there.
2. To debug a small portion of code, we recommend writing a unit test and debug it. You can go to any test file and find the same green triangle to run in debug mode. For example, `pkg/cast/cast_test.go` TestMapConvert_Funcs can run as debug.

#### Debug edgex code

Users can modify edgex source/sink code to meet their requirement. In this case, the best practice is letting the other services running in docker mode but eKuiper run locally.
Users can follow these steps to set up the environment.

#### expose message bus

  eKuiper subscribe messages by topic and by default edgex is using redis as message bus. This guide will use redis as example to show how to expose message bus.
  In the docker-compose file, find the redis service and in ports part change 127.0.0.1:6379
to 0.0.0.0:6379, then restart all the services.

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

#### change edgex local config

Change edgex source config according to message bus type, the following table is message bus configuration
the file locates in `etc/sources/edgex.yaml`.

| message bus   | type  | protocol | server       | port |
|---------------|-------|----------|--------------|------|
| redis  server | redis | redis    | 10.65.38.224 | 6379 |
| mqtt  broker  | mqtt  | tcp      | 10.65.38.224 | 1883 |
| zemo mq       | zero  | tcp      | 10.65.38.224 | 5566 |

Take the redis as example, the following config will let eKuiper connect to 10.65.38.224's 6379 port.

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

After changing this, redis will listen on the host 6379 port, developers can connect to the machine that edgex runs remotely by the server address.
For example, the host ip address is 10.65.38.224 , users can connect to this machine by the ip address.

#### enable eKuiper console log and set rest api port

Change the config file in `etc/kuiper.yaml`, set the console log true and set eKuiper rest api port to 59720

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

  Use the [former method](./CONTRIBUTING.md#debug-your-code) to run the eKuiper

### Testing

The eKuiper project leverages Github actions to run unit test & FVT (functional verification test), so please take a
look at the PR status result, and make sure that all of testcases run successfully.

- Write Golang unit testcases to test your code if necessary.
- A set of [FVT testcases](../../test/README.md) will be triggered with any PR submission, so please make sure that these
  testcases can be run successfully.

### Licensing

All code contributed to eKuiper will be licensed under Apache License V2. You need to ensure every new files you are adding have the right license header.

### Sign-off commit

The sign-off is to certify the origin of the commit. It is required to commit to this project. If you set
your `user.name` and `user.email` git configs, you can sign your commit automatically with `git commit -s`. Each commit must be signed off.

### Syncing

Periodically while you work, and certainly before submitting a pull request, you should update your branch with the most recent changes to the target branch. We prefer rebase than merge to avoid extraneous merge commits.

```shell
git fetch upstream
git rebase upstream/master
```

Then you can push to your forked repo. Assume the remove name for your forked is the default `origin`. If you have rebased the git history before the last push, add `-f` to force pushing the changes.

```shell
git push origin -f
```

### Submitting changes

The `master` branch is active development branch, so it's recommended to set `master` as base branch, and also create PR
against `master` branch.

Organize your commits to make a committer’s job easier when reviewing. Committers normally prefer multiple small pull requests, instead of a single large pull request. Within a pull request, a relatively small number of commits that break the problem into logical steps is preferred. For most pull requests, you'll squash your changes down to 1 commit. You can use the following command to re-order, squash, edit, or change description of individual commits.

```shell
git rebase -i upstream/master
```

Make sure all your commits comply to the [commit message guidelines](#commit-message-guidelines).

You'll then push to your branch on your forked repo and then navigate to eKuiper repo to create a pull request. Our GitHub repo provides automatic testing with GitHub action. Please make sure those tests pass. We will review the code after all tests passed.

### Commit Message Guidelines

Each commit message consists of a **header**, a **body** and a **footer**. The header has a special format that includes a **type**, a **scope** and a **subject**:

```text
<type>(<scope>): <subject>
<BLANK LINE>
<body>
<BLANK LINE>
<footer>
```

The **header** with **type** is mandatory. The **scope** of the header is optional. This repository has no predefined scopes. A custom scope can be used for clarity if desired.

Any line of the commit message cannot be longer 100 characters! This allows the message to be easier to read on GitHub as well as in various git tools.

The footer should contain a [closing reference to an issue](https://help.github.com/articles/closing-issues-via-commit-messages/) if any.

Example 1:

```text
feat: add Fuji release compose files
```

```text
fix(script): correct run script to use the right ports

Previously device services used wrong port numbers. This commit fixes the port numbers to use the latest port numbers.

Closes: #123, #245, #992
```

#### Revert

If the commit reverts a previous commit, it should begin with `revert:`, followed by the header of the reverted commit. In the body it should say: `This reverts commit <hash>.`, where the hash is the SHA of the commit being reverted.

#### Type

Must be one of the following:

- **feat**: New feature for the user, not a new feature for build script
- **fix**: Bug fix for the user, not a fix to a build script
- **docs**: Documentation only changes
- **style**: Formatting, missing semi colons, etc; no production code change
- **refactor**: Refactoring production code, eg. renaming a variable
- **chore**: Updating grunt tasks etc; no production code change
- **perf**: A code change that improves performance
- **test**: Adding missing tests, refactoring tests; no production code change
- **build**: Changes that affect the CI/CD pipeline or build system or external dependencies (example scopes: travis, jenkins, makefile)
- **ci**: Changes provided by DevOps for CI purposes.
- **revert**: Reverts a previous commit.

#### Scope

There are no predefined scopes for this repository. A custom scope can be provided for clarity.

#### Subject

The subject contains a succinct description of the change:

- use the imperative, present tense: "change" not "changed" nor "changes"
- don't capitalize the first letter
- no dot (.) at the end

#### Body

Just as in the **subject**, use the imperative, present tense: "change" not "changed" nor "changes". The body should include the motivation for the change and contrast this with previous behavior.

#### Footer

The footer should contain any information about **Breaking Changes** and is also the place to reference GitHub issues that this commit **Closes**.

**Breaking Changes** should start with the word `BREAKING CHANGE:` with a space or two newlines. The rest of the commit message is then used for this.

## Community Promotion

Besides coding, other types of contributions are a great way to get involved. Welcome to contribute to this project by
promoting it to the open source community and the world.

The promotion contributions include but not limit to:

- Integrate of eKuiper to your open source project
- Organize workshops or meetups about the project
- Answer questions about the project on issues, slack or maillist
- Write tutorials for how project can be used
- Offer to mentor another contributor

Thank you for taking the time to contribute!

# LF Edge eKuiper - エッジ軽量IoTデータ分析ソフトウェア

[![GitHub Release](https://img.shields.io/github/release/lf-edge/ekuiper?color=brightgreen)](https://github.com/lf-edge/ekuiper/releases)
[![Docker Pulls](https://img.shields.io/docker/pulls/emqx/kuiper)](https://hub.docker.com/r/lfedge/ekuiper)
[![codecov](https://codecov.io/gh/lf-edge/ekuiper/branch/master/graph/badge.svg?token=24E9Q3C0M0)](https://codecov.io/gh/lf-edge/ekuiper)
[![Go Report Card](https://goreportcard.com/badge/github.com/lf-edge/ekuiper)](https://goreportcard.com/report/github.com/lf-edge/ekuiper)
[![Slack](https://img.shields.io/badge/Slack-LF%20Edge-39AE85?logo=slack)](https://slack.lfedge.org/)
[![Twitter](https://img.shields.io/badge/Follow-EMQ-1DA1F2?logo=twitter)](https://twitter.com/EMQTech)
[![Community](https://img.shields.io/badge/Community-Kuiper-yellow?logo=github)](https://github.com/lf-edge/ekuiper/discussions)
[![YouTube](https://img.shields.io/badge/Subscribe-EMQ-FF0000?logo=youtube)](https://www.youtube.com/channel/UC5FjR77ErAxvZENEWzQaO5Q)

[English](README.md) | [简体中文](README-CN.md) | [日本語](README-JP.md)

## 概要

LF Edge eKuiperは、リソース制約のあるエッジデバイス上で実行される軽量なIoTデータ分析およびストリーム処理エンジンです。eKuiperの主な目標の1つは、エッジ側でストリーミングソフトウェアフレームワーク（[Apache Flink](https://flink.apache.org)に似ている）を提供することです。eKuiperの**ルールエンジン**により、ユーザーはSQLベースまたはグラフベース（Node-REDに似ている）のルールを提供して、数分以内にIoTエッジ分析アプリケーションを作成できます。

![arch](./docs/en_US/resources/arch.png)

**ユーザーシナリオ**

IIoTの生産ラインデータのリアルタイム処理、IoVの接続された車両がCANからのデータを分析、スマートエネルギーの風力タービンおよびスマート大容量エネルギー貯蔵のリアルタイム分析など、さまざまなIoTエッジユーザーシナリオで実行できます。

eKuiperのエッジでの処理により、システムの応答遅延を大幅に削減し、ネットワーク帯域幅とストレージコストを節約し、システムのセキュリティを向上させることができます。

## 特徴

- 軽量

  - コアサーバーパッケージは約4.5Mのみで、メモリフットプリントは約10MBです

- クロスプラットフォーム

  - CPUアーキテクチャ：X86 AMD * 32/64; ARM * 32/64; PPC
  - 人気のあるLinuxディストリビューション、OpenWrt Linux、MacOS、Docker
  - インダストリアルPC、ラズベリーパイ、インダストリアルゲートウェイ、ホームゲートウェイ、MECエッジクラウドサーバー

- データ分析サポート

  - データETLをサポート
  - データの順序付け、グループ化、集約、および異なるデータソース（データベースおよびファイルからのデータ）との結合
  - 数学、文字列、集約、ハッシュなどを含む60以上の関数
  - 4つの時間ウィンドウとカウントウィンドウ

- 高い拡張性

  GolangまたはPythonで`ソース`、`関数`、および`シンク`を拡張することをサポートします。

  - ソース：ユーザーが分析のためにより多くのデータソースを追加できるようにします。
  - シンク：ユーザーが分析結果をさまざまなカスタマイズされたシステムに送信できるようにします。
  - UDF関数：ユーザーがデータ分析のためのカスタマイズされた関数を追加できるようにします（たとえば、AI/ML関数の呼び出し）。

- 管理

  - 可視化された管理のための[無料のWebベースの管理ダッシュボード](https://hub.docker.com/r/emqx/ekuiper-manager)
  - CLI、REST API、および設定マップ（Kubernetes）を介したプラグイン、ストリーム、およびルールの管理
  - [KubeEdge](https://github.com/kubeedge/kubeedge)、[OpenYurt](https://openyurt.io/)、[K3s](https://github.com/rancher/k3s) [Baetyl](https://github.com/baetyl/baetyl)などのKubernetesフレームワークとの簡単な統合

- EMQX製品との統合

  [EMQX](https://www.emqx.io/)、[Neuron](https://neugates.io/)、[NanoMQ](https://nanomq.io/)などの製品とシームレスに統合し、IIoT、IoVなどのエンドツーエンドソリューションを提供します。

## クイックスタート

- [5分クイックスタート](docs/en_US/getting_started/quick_start_docker.md)
- [入門](docs/en_US/getting_started/getting_started.md)
- [EdgeXルールエンジンチュートリアル](docs/en_US/edgex/edgex_rule_engine_tutorial.md)

## コミュニティ

[Slack](https://slack.lfedge.org/)に参加し、[ekuiper](https://lfedge.slack.com/archives/C024F4P7KCK)または[ekuiper-user](https://lfedge.slack.com/archives/C024F4SMEMR)チャンネルに参加してください。

### 会議

コミュニティイベントの[カレンダー](https://lists.lfedge.org/g/ekuiper-tsc/calendar?calstart=2021-08-06)を購読してください。

毎週金曜日の午前10時30分（GMT+8）に開催されるコミュニティミーティング：
- [Zoomミーティングリンク](https://zoom.us/j/95097577087?pwd=azZaOXpXWmFoOXpqK293RFp0N1pydz09)
- [会議の議事録](https://wiki.lfedge.org/display/EKUIPER/Weekly+Development+Meeting)

### 貢献
ご協力ありがとうございます！詳細については、[CONTRIBUTING.md](./docs/en_US/CONTRIBUTING.md)を参照してください。

## パフォーマンステスト結果

### MQTTスループットテスト

- JMeter MQTTプラグインを使用して、IoTデータ（例：`{"temperature": 10, "humidity" : 90}`）を[EMQX Broker](https://www.emqx.io/)に送信します。温度と湿度の値は0〜100の間のランダムな整数です。
- eKuiperはEMQX Brokerからサブスクライブし、SQLでデータを分析します：`SELECT * FROM demo WHERE temperature > 50`
- 分析結果は、[ファイルシンクプラグイン](docs/en_US/guide/sinks/plugin/file.md)を使用してローカルファイルシステムに書き込まれます。

| デバイス                                          | 1秒あたりのメッセージ数 | CPU使用率       | メモリ使用量 |
|------------------------------------------------|------------------|-----------------|------------|
| Raspberry Pi 3B+                               | 12k              | sys+user: 70%   | 20M        |
| AWS t2.micro( 1 Core * 1 GB) <br />Ubuntu18.04 | 10k              | sys+user: 25%   | 20M        |

### EdgeXスループットテスト

- [Goアプリケーション](test/edgex/benchmark/pub.go)を作成して、ZeroMQメッセージバスにデータを送信します。データは以下の通りです。

  ```
  {
    "Device": "demo", "Created": 000, …
    "readings": 
    [
       {"Name": "Temperature", value: "30", "Created":123 …},
       {"Name": "Humidity", value: "20", "Created":456 …}
    ]
  }
  ```

- eKuiperはEdgeX ZeroMQメッセージバスからサブスクライブし、SQLでデータを分析します：`SELECT * FROM demo WHERE temperature > 50`。ルールによって90%のデータがフィルタリングされます。

- 分析結果は、[nop sink](docs/en_US/guide/sinks/builtin/nop.md)に送信され、すべての結果データが無視されます。

|                                                | 1秒あたりのメッセージ数 | CPU使用率       | メモリ使用量 |
|------------------------------------------------|------------------|-----------------|------------|
| AWS t2.micro( 1 Core * 1 GB) <br />Ubuntu18.04 | 11.4 k           | sys+user: 75%   | 32M        |

### 最大ルール数サポート

- 8000ルールで、合計800メッセージ/秒
- 設定
  - AWS 2コア * 4GBメモリ
  - Ubuntu
- リソース使用量
  - メモリ: 89% ~ 72%
  - CPU: 25%
  - ルールあたり400KB - 500KB
- ルール
  - ソース: MQTT
  - SQL: `SELECT temperature FROM source WHERE temperature > 20`（90%のデータがフィルタリングされます）
  - シンク: ログ

### 共有ソースインスタンスを持つ複数のルール

- 1つの共有MQTTストリームインスタンスを持つ300ルール。
  - MQTTソースで500メッセージ/秒
  - 合計で150,000メッセージ/秒の処理
- 設定：
  - AWS 2コア * 2GBメモリ
  - Ubuntu
- リソース使用量
  - メモリ: 95MB
  - CPU: 50%
- ルール
  - ソース: MQTT
  - SQL: `SELECT temperature FROM source WHERE temperature > 20`（90%のデータがフィルタリングされます）
  - シンク: 90% nop、10% MQTT

自分でベンチマークを実行するには、[この手順](./test/benchmark/multiple_rules/readme.md)を確認してください。

## ドキュメント

公式ウェブサイトで[最新のドキュメント](https://ekuiper.org/docs/en/latest/)をチェックしてください。

## ソースからビルド

#### 準備

- Goバージョン >= 1.22

#### コンパイル

+ バイナリ：

  - バイナリ：`$ make`

  - EdgeXをサポートするバイナリファイル：`$ make build_with_edgex`

  - コアランタイムのみを持つ最小バイナリファイル：`$ make build_core`

+ パッケージ：`$ make pkg`

  - パッケージ：`$ make pkg`

  - EdgeXをサポートするパッケージファイル：`$ make pkg_with_edgex`

+ Dockerイメージ：`$ make docker`

  > DockerイメージはデフォルトでEdgeXをサポートします

リリースアセットには、事前にビルドされたバイナリファイルが提供されています。事前にビルドされたバイナリファイルがないOSまたはアーキテクチャを使用している場合は、クロスコンパイルを使用して自分でビルドしてください。[このドキュメント](docs/en_US/operation/compile/cross-compile.md)を参照してください。

コンパイル中に、goビルドタグを選択して、必要な機能のみを含むカスタマイズされた製品をビルドし、バイナリサイズを削減することができます。ターゲットの展開環境にリソース制約がある場合、パッケージのサイズは特に重要です。詳細については、[機能](docs/en_US/operation/compile/features.md)を参照してください。

## オープンソースライセンス

[Apache 2.0](LICENSE)

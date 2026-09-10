# rabbitmq-topic

基于 [rabbitmq-amqp-go-client](https://github.com/rabbitmq/rabbitmq-amqp-go-client) 的普通队列（topic 交换机）消费封装，接口风格与同目录的 `rabbitmq-stream` 保持一致。

## 与 rabbitmq-stream 的区别

| | rabbitmq-stream | rabbitmq-topic |
|---|---|---|
| 消费模型 | 按 offset 重复读取同一份数据 | 队列消费，服务端在多个消费者间分发 |
| 多副本 | 按 pk 哈希分片，各取一部分 | 同样按 pk 哈希分片，`num` 为副本数 |
| 位点 | 客户端保存在 redis | 服务端跟踪，通过 ack/nack 回执 |
| 失败处理 | 上报错误并重建实例 | 按 `requeue` 重新入队，或丢弃进死信队列 |

## 配置

```yaml
rabbitmq:
  topic:
    host: "127.0.0.1"
    port: 5672
    username: "guest"
    password: "guest"
    vhost: "/"
    exchange: "hotgo"
    tls: false                  # 生产环境建议开启，否则凭据与消息体明文传输
    tlsSkipVerify: false        # 仅自签证书的测试环境使用
    consumers:
      - name: "oms"             # 对应 handles 的 key
        queue: "hotgo.oms"
        enable: true
        num: 3                  # 副本数量，按处理器的 Pk() 哈希分片
        requeue: false          # 处理失败是否重新入队
        quorum: false           # 声明为仲裁队列
        deadLetterExchange: ""  # 配合 requeue=false 使用
        receiveTimeout: "30s"
        bind:
          - "order.created"
          - "order.paid"
```

## 接入

处理器实现 `ProcessorInterface`（`Process`/`Clone`/`Name`/`Set`），入口参考 `example_test.go`，
放到 `internal/cmd/topic.go` 即可，写法与 `internal/cmd/stream.go` 相同。

## 分片

用于 binlog 这类需要按业务键串行处理的场景：同一个键的消息始终由同一个副本处理，
保证单键有序，不同键之间并发。哈希算法与 `rabbitmq-stream` 完全一致，同一个键在两边落到同一个分片。

- 分片键由处理器的 `Pk()` 返回，取值路径为 `data.<Pk>`
- 不属于当前副本的消息**直接确认**（`Accept`），既不重新入队也不丢弃 —— 重新入队会在副本间来回打转，
  丢弃会进死信队列
- `num > 1` 但 `Pk()` 返回空串时拒绝启动，不会等到收到第一条消息才报错

### 队列结构

分片的前提是**每个副本都能收到全量消息**，因此 `num > 1` 时会自动为每个副本声明独立队列：

```
num=1   ->  hotgo.oms
num=3   ->  hotgo.oms-000 / hotgo.oms-001 / hotgo.oms-002
```

三个队列绑定相同的路由键，交换机把每条消息复制到每个队列，各副本再按哈希挑走属于自己的那部分。
**代价是消息在 broker 侧存 N 份**，binlog 量大时要评估磁盘和网络。

### 副本数变更

`num` 变化会改变队列名，也会改变 `hash(key) % num` 的归属。启动时会自动探测可能残留的队列并打警告：

```
发现可能残留的队列 hotgo.oms（消息 12345 条，消费者 0 个），当前使用的是 [hotgo.oms-000 hotgo.oms-001]。
若已不再需要，请先解绑再删除，否则消息会一直堆积
```

只提示不自动删除 —— 删队列会丢数据，必须人工确认。

调整 `num` 的安全做法：先停掉全部消费者、等老队列排空、再改配置重启。
直接改会让同一个键在变更前后落到不同副本，破坏单键有序。

如果不接受这个复制成本，改用 RabbitMQ 的 `rabbitmq_consistent_hash_exchange` 插件：
交换机按哈希把每条消息只投递到 N 个队列中的一个，不复制，客户端也不再需要分片逻辑
（此时把 `num` 设为 1，起 N 个进程各消费一个队列即可）。

## 限制

- **一个 `name` 只能注册一个处理器。** 队列是竞争消费的，同一批队列上挂两个处理器会让它们各拿到一半消息，
  分片判断再把另一半确认掉，两边都残缺。需要多个处理器时配置成多个 `consumers`，各用各的 `queue`。
  （`rabbitmq-stream` 没有这个限制，因为 stream 是广播读。）

## 注意

- 队列与绑定在启动时声明一次，声明失败不会启动消费者，避免消费者空转收不到消息。
- 报文无法解析时直接丢弃，不重新入队，否则会形成死循环。
- 处理器 panic 会被捕获并按处理失败回执，不会带走消费协程。
- 消息体不会被打印到日志，只记录消息 ID 和长度，需要时由处理器自行按需输出。

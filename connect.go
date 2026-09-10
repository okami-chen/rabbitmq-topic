package rabbitmq_topic

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/gogf/gf/v2/util/grand"
	"github.com/okami-chen/rabbitmq-topic/simple"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

const (
	// defaultReceiveTimeout 单次拉取消息的超时时间。超时只是让消费循环有机会
	// 检查退出信号，不代表异常
	defaultReceiveTimeout = 30 * time.Second
	// reconnectMinInterval/reconnectMaxInterval 重连退避区间，单位秒
	reconnectMinInterval = 3
	reconnectMaxInterval = 15
)

// Connect 维持一个消费者实例，断开后自动重建，直到ctx被取消
func Connect(ctx context.Context, app *ApplicationContext, cfg *RabbitInfo, info *ConsumerInfo) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		instance := app.Clone()
		if err := NewInstance(ctx, cfg, info, instance); err != nil {
			g.Log().Warningf(ctx, "%s@%s-%d -> failed to create instance: %+v", info.Queue, app.Name, app.Index, err)
			if !sleepOrDone(ctx, reconnectInterval()) {
				return
			}
			continue
		}

		// 消费循环退出即代表这个实例已经不可用
		instance.consume(ctx, info)
		instance.close(ctx)

		select {
		case <-ctx.Done():
			return
		default:
		}

		if !sleepOrDone(ctx, reconnectInterval()) {
			return
		}
	}
}

// consume 持续拉取并处理消息，直到实例不可用或ctx被取消
func (c *ApplicationContext) consume(ctx context.Context, info *ConsumerInfo) {
	timeout := info.ReceiveTimeout
	if timeout <= 0 {
		timeout = defaultReceiveTimeout
	}

	for {
		select {
		case <-ctx.Done():
			return
		case err := <-c.Done:
			g.Log().Warningf(ctx, "%s@%d consumer stopped: %+v", c.Queue, c.Index, err)
			return
		default:
		}

		receiveCtx, cancel := context.WithTimeout(ctx, timeout)
		delivery, err := c.Consumer.Receive(receiveCtx)
		cancel()

		if err != nil {
			// 拉取超时属于正常空闲，继续下一轮；ctx被取消则退出
			if ctx.Err() != nil {
				return
			}
			if isTimeout(err) {
				continue
			}
			g.Log().Warningf(ctx, "%s@%d receive message failed: %+v", c.Queue, c.Index, err)
			return
		}

		if delivery == nil {
			continue
		}
		if !c.Process(ctx, delivery) {
			return
		}
	}
}

// close 释放实例持有的连接资源
func (c *ApplicationContext) close(ctx context.Context) {
	// 关闭时ctx可能已经取消，这里用独立的超时上下文，保证回收动作能正常发出
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if c.Consumer != nil {
		if err := c.Consumer.Close(closeCtx); err != nil {
			g.Log().Warningf(ctx, "failed to close consumer %s@%03d: %+v", c.Name, c.Index, err)
		}
		c.Consumer = nil
	}
	if c.Conn != nil {
		if err := c.Conn.Close(closeCtx); err != nil {
			g.Log().Warningf(ctx, "failed to close connection %s@%03d: %+v", c.Name, c.Index, err)
		}
		c.Conn = nil
	}
}

// NewInstance 建立连接并创建消费者
func NewInstance(ctx context.Context, config *RabbitInfo, info *ConsumerInfo, app *ApplicationContext) error {
	env := rmq.NewEnvironment(config.Address(), connOptions(config, app.Tag()))

	conn, err := env.NewConnection(ctx)
	if err != nil {
		// 错误信息里不要带上连接串，否则密码会跟着进日志
		return fmt.Errorf("failed to connect %s: %w", config.SafeAddress(), err)
	}

	// 消费的是副本自己的队列，不是配置里的原始队列名
	consumer, err := conn.NewConsumer(ctx, app.Queue, &rmq.ConsumerOptions{Id: app.Tag()})
	if err != nil {
		_ = conn.Close(ctx)
		return fmt.Errorf("failed to create consumer on %s: %w", app.Queue, err)
	}

	app.Conn = conn
	app.Consumer = consumer

	// 连接状态变化时上报，让消费循环尽快退出并重建
	stateChan := make(chan *rmq.StateChanged, 4)
	conn.NotifyStatusChange(stateChan)
	simple.SafeGo(ctx, func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case state, ok := <-stateChan:
				if !ok {
					return
				}
				if state == nil {
					continue
				}
				if _, closed := state.To.(*rmq.StateClosed); closed {
					app.notifyDone(fmt.Errorf("%s@%d connection closed", info.Queue, app.Index))
					return
				}
			}
		}
	})

	g.Log().Infof(ctx, "topic consumer ready, queue:%s, tag:%s", app.Queue, app.Tag())
	return nil
}

// NewBind 声明副本队列并绑定路由键，重复执行是幂等的。
// queues为本次实际要使用的全部队列，多副本时由ShardQueueName按序号生成
func NewBind(ctx context.Context, config *RabbitInfo, cfg *ConsumerInfo, queues []string) error {
	if len(queues) == 0 {
		return fmt.Errorf("queue name is empty")
	}

	env := rmq.NewEnvironment(config.Address(), connOptions(config, cfg.Name+"-declare"))

	conn, err := env.NewConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect %s: %w", config.SafeAddress(), err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			g.Log().Warningf(ctx, "failed to close declare connection: %+v", err)
		}
	}()

	management := conn.Management()

	for _, queue := range queues {
		if cfg.Quorum {
			_, err = management.DeclareQueue(ctx, &rmq.QuorumQueueSpecification{
				Name:               queue,
				DeadLetterExchange: cfg.DeadLetterExchange,
			})
		} else {
			_, err = management.DeclareQueue(ctx, &rmq.ClassicQueueSpecification{
				Name:               queue,
				DeadLetterExchange: cfg.DeadLetterExchange,
			})
		}
		if err != nil {
			return fmt.Errorf("failed to declare queue %s: %w", queue, err)
		}

		// 所有副本队列绑定相同的路由键，交换机据此把每条消息复制到每个队列
		for _, routeKey := range cfg.Bind {
			g.Log().Infof(ctx, "binding %s -> %s -> %s", config.Exchange, queue, routeKey)
			if _, err = management.Bind(ctx, &rmq.ExchangeToQueueBindingSpecification{
				SourceExchange:   config.Exchange,
				DestinationQueue: queue,
				BindingKey:       routeKey,
			}); err != nil {
				return fmt.Errorf("failed to bind %s -> %s with key %s: %w", config.Exchange, queue, routeKey, err)
			}
		}
	}

	warnOrphanQueues(ctx, management, cfg.Queue, queues)
	return nil
}

// orphanLookahead 检查残留队列时向后多探测的副本数量，用于发现副本数调小后遗留的队列
const orphanLookahead = 8

// orphanCandidates 列出需要探测的残留队列名：未分片的原始队列名，
// 以及从-000开始到当前副本数之后若干个的分片队列名，正在使用的会被排除
func orphanCandidates(queue string, active []string) []string {
	using := make(map[string]struct{}, len(active))
	for _, name := range active {
		using[name] = struct{}{}
	}

	candidates := make([]string, 0, len(active)+orphanLookahead+1)
	appendIfUnused := func(name string) {
		if _, ok := using[name]; ok {
			return
		}
		using[name] = struct{}{}
		candidates = append(candidates, name)
	}

	// 副本数从1调大时，未分片的老队列会被遗忘
	appendIfUnused(queue)
	// 副本数调小时，序号更大的分片队列会被遗忘。
	// ShardQueueName的第二个参数只用于判断是否分片，这里固定取2以生成分片形式的名称
	for i := 0; i < len(active)+orphanLookahead; i++ {
		appendIfUnused(ShardQueueName(queue, i, 2))
	}
	return candidates
}

// warnOrphanQueues 检查可能残留的队列。
//
// 副本数变化时队列名会跟着变：num从1调到3，老的<queue>还绑在交换机上继续收消息但没人消费；
// num从5调到3，<queue>-003和-004同样会被遗忘。这些队列会一直堆积直到撑爆磁盘，
// 这里只做提示，不自动删除——删队列会丢数据，必须由人确认
func warnOrphanQueues(ctx context.Context, management *rmq.AmqpManagement, queue string, active []string) {
	for _, candidate := range orphanCandidates(queue, active) {
		info, err := management.QueueInfo(ctx, candidate)
		if err != nil || info == nil {
			// 不存在是正常情况，其余错误不影响启动，只记调试日志
			if err != nil && !errorsIs(err, rmq.ErrDoesNotExist) {
				g.Log().Debugf(ctx, "check orphan queue %s failed: %+v", candidate, err)
			}
			continue
		}

		g.Log().Warningf(ctx,
			"发现可能残留的队列 %s（消息 %d 条，消费者 %d 个），当前使用的是 %v。"+
				"若已不再需要，请先解绑再删除，否则消息会一直堆积",
			candidate, info.MessageCount(), info.ConsumerCount(), active)
	}
}

// connOptions 连接选项，开启TLS时补上证书配置
func connOptions(config *RabbitInfo, containerId string) *rmq.AmqpConnOptions {
	options := &rmq.AmqpConnOptions{
		ContainerID: containerId,
		// 关闭客户端库自带的自动恢复：重连由Connect统一负责。
		// 两者同时开启时，连接断开会让Receive立刻返回错误、Connect随即销毁实例并重建，
		// 库内部的恢复过程会被中途打断，白白产生一轮无用的重连和告警日志
		RecoveryConfiguration: &rmq.RecoveryConfiguration{ActiveRecovery: false},
	}
	if config.TLS {
		options.TLSConfig = &tls.Config{
			ServerName:         config.Host,
			InsecureSkipVerify: config.TLSSkipVerify,
		}
	}
	return options
}

// reconnectInterval 带抖动的重连间隔，避免多个消费者同时重连打爆服务端
func reconnectInterval() time.Duration {
	return time.Duration(grand.N(reconnectMinInterval, reconnectMaxInterval)) * time.Second
}

// sleepOrDone 等待指定时间，ctx被取消时立即返回false
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// isTimeout 判断是否为拉取超时
func isTimeout(err error) bool {
	return errorsIs(err, context.DeadlineExceeded)
}

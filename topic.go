// Package rabbitmq_topic 基于AMQP 1.0的普通队列（topic交换机）消费封装。
//
// 与rabbitmq-stream的区别：
//   - stream按offset重复读取同一份数据，多副本靠哈希分片各取一部分；
//     普通队列由RabbitMQ在多个消费者之间自然分发，因此这里没有分片逻辑，Num就是并发消费者数量。
//   - stream的消费位点由客户端保存在redis里；普通队列由服务端跟踪，处理结果通过ack/nack回执。
package rabbitmq_topic

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/okami-chen/rabbitmq-topic/simple"
)

// RabbitMQConfig 一组消费者的完整配置
type RabbitMQConfig struct {
	*RabbitInfo
	Consumers []ConsumersConfig `yaml:"consumers" json:"consumers"`

	wg sync.WaitGroup // 跟踪Start启动的全部协程，供Wait等待退出
}

// Wait 等待Start启动的协程全部退出（需先取消传给Start的ctx），超时返回false
func (r *RabbitMQConfig) Wait(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// Start 按配置启动全部消费者，handles的key对应consumer.name
func (r *RabbitMQConfig) Start(ctx context.Context, handles map[string][]ProcessorInterface) {
	if r == nil || r.RabbitInfo == nil {
		g.Log().Error(ctx, "rabbitmq topic config is empty, nothing to start")
		return
	}

	for _, consumer := range r.Consumers {
		item := consumer
		if item.ConsumerInfo == nil {
			g.Log().Warning(ctx, "ignore topic consumer with empty config")
			continue
		}
		if !item.Enable {
			g.Log().Infof(ctx, "ignore topic %s@%s", item.Name, item.Queue)
			continue
		}

		objects, ok := handles[item.Name]
		if !ok || len(objects) == 0 {
			g.Log().Warningf(ctx, "ignore topic %s, no processor registered", item.Name)
			continue
		}

		// 队列是竞争消费的：同一个name注册多个处理器会让它们抢同一批队列，
		// 每个处理器只能拿到一部分消息，分片判断还会把不属于自己的那部分确认掉，
		// 结果是两边都残缺。需要多个处理器时请配置成多个consumers，各用各的队列
		if len(objects) > 1 {
			g.Log().Errorf(ctx,
				"topic %s registered %d processors, but a queue is consumed competitively; "+
					"配置成多个consumers（各自独立queue）后重试，当前consumer未启动",
				item.Name, len(objects))
			continue
		}

		instance := objects[0]
		r.wg.Add(1)
		simple.SafeGo(ctx, func(ctx context.Context) {
			defer r.wg.Done()
			g.Log().Infof(ctx, "start topic %s@%s", item.Name, instance.Name())
			item.run(ctx, r.RabbitInfo, instance, &r.wg)
		})
	}
}

// RabbitInfo 连接信息
type RabbitInfo struct {
	Host     string `yaml:"host" json:"host"`
	Port     int    `yaml:"port" json:"port"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	VHost    string `yaml:"vhost" json:"vhost"`
	Exchange string `yaml:"exchange" json:"exchange"`
	// TLS 开启后使用amqps连接，生产环境建议开启，否则凭据与消息体均为明文传输
	TLS bool `yaml:"tls" json:"tls"`
	// TLSSkipVerify 跳过服务端证书校验，仅用于自签证书的测试环境
	TLSSkipVerify bool `yaml:"tlsSkipVerify" json:"tlsSkipVerify"`
}

// Address 连接地址。用户名和密码交给url编码，避免密码中的@、/、:等字符把URI解析歪
func (r *RabbitInfo) Address() string {
	return r.buildURL(false)
}

// SafeAddress 隐去密码的连接地址，用于日志输出
func (r *RabbitInfo) SafeAddress() string {
	return r.buildURL(true)
}

func (r *RabbitInfo) buildURL(mask bool) string {
	scheme := "amqp"
	if r.TLS {
		scheme = "amqps"
	}

	host := r.Host
	if r.Port > 0 {
		host = net.JoinHostPort(r.Host, strconv.Itoa(r.Port))
	}

	u := &url.URL{Scheme: scheme, Host: host, Path: "/" + strings.TrimPrefix(strings.TrimSpace(r.VHost), "/")}
	if r.Username != "" {
		if mask {
			u.User = url.UserPassword(r.Username, "******")
		} else {
			u.User = url.UserPassword(r.Username, r.Password)
		}
	}
	return u.String()
}

func (r *RabbitInfo) Clone() RabbitInfo {
	return *r
}

// ConsumersConfig 单个消费者配置
type ConsumersConfig struct {
	*ConsumerInfo
}

// Run 按Num启动多个消费者协程，它们共同消费同一个队列，由RabbitMQ负责分发
func (r *ConsumersConfig) Run(ctx context.Context, info *RabbitInfo, handle ProcessorInterface) {
	r.run(ctx, info, handle, nil)
}

// run 在wg不为空时登记启动的消费者协程。调用方需已在wg中占位，保证Add发生在Wait归零之前
func (r *ConsumersConfig) run(ctx context.Context, info *RabbitInfo, handle ProcessorInterface, wg *sync.WaitGroup) {
	num := r.Num
	if num <= 0 {
		num = 1
	}

	// 开启分片却没有分片键时，所有消息都会落到同一个副本，属于配置错误，直接拒绝启动。
	// 不在收到第一条消息时才panic，避免把问题拖到运行期
	if num > 1 && handle.Pk() == "" {
		g.Log().Errorf(ctx, "topic %s@%s num=%d but processor %s has no shard key, consumers not started",
			r.Name, r.Queue, num, handle.Name())
		return
	}

	// 每个副本使用独立队列，交换机会把每条消息复制到所有队列，
	// 副本之间才能各自看到全量数据再按分片挑走属于自己的那部分。
	// 共用一个队列时消息只会投给其中一个副本，跳过分片的确认动作就等于丢消息
	queues := make([]string, 0, num)
	for i := 0; i < num; i++ {
		queues = append(queues, ShardQueueName(r.Queue, i, num))
	}
	// 声明失败多为RabbitMQ暂不可用，按重连间隔重试，直到成功或ctx被取消
	for {
		err := NewBind(ctx, info, r.ConsumerInfo, queues)
		if err == nil {
			break
		}
		g.Log().Warningf(ctx, "declare topic %s@%s failed, retrying: %+v", r.Name, r.Queue, err)
		if !sleepOrDone(ctx, reconnectInterval()) {
			return
		}
	}

	for i := 0; i < num; i++ {
		index := i
		instance := handle.Clone()
		instance.Set("key", fmt.Sprintf("%s:%s:%s:%03d", info.Exchange, r.Name, instance.Name(), index))
		instance.Set("index", index)

		if wg != nil {
			wg.Add(1)
		}
		simple.SafeGo(ctx, func(ctx context.Context) {
			if wg != nil {
				defer wg.Done()
			}
			g.Log().Infof(ctx, "start -> %s@%s-%03d -> %03d", r.Name, instance.Name(), index, num)
			app := &ApplicationContext{
				AppName:   info.Exchange,
				Name:      r.Name,
				Queue:     ShardQueueName(r.Queue, index, num),
				Pk:        handle.Pk(),
				Index:     index,
				Total:     num,
				Requeue:   r.Requeue,
				Done:      make(chan error, 1),
				Ctx:       ctx,
				Processor: instance,
			}
			Connect(ctx, app, info, r.ConsumerInfo)
		})
	}
}

// ConsumerInfo 队列与绑定信息
type ConsumerInfo struct {
	Name   string   `yaml:"name" json:"name"`     // 消费者名称，对应handles的key
	Queue  string   `yaml:"queue" json:"queue"`   // 队列名称
	Enable bool     `yaml:"enable" json:"enable"` // 是否启用
	Key    string   `yaml:"key" json:"key"`
	Num    int      `yaml:"num" json:"num"`   // 并发消费者数量
	Bind   []string `yaml:"bind" json:"bind"` // 需要绑定的路由键
	// Requeue 处理失败时是否重新入队。为false时消息被丢弃，
	// 队列配置了死信交换机的话会进入死信队列，否则直接丢失
	Requeue bool `yaml:"requeue" json:"requeue"`
	// Quorum 声明为仲裁队列，默认为经典队列
	Quorum bool `yaml:"quorum" json:"quorum"`
	// DeadLetterExchange 死信交换机，配合Requeue=false使用
	DeadLetterExchange string `yaml:"deadLetterExchange" json:"deadLetterExchange"`
	// ReceiveTimeout 单次拉取消息的超时时间，为空时使用默认值
	ReceiveTimeout time.Duration `yaml:"receiveTimeout" json:"receiveTimeout"`
}

// ShardQueueName 副本队列名称。单副本时保持原队列名不变，
// 多副本时按序号拆分，保证每个副本都能收到全量消息
func ShardQueueName(queue string, index, total int) string {
	if total <= 1 {
		return queue
	}
	return fmt.Sprintf("%s-%03d", queue, index)
}

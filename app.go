package rabbitmq_topic

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/Azure/go-amqp"
	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/okami-chen/rabbitmq-topic/consts"
	"github.com/pkg/errors"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
)

// ProcessorInterface 消息处理器
type ProcessorInterface interface {
	// Process 处理一条消息，返回error表示处理失败
	Process(ctx context.Context, j *gjson.Json) error
	// Clone 复制一个处理器实例，每个消费者协程持有独立实例
	Clone() ProcessorInterface
	// Pk 分片键在消息中的字段名，取值路径为 data.<Pk>。
	// 单消费者时可以返回空串
	Pk() string
	// Name 处理器名称
	Name() string
	// Set 设置运行期参数，目前会注入key和index
	Set(key string, value interface{})
}

// ApplicationContext 单个消费者协程的运行上下文
type ApplicationContext struct {
	AppName   string
	Name      string
	Queue     string
	Pk        string
	Index     int
	Total     int
	Requeue   bool
	Done      chan error
	Ctx       context.Context
	Consumer  *rmq.Consumer
	Conn      *rmq.AmqpConnection
	Processor ProcessorInterface
}

// Process 处理一条投递。返回值表示是否需要继续消费，
// 返回false时由调用方结束当前实例并触发重连
func (c *ApplicationContext) Process(ctx context.Context, delivery rmq.IDeliveryContext) (next bool) {
	defer func() {
		if r := recover(); r != nil {
			// 处理器panic不能带走整个消费协程，按处理失败走回执后继续消费。
			// 这里必须用具名返回值，否则panic恢复后会返回零值false，导致实例被误判为不可用
			g.Log().Errorf(ctx, "%s@%d process panic:%+v, stack:%+v", c.Queue, c.Index, r, string(debug.Stack()))
			c.settle(ctx, delivery, fmt.Errorf("process panic: %+v", r))
			next = true
		}
	}()

	message := delivery.Message()
	if message == nil {
		c.settle(ctx, delivery, errors.New("消息内容为空"))
		return true
	}

	j, err := gjson.DecodeToJson(message.GetData())
	if err != nil {
		// 报文本身无法解析，重新入队也不会变好，直接丢弃避免死循环
		g.Log().Errorf(ctx, "%s@%d decode message failed:%+v", c.Queue, c.Index, err)
		c.discard(ctx, delivery, err)
		return true
	}

	// 分片：不属于当前副本的消息直接确认，既不重新入队也不丢弃。
	// 要求每个副本都能收到全量消息（各自独立队列绑定同样的路由键），
	// 若多个副本共用一个队列，消息只会投递给其中一个，确认掉就等于丢失
	if c.Total > 1 {
		pkVal := j.Get("data." + c.Pk).String()
		if HashShardByCount(pkVal, c.Total) != c.Index {
			if err = delivery.Accept(ctx); err != nil {
				g.Log().Warningf(ctx, "%s@%d accept skipped message failed:%+v", c.Queue, c.Index, err)
				c.notifyDone(errors.Wrap(err, "回执失败"))
				return false
			}
			return true
		}
	}

	processCtx := context.WithValue(ctx, consts.ContextKeyQueue, c.Queue)
	processCtx = context.WithValue(processCtx, consts.ContextKeyMessageId, messageId(message))

	if err = c.Processor.Process(processCtx, j); err != nil {
		g.Log().Errorf(ctx, "%s@%d process failed:%+v", c.Queue, c.Index, err)
		c.settle(ctx, delivery, err)
		return true
	}

	if err = delivery.Accept(ctx); err != nil {
		// 回执失败说明连接已经不可用，交给外层重连
		g.Log().Warningf(ctx, "%s@%d accept message failed:%+v", c.Queue, c.Index, err)
		c.notifyDone(errors.Wrap(err, "回执失败"))
		return false
	}

	// 只记录必要信息。消息体可能包含敏感数据，需要时通过处理器自行按需打印
	g.Log().Debugf(ctx, "%s@%d processed message, id:%s, size:%d",
		c.Queue, c.Index, messageId(message), len(message.GetData()))
	return true
}

// settle 处理失败时的回执：按配置重新入队或丢弃
func (c *ApplicationContext) settle(ctx context.Context, delivery rmq.IDeliveryContext, cause error) {
	if c.Requeue {
		if err := delivery.Requeue(ctx); err != nil {
			g.Log().Warningf(ctx, "%s@%d requeue message failed:%+v", c.Queue, c.Index, err)
		}
		return
	}
	c.discard(ctx, delivery, cause)
}

// discard 丢弃消息，队列配置了死信交换机时会进入死信队列
func (c *ApplicationContext) discard(ctx context.Context, delivery rmq.IDeliveryContext, cause error) {
	if err := delivery.Discard(ctx, nil); err != nil {
		g.Log().Warningf(ctx, "%s@%d discard message failed:%+v, cause:%+v", c.Queue, c.Index, err, cause)
	}
}

// notifyDone 上报实例异常，通知Connect重建实例。
// Done只会被Connect消费一次，多余的错误直接丢弃，
// 否则写满缓冲的一方会永久阻塞并泄漏协程
func (c *ApplicationContext) notifyDone(err error) {
	if err == nil || c.Done == nil {
		return
	}
	select {
	case c.Done <- err:
	default:
	}
}

// Clone 复制一份运行上下文，用于重连时重建实例
func (c *ApplicationContext) Clone() *ApplicationContext {
	return &ApplicationContext{
		AppName:   c.AppName,
		Name:      c.Name,
		Queue:     c.Queue,
		Pk:        c.Pk,
		Index:     c.Index,
		Total:     c.Total,
		Requeue:   c.Requeue,
		Done:      make(chan error, 1),
		Ctx:       c.Ctx,
		Processor: c.Processor.Clone(),
	}
}

// Tag 消费者标识，便于在RabbitMQ管理端识别
func (c *ApplicationContext) Tag() string {
	return fmt.Sprintf("%s-%s-%s-%03d", c.AppName, c.Name, c.Processor.Name(), c.Index)
}

// messageId 取消息ID，没有则返回空串
func messageId(message *amqp.Message) string {
	if message == nil || message.Properties == nil || message.Properties.MessageID == nil {
		return ""
	}
	return fmt.Sprintf("%v", message.Properties.MessageID)
}

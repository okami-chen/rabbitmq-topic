package rabbitmq_topic_test

import (
	"context"

	"github.com/gogf/gf/v2/encoding/gjson"
	"github.com/gogf/gf/v2/frame/g"
	rmq "github.com/okami-chen/rabbitmq-topic"
)

// OrderProcessor 一个最小的处理器实现
type OrderProcessor struct {
	key   string
	index int
}

func (p *OrderProcessor) Process(ctx context.Context, j *gjson.Json) error {
	g.Log().Infof(ctx, "handle order: %s", j.Get("data.order_no").String())
	return nil
}

// Clone 每个消费者协程持有独立实例
func (p *OrderProcessor) Clone() rmq.ProcessorInterface {
	return &OrderProcessor{}
}

// Pk 分片键，取值路径为 data.order_no
func (p *OrderProcessor) Pk() string { return "order_no" }

func (p *OrderProcessor) Name() string { return "order" }

func (p *OrderProcessor) Set(key string, value interface{}) {
	switch key {
	case "key":
		p.key = value.(string)
	case "index":
		p.index = value.(int)
	}
}

// Example 展示cmd入口的接线方式，对应 hotgo topic -group=topic
func Example() {
	ctx := context.Background()

	var consumer rmq.RabbitMQConfig
	if err := g.Config().MustGet(ctx, "rabbitmq.topic").Struct(&consumer); err != nil {
		g.Log().Error(ctx, err)
		return
	}

	handles := make(map[string][]rmq.ProcessorInterface)
	handles["oms"] = append(handles["oms"], &OrderProcessor{})

	consumer.Start(ctx, handles)
}

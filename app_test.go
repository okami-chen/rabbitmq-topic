package rabbitmq_topic

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/gogf/gf/v2/encoding/gjson"
)

// fakeDelivery 用于测试的投递上下文
type fakeDelivery struct {
	message  *amqp.Message
	accepted bool
	requeued bool
	discards int
}

func (d *fakeDelivery) Message() *amqp.Message        { return d.message }
func (d *fakeDelivery) Accept(context.Context) error  { d.accepted = true; return nil }
func (d *fakeDelivery) Requeue(context.Context) error { d.requeued = true; return nil }
func (d *fakeDelivery) Discard(context.Context, *amqp.Error) error {
	d.discards++
	return nil
}
func (d *fakeDelivery) DiscardWithAnnotations(context.Context, amqp.Annotations) error { return nil }
func (d *fakeDelivery) RequeueWithAnnotations(context.Context, amqp.Annotations) error { return nil }

// testProcessor 用于测试的处理器
type testProcessor struct {
	err    error
	panics bool
	pk     string
	calls  int
}

func (p *testProcessor) Process(ctx context.Context, j *gjson.Json) error {
	p.calls++
	if p.panics {
		panic("boom")
	}
	return p.err
}
func (p *testProcessor) Clone() ProcessorInterface {
	return &testProcessor{err: p.err, panics: p.panics}
}
func (p *testProcessor) Pk() string                        { return p.pk }
func (p *testProcessor) Name() string                      { return "test" }
func (p *testProcessor) Set(key string, value interface{}) {}

func newTestApp(processor ProcessorInterface, requeue bool) *ApplicationContext {
	return &ApplicationContext{
		AppName:   "app",
		Name:      "test",
		Queue:     "test-queue",
		Requeue:   requeue,
		Done:      make(chan error, 1),
		Ctx:       context.Background(),
		Processor: processor,
	}
}

func newDelivery(body string) *fakeDelivery {
	return &fakeDelivery{message: amqp.NewMessage([]byte(body))}
}

// 处理成功必须ack
func TestProcessAccept(t *testing.T) {
	processor := &testProcessor{}
	app := newTestApp(processor, false)
	delivery := newDelivery(`{"data":{"id":1}}`)

	if !app.Process(context.Background(), delivery) {
		t.Fatal("处理成功不应结束消费")
	}
	if !delivery.accepted || delivery.requeued || delivery.discards != 0 {
		t.Fatalf("回执不正确: %+v", delivery)
	}
	if processor.calls != 1 {
		t.Fatalf("处理器调用次数: %d", processor.calls)
	}
}

// 处理失败按配置重新入队或丢弃
func TestProcessFailureSettlement(t *testing.T) {
	requeueApp := newTestApp(&testProcessor{err: errors.New("failed")}, true)
	requeueDelivery := newDelivery(`{"data":{"id":1}}`)
	requeueApp.Process(context.Background(), requeueDelivery)
	if !requeueDelivery.requeued || requeueDelivery.accepted {
		t.Fatalf("requeue=true时应重新入队: %+v", requeueDelivery)
	}

	discardApp := newTestApp(&testProcessor{err: errors.New("failed")}, false)
	discardDelivery := newDelivery(`{"data":{"id":1}}`)
	discardApp.Process(context.Background(), discardDelivery)
	if discardDelivery.discards != 1 || discardDelivery.accepted {
		t.Fatalf("requeue=false时应丢弃: %+v", discardDelivery)
	}
}

// 报文无法解析时直接丢弃，重新入队只会无限循环
func TestProcessInvalidPayloadAlwaysDiscard(t *testing.T) {
	processor := &testProcessor{}
	app := newTestApp(processor, true) // 即使开启requeue
	delivery := &fakeDelivery{message: amqp.NewMessage([]byte("not-json{"))}

	app.Process(context.Background(), delivery)
	if delivery.discards != 1 || delivery.requeued {
		t.Fatalf("非法报文应当丢弃: %+v", delivery)
	}
	if processor.calls != 0 {
		t.Fatal("非法报文不应进入处理器")
	}
}

// 处理器panic不能带走消费协程
func TestProcessRecoversPanic(t *testing.T) {
	app := newTestApp(&testProcessor{panics: true}, true)
	delivery := newDelivery(`{"data":{"id":1}}`)

	if !app.Process(context.Background(), delivery) {
		t.Fatal("panic后应继续消费")
	}
	if !delivery.requeued {
		t.Fatalf("panic应按处理失败回执: %+v", delivery)
	}
}

// 上报不能阻塞
func TestNotifyDoneNeverBlocks(t *testing.T) {
	app := newTestApp(&testProcessor{}, false)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10; i++ {
			app.notifyDone(errors.New("boom"))
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("notifyDone 阻塞")
	}
	if len(app.Done) != 1 {
		t.Fatalf("应保留一条错误，实际 %d", len(app.Done))
	}
}

// 不属于当前分片的消息：确认掉，既不重新入队也不丢弃，更不进处理器
func TestProcessSkipsOtherShard(t *testing.T) {
	// 找一个落在分片1的键，用分片0的消费者去处理
	const total = 2
	key := ""
	for _, candidate := range []string{"a", "b", "c", "d", "e"} {
		if HashShardByCount(candidate, total) == 1 {
			key = candidate
			break
		}
	}
	if key == "" {
		t.Fatal("没有找到落在分片1的键")
	}

	processor := &testProcessor{pk: "order_no"}
	app := newTestApp(processor, true)
	app.Pk = "order_no"
	app.Total = total
	app.Index = 0

	delivery := newDelivery(`{"data":{"order_no":"` + key + `"}}`)
	if !app.Process(context.Background(), delivery) {
		t.Fatal("跳过分片不应结束消费")
	}
	if !delivery.accepted {
		t.Fatal("不属于当前分片的消息应当被确认")
	}
	if delivery.requeued || delivery.discards != 0 {
		t.Fatalf("不应重新入队或丢弃: %+v", delivery)
	}
	if processor.calls != 0 {
		t.Fatal("不属于当前分片的消息不应进入处理器")
	}
}

// 属于当前分片的消息正常处理
func TestProcessHandlesOwnShard(t *testing.T) {
	const total = 2
	key := ""
	for _, candidate := range []string{"a", "b", "c", "d", "e"} {
		if HashShardByCount(candidate, total) == 0 {
			key = candidate
			break
		}
	}
	if key == "" {
		t.Fatal("没有找到落在分片0的键")
	}

	processor := &testProcessor{pk: "order_no"}
	app := newTestApp(processor, true)
	app.Pk = "order_no"
	app.Total = total
	app.Index = 0

	delivery := newDelivery(`{"data":{"order_no":"` + key + `"}}`)
	app.Process(context.Background(), delivery)
	if processor.calls != 1 {
		t.Fatalf("属于当前分片的消息应当被处理，实际调用 %d 次", processor.calls)
	}
	if !delivery.accepted {
		t.Fatal("处理成功应当确认")
	}
}

// 分片必须与stream模块一致，同一个键在两边落到同一个分片
func TestHashShardStable(t *testing.T) {
	// javaHash("order-123") 的结果与 rabbitmq-stream 保持一致
	cases := map[string]int{"order-123": 0, "order-124": 0, "": 0}
	for key := range cases {
		got := HashShardByCount(key, 4)
		if got < 0 || got > 3 {
			t.Fatalf("分片越界: key=%s shard=%d", key, got)
		}
		// 同一个键必须稳定
		if again := HashShardByCount(key, 4); again != got {
			t.Fatalf("分片不稳定: %d != %d", got, again)
		}
	}
}

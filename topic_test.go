package rabbitmq_topic

import (
	"context"
	"strings"
	"testing"
	"time"
)

// 连接串必须对用户名密码做url编码，否则特殊字符会把URI解析歪
func TestAddressEncodesCredentials(t *testing.T) {
	info := &RabbitInfo{
		Host:     "127.0.0.1",
		Port:     5672,
		Username: "user@name",
		Password: "p@ss:w/rd",
		VHost:    "prod",
	}

	address := info.Address()
	if strings.Contains(address, "p@ss:w/rd") {
		t.Fatalf("密码没有被编码: %s", address)
	}
	if !strings.HasPrefix(address, "amqp://") || !strings.HasSuffix(address, "/prod") {
		t.Fatalf("连接串格式不正确: %s", address)
	}
	// 编码后主机名必须仍然可以被正确解析出来
	if !strings.Contains(address, "@127.0.0.1:5672/") {
		t.Fatalf("主机解析错误: %s", address)
	}
}

// 开启TLS时使用amqps
func TestAddressTLSScheme(t *testing.T) {
	info := &RabbitInfo{Host: "mq.test", Port: 5671, Username: "u", Password: "p", TLS: true}
	if !strings.HasPrefix(info.Address(), "amqps://") {
		t.Fatalf("TLS应使用amqps: %s", info.Address())
	}
}

// 日志用的地址不能带出密码
func TestSafeAddressMasksPassword(t *testing.T) {
	info := &RabbitInfo{Host: "mq.test", Port: 5672, Username: "u", Password: "super-secret"}
	safe := info.SafeAddress()
	if strings.Contains(safe, "super-secret") {
		t.Fatalf("密码泄漏到日志地址: %s", safe)
	}
	if !strings.Contains(safe, "u:") {
		t.Fatalf("用户名应保留便于排查: %s", safe)
	}
}

// vhost为空时落到默认的根vhost
func TestAddressDefaultVHost(t *testing.T) {
	info := &RabbitInfo{Host: "mq.test", Port: 5672}
	if got := info.Address(); !strings.HasSuffix(got, "/") {
		t.Fatalf("默认vhost不正确: %s", got)
	}
}

// 单副本沿用原队列名，多副本按序号拆分成独立队列
func TestShardQueueName(t *testing.T) {
	if got := ShardQueueName("hotgo.oms", 0, 1); got != "hotgo.oms" {
		t.Fatalf("单副本不应改名: %s", got)
	}

	names := map[string]bool{}
	const total = 3
	for i := 0; i < total; i++ {
		name := ShardQueueName("hotgo.oms", i, total)
		if name == "hotgo.oms" {
			t.Fatalf("多副本必须使用独立队列: %s", name)
		}
		if names[name] {
			t.Fatalf("副本队列重名: %s", name)
		}
		names[name] = true
	}
	if len(names) != total {
		t.Fatalf("应当有 %d 个不同的队列，实际 %d 个", total, len(names))
	}
	if got := ShardQueueName("hotgo.oms", 1, 3); got != "hotgo.oms-001" {
		t.Fatalf("队列命名不符合预期: %s", got)
	}
}

// 残留队列探测：副本数调大调小两种情况都要覆盖，正在使用的不能被列进去
func TestOrphanCandidates(t *testing.T) {
	contains := func(list []string, name string) bool {
		for _, item := range list {
			if item == name {
				return true
			}
		}
		return false
	}

	// 单副本：使用未分片的队列名，-000 起的分片队列都是可能的残留
	single := orphanCandidates("hotgo.oms", []string{"hotgo.oms"})
	if contains(single, "hotgo.oms") {
		t.Fatal("正在使用的队列不应被当成残留")
	}
	if !contains(single, "hotgo.oms-000") {
		t.Fatalf("副本数缩回1时应当探测 -000: %v", single)
	}

	// 三副本：未分片的老队列是残留，-000..002 正在使用，-003 起是缩容残留
	multi := orphanCandidates("hotgo.oms", []string{"hotgo.oms-000", "hotgo.oms-001", "hotgo.oms-002"})
	if !contains(multi, "hotgo.oms") {
		t.Fatalf("副本数调大后未分片的老队列应当被探测: %v", multi)
	}
	for _, using := range []string{"hotgo.oms-000", "hotgo.oms-001", "hotgo.oms-002"} {
		if contains(multi, using) {
			t.Fatalf("正在使用的队列 %s 不应被当成残留", using)
		}
	}
	if !contains(multi, "hotgo.oms-003") {
		t.Fatalf("应当向后探测缩容残留: %v", multi)
	}

	// 候选名不能重复
	seen := map[string]bool{}
	for _, name := range multi {
		if seen[name] {
			t.Fatalf("候选名重复: %s", name)
		}
		seen[name] = true
	}
}

// 同名注册多个处理器时必须拒绝启动，否则它们会竞争同一批队列各拿一半消息
func TestStartRejectsMultipleProcessors(t *testing.T) {
	config := &RabbitMQConfig{
		RabbitInfo: &RabbitInfo{Host: "127.0.0.1", Port: 5672, Exchange: "test"},
		Consumers: []ConsumersConfig{
			{ConsumerInfo: &ConsumerInfo{Name: "oms", Queue: "test.oms", Enable: true, Num: 1}},
		},
	}

	// 两个处理器：不应发起任何连接，直接跳过
	handles := map[string][]ProcessorInterface{
		"oms": {&testProcessor{}, &testProcessor{}},
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		config.Start(context.Background(), handles)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Start 不应阻塞，多处理器时应直接跳过")
	}
}

// 配置为空时不应panic
func TestStartWithEmptyConfig(t *testing.T) {
	(&RabbitMQConfig{}).Start(context.Background(), nil)

	config := &RabbitMQConfig{
		RabbitInfo: &RabbitInfo{Host: "127.0.0.1"},
		Consumers:  []ConsumersConfig{{}},
	}
	config.Start(context.Background(), nil)
}

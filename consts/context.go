// Package consts
package consts

type CtxKey string

// ContextKey 上下文
const (
	ContextKeyQueue      CtxKey = "queue"      // 消息来源队列
	ContextKeyMessageId  CtxKey = "messageId"  // 消息ID
	ContextKeyDeliveryNo CtxKey = "deliveryNo" // 本次投递序号，用于日志串联
)

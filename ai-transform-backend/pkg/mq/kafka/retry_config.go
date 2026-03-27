package kafka

import (
	"time"

	"ai-transform-backend/pkg/constants"
)

// DeadLetterHandler 死信队列处理函数
// originalValue: 原始消息内容
// lastError: 最后的错误信息
// retryCount: 重试次数
type DeadLetterHandler func(originalValue []byte, lastError string, retryCount int)

// RetryConfig 重试配置
type RetryConfig struct {
	// 本地重试配置
	LocalRetryEnabled bool          // 本地重试开关
	LocalMaxRetries   int           // 本地最大重试次数 (默认 3)
	LocalRetryDelay   time.Duration // 本地重试初始延迟 (默认 2s)

	// 重试队列配置
	RetryQueueEnabled    bool            // 重试队列开关
	RetryQueueMaxRetries int             // 队列最大重试次数 (默认 5)
	RetryQueueIntervals  []time.Duration // 重试间隔

	// 死信队列配置
	DeadLetterEnabled bool              // 死信队列开关
	OnDeadLetter      DeadLetterHandler // 进入死信队列时的回调函数

	// 生产者 key
	ProducerKey string // 用于发送重试消息的生产者 key
}

// DefaultRetryConfig 返回默认重试配置
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		LocalRetryEnabled:    true,
		LocalMaxRetries:      3,
		LocalRetryDelay:      2 * time.Second,
		RetryQueueEnabled:    true,
		RetryQueueMaxRetries: 5,
		RetryQueueIntervals: []time.Duration{
			1 * time.Minute,
			5 * time.Minute,
			30 * time.Minute,
			1 * time.Hour,
			6 * time.Hour,
		},
		DeadLetterEnabled: true,
		ProducerKey:       Producer,
	}
}

// GetRetryTopic 获取重试 topic
func GetRetryTopic(originalTopic string) string {
	return originalTopic + constants.KAFKA_TOPIC_RETRY_SUFFIX
}

// GetDeadLetterTopic 获取死信 topic
func GetDeadLetterTopic(originalTopic string) string {
	return originalTopic + constants.KAFKA_TOPIC_DEAD_SUFFIX
}

// IsRetryTopic 判断是否为重试 topic
func IsRetryTopic(topic string) bool {
	return len(topic) > len(constants.KAFKA_TOPIC_RETRY_SUFFIX) &&
		topic[len(topic)-len(constants.KAFKA_TOPIC_RETRY_SUFFIX):] == constants.KAFKA_TOPIC_RETRY_SUFFIX
}

// GetOriginalTopicFromRetry 从重试 topic 获取原始 topic
func GetOriginalTopicFromRetry(retryTopic string) string {
	if IsRetryTopic(retryTopic) {
		return retryTopic[:len(retryTopic)-len(constants.KAFKA_TOPIC_RETRY_SUFFIX)]
	}
	return retryTopic
}

// GetRetryInterval 获取指定重试次数的间隔时间
func (c *RetryConfig) GetRetryInterval(retryCount int) time.Duration {
	if retryCount < 0 {
		return 0
	}
	// if c.RetryQueueIntervals == nil || len(c.RetryQueueIntervals) == 0 {
	if len(c.RetryQueueIntervals) == 0 {
		// 默认间隔
		defaultIntervals := []time.Duration{
			1 * time.Minute,
			5 * time.Minute,
			30 * time.Minute,
			1 * time.Hour,
			6 * time.Hour,
		}
		if retryCount >= len(defaultIntervals) {
			return defaultIntervals[len(defaultIntervals)-1]
		}
		return defaultIntervals[retryCount]
	}
	if retryCount >= len(c.RetryQueueIntervals) {
		return c.RetryQueueIntervals[len(c.RetryQueueIntervals)-1]
	}
	return c.RetryQueueIntervals[retryCount]
}

// ShouldRetryToLocal 判断是否应该进行本地重试
func (c *RetryConfig) ShouldRetryToLocal(localRetryCount int) bool {
	return c.LocalRetryEnabled && localRetryCount < c.LocalMaxRetries
}

// ShouldSendToRetryQueue 判断是否应该发送到重试队列
func (c *RetryConfig) ShouldSendToRetryQueue(queueRetryCount int) bool {
	return c.RetryQueueEnabled && queueRetryCount < c.RetryQueueMaxRetries
}

// ShouldSendToDeadLetter 判断是否应该发送到死信队列
func (c *RetryConfig) ShouldSendToDeadLetter(queueRetryCount int) bool {
	return c.DeadLetterEnabled && queueRetryCount >= c.RetryQueueMaxRetries
}

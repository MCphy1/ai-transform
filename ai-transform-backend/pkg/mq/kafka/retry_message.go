package kafka

import (
	"encoding/json"
	"time"
)

// RetryMessage 重试消息包装结构
type RetryMessage struct {
	OriginalValue   []byte `json:"original_value"`   // 原始消息内容
	OriginalTopic   string `json:"original_topic"`   // 原始 topic
	RetryCount      int    `json:"retry_count"`      // 队列重试次数
	LocalRetryCount int    `json:"local_retry_count"` // 本地重试次数
	NextRetryAt     int64  `json:"next_retry_at"`    // 下次重试时间 (Unix 时间戳)
	FirstFailedAt   int64  `json:"first_failed_at"`  // 首次失败时间 (Unix 时间戳)
	LastFailedAt    int64  `json:"last_failed_at"`   // 最后失败时间 (Unix 时间戳)
	LastError       string `json:"last_error"`       // 最后错误信息
}

// NewRetryMessage 创建新的重试消息
func NewRetryMessage(originalValue []byte, originalTopic string, lastError string) *RetryMessage {
	now := time.Now().Unix()
	return &RetryMessage{
		OriginalValue:   originalValue,
		OriginalTopic:   originalTopic,
		RetryCount:      0,
		LocalRetryCount: 0,
		FirstFailedAt:   now,
		LastFailedAt:    now,
		LastError:       lastError,
	}
}

// IncrementRetry 增加重试计数并更新时间
func (m *RetryMessage) IncrementRetry(nextDelay time.Duration) {
	m.RetryCount++
	m.LocalRetryCount = 0 // 重置本地重试计数
	m.LastFailedAt = time.Now().Unix()
	m.NextRetryAt = time.Now().Add(nextDelay).Unix()
}

// IncrementLocalRetry 增加本地重试计数
func (m *RetryMessage) IncrementLocalRetry() {
	m.LocalRetryCount++
	m.LastFailedAt = time.Now().Unix()
}

// CanLocalRetry 检查是否可以进行本地重试
func (m *RetryMessage) CanLocalRetry(config *RetryConfig) bool {
	return config.LocalRetryEnabled && m.LocalRetryCount < config.LocalMaxRetries
}

// CanQueueRetry 检查是否可以进行队列重试
func (m *RetryMessage) CanQueueRetry(config *RetryConfig) bool {
	return config.RetryQueueEnabled && m.RetryCount < config.RetryQueueMaxRetries
}

// ShouldGoToDeadLetter 检查是否应该进入死信队列
func (m *RetryMessage) ShouldGoToDeadLetter(config *RetryConfig) bool {
	return m.RetryCount >= config.RetryQueueMaxRetries
}

// IsReadyForRetry 检查是否已到达重试时间
func (m *RetryMessage) IsReadyForRetry() bool {
	if m.NextRetryAt == 0 {
		return true
	}
	return time.Now().Unix() >= m.NextRetryAt
}

// SetError 设置错误信息
func (m *RetryMessage) SetError(err string) {
	m.LastError = err
	m.LastFailedAt = time.Now().Unix()
}

// ToJSON 序列化为 JSON
func (m *RetryMessage) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}

// ParseRetryMessage 从 JSON 解析重试消息
func ParseRetryMessage(data []byte) (*RetryMessage, error) {
	var msg RetryMessage
	err := json.Unmarshal(data, &msg)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

// Clone 克隆重试消息
func (m *RetryMessage) Clone() *RetryMessage {
	return &RetryMessage{
		OriginalValue:   m.OriginalValue,
		OriginalTopic:   m.OriginalTopic,
		RetryCount:      m.RetryCount,
		LocalRetryCount: m.LocalRetryCount,
		NextRetryAt:     m.NextRetryAt,
		FirstFailedAt:   m.FirstFailedAt,
		LastFailedAt:    m.LastFailedAt,
		LastError:       m.LastError,
	}
}

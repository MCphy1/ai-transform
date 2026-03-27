package kafka

import (
	"ai-transform-backend/pkg/log"
	"context"
	"fmt"
	"math"
	"time"

	"github.com/IBM/sarama"
)

type ConsumerGroupConfig struct {
	Config
	RetryConfig *RetryConfig
}

type ConsumerGroup interface {
	Start(ctx context.Context, groupID string, topics []string)
}
type MessageHandleFunc func(message *sarama.ConsumerMessage) error
type consumerGroup struct {
	log               log.ILogger
	conf              *ConsumerGroupConfig
	retryConfig       *RetryConfig
	messageHandleFunc MessageHandleFunc
}

func NewConsumerGroup(config *ConsumerGroupConfig, log log.ILogger, messageHandleFunc MessageHandleFunc) ConsumerGroup {
	// 如果没有提供重试配置，使用默认配置
	retryConfig := config.RetryConfig
	if retryConfig == nil {
		retryConfig = DefaultRetryConfig()
	}
	return &consumerGroup{
		log:               log,
		conf:              config,
		retryConfig:       retryConfig,
		messageHandleFunc: messageHandleFunc,
	}
}
func (cg *consumerGroup) Start(ctx context.Context, groupID string, topics []string) {
	// 自动创建重试和死信 topic
	cg.ensureTopicsExist(topics)

	config := sarama.NewConfig()
	config.Version = cg.conf.Version
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Net.SASL.Enable = true
	config.Net.SASL.User = cg.conf.User
	config.Net.SASL.Password = cg.conf.Pwd
	config.Net.SASL.Mechanism = sarama.SASLMechanism(cg.conf.SASLMechanism)
	client, err := sarama.NewConsumerGroup(cg.conf.BrokerList, groupID, config)
	if err != nil {
		cg.log.Error(err)
		return
	}
	cgh := &consumerGroupHandler{
		messageHandleFunc: cg.messageHandleFunc,
		log:               cg.log,
		retryConfig:       cg.retryConfig,
	}
	for {
		if err := client.Consume(ctx, topics, cgh); err != nil {
			if err == sarama.ErrClosedConsumerGroup {
				return
			}
			cg.log.Error(err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// ensureTopicsExist 确保重试和死信 topic 存在
func (cg *consumerGroup) ensureTopicsExist(topics []string) {
	// 创建 admin client 配置
	config := sarama.NewConfig()
	config.Version = cg.conf.Version
	config.Net.SASL.Enable = true
	config.Net.SASL.User = cg.conf.User
	config.Net.SASL.Password = cg.conf.Pwd
	config.Net.SASL.Mechanism = sarama.SASLMechanism(cg.conf.SASLMechanism)

	adminClient, err := sarama.NewClusterAdmin(cg.conf.BrokerList, config)
	if err != nil {
		cg.log.ErrorF("Failed to create cluster admin: %v", err)
		return
	}
	defer adminClient.Close()

	// 获取已存在的 topic 列表
	existingTopics, err := adminClient.ListTopics()
	if err != nil {
		cg.log.ErrorF("Failed to list topics: %v", err)
		return
	}

	// 为每个原始 topic 创建重试和死信 topic
	for _, topic := range topics {
		if IsRetryTopic(topic) {
			continue // 跳过重试 topic
		}

		retryTopic := GetRetryTopic(topic)
		deadLetterTopic := GetDeadLetterTopic(topic)

		// 创建重试 topic
		if _, exists := existingTopics[retryTopic]; !exists && cg.retryConfig.RetryQueueEnabled {
			err := adminClient.CreateTopic(retryTopic, &sarama.TopicDetail{
				NumPartitions:     3,
				ReplicationFactor: 1,
			}, false)
			if err != nil {
				cg.log.ErrorF("Failed to create retry topic %s: %v", retryTopic, err)
			} else {
				cg.log.InfoF("Created retry topic: %s", retryTopic)
			}
		}

		// 创建死信 topic
		if _, exists := existingTopics[deadLetterTopic]; !exists && cg.retryConfig.DeadLetterEnabled {
			err := adminClient.CreateTopic(deadLetterTopic, &sarama.TopicDetail{
				NumPartitions:     1,
				ReplicationFactor: 1,
			}, false)
			if err != nil {
				cg.log.ErrorF("Failed to create dead letter topic %s: %v", deadLetterTopic, err)
			} else {
				cg.log.InfoF("Created dead letter topic: %s", deadLetterTopic)
			}
		}
	}
}

type consumerGroupHandler struct {
	messageHandleFunc MessageHandleFunc
	log               log.ILogger
	retryConfig       *RetryConfig
}

func (cgh *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}
func (cgh *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
func (cgh *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			{
				if !ok {
					log.Info("message channel closed")
					return nil
				}
				cgh.handleMessageWithRetry(session, message, claim.Topic())
				session.MarkMessage(message, "")
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

// handleMessageWithRetry 主重试逻辑
func (cgh *consumerGroupHandler) handleMessageWithRetry(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage, topic string) {
	// 判断是否是重试消息
	if IsRetryTopic(topic) {
		cgh.handleRetryMessage(session, message, topic)
		return
	}

	// 处理原始消息
	err := cgh.messageHandleFunc(message)
	if err == nil {
		return
	}

	cgh.log.ErrorF("Message handling failed for topic %s: %v", topic, err)

	// 开始重试流程
	cgh.startRetryProcess(message, topic, err)
}

// handleRetryMessage 处理来自重试队列的消息
func (cgh *consumerGroupHandler) handleRetryMessage(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage, topic string) {
	retryMsg, err := ParseRetryMessage(message.Value)
	if err != nil {
		cgh.log.ErrorF("Failed to parse retry message: %v", err)
		return
	}

	// 检查是否到达重试时间
	if !retryMsg.IsReadyForRetry() {
		// 计算需要等待的时间
		now := time.Now().Unix()
		waitSeconds := retryMsg.NextRetryAt - now

		if waitSeconds > 60 {
			// 长时间延迟：先等待60秒，再重新入队
			// 这样避免频繁重新入队，也不会阻塞太久
			cgh.log.InfoF("Retry message not ready yet (wait %ds), waiting 60s then re-queuing", waitSeconds)
			time.Sleep(60 * time.Second)
			cgh.requeueRetryMessage(retryMsg)
			return
		} else if waitSeconds > 0 {
			// 短时间延迟：直接等待
			cgh.log.InfoF("Retry message not ready yet, waiting %d seconds until next retry", waitSeconds)
			time.Sleep(time.Duration(waitSeconds) * time.Second)
		}
	}

	cgh.log.InfoF("Processing retry message, queue retry count: %d", retryMsg.RetryCount)

	// 构造原始消息进行重试
	originalMsg := &sarama.ConsumerMessage{
		Topic:     retryMsg.OriginalTopic,
		Partition: message.Partition,
		Offset:    message.Offset,
		Key:       message.Key,
		Value:     retryMsg.OriginalValue,
		Headers:   message.Headers,
		Timestamp: message.Timestamp,
	}

	// 尝试处理消息
	var processErr error
	if cgh.retryConfig.LocalRetryEnabled {
		// 本地重试启用：使用本地重试逻辑
		processErr = cgh.localRetry(originalMsg, retryMsg)
		if processErr == nil {
			cgh.log.InfoF("Retry succeeded after %d local retries, total queue retries: %d",
				retryMsg.LocalRetryCount, retryMsg.RetryCount)
			return
		}
		cgh.log.ErrorF("Local retry failed: %v", processErr)
	} else {
		// 本地重试禁用：直接尝试处理一次
		processErr = cgh.messageHandleFunc(originalMsg)
		if processErr == nil {
			cgh.log.InfoF("Retry succeeded at queue retry count: %d", retryMsg.RetryCount)
			return
		}
		cgh.log.ErrorF("Message processing failed: %v", processErr)
		retryMsg.SetError(processErr.Error())
	}

	// 处理失败，判断下一步
	if retryMsg.ShouldGoToDeadLetter(cgh.retryConfig) {
		// 发送到死信队列
		cgh.sendToDeadLetterQueue(retryMsg)
		return
	}

	// 发送到重试队列
	cgh.sendToRetryQueue(retryMsg)
}

// requeueRetryMessage 重新发送重试消息到队列（不增加重试次数）
func (cgh *consumerGroupHandler) requeueRetryMessage(retryMsg *RetryMessage) {
	producer := GetProducer(cgh.retryConfig.ProducerKey)
	if producer == nil {
		cgh.log.Error("Producer not found for requeue")
		return
	}

	// 序列化消息（保持原有的重试次数和重试时间）
	data, err := retryMsg.ToJSON()
	if err != nil {
		cgh.log.ErrorF("Failed to serialize retry message: %v", err)
		return
	}

	retryTopic := GetRetryTopic(retryMsg.OriginalTopic)
	msg := &sarama.ProducerMessage{
		Topic: retryTopic,
		Key:   sarama.StringEncoder(fmt.Sprintf("%d", retryMsg.FirstFailedAt)),
		Value: sarama.ByteEncoder(data),
	}

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		cgh.log.ErrorF("Failed to requeue retry message: %v", err)
		return
	}

	cgh.log.DebugF("Re-queued retry message to %s, will retry at %d", retryTopic, retryMsg.NextRetryAt)
}

// startRetryProcess 开始重试流程
func (cgh *consumerGroupHandler) startRetryProcess(message *sarama.ConsumerMessage, topic string, originalErr error) {
	retryMsg := NewRetryMessage(message.Value, topic, originalErr.Error())

	// 尝试本地重试
	if cgh.retryConfig.LocalRetryEnabled {
		err := cgh.localRetry(message, retryMsg)
		if err == nil {
			cgh.log.InfoF("Message processed successfully after %d local retries", retryMsg.LocalRetryCount)
			return
		}
		cgh.log.ErrorF("Local retry exhausted after %d attempts: %v", retryMsg.LocalRetryCount, err)
	}

	// 本地重试失败，判断是否发送到重试队列
	if cgh.retryConfig.ShouldSendToRetryQueue(retryMsg.RetryCount) {
		cgh.sendToRetryQueue(retryMsg)
		return
	}

	// 超过最大重试次数，发送到死信队列
	if cgh.retryConfig.DeadLetterEnabled {
		cgh.sendToDeadLetterQueue(retryMsg)
	}
}

// localRetry 本地重试（指数退避）
func (cgh *consumerGroupHandler) localRetry(message *sarama.ConsumerMessage, retryMsg *RetryMessage) error {
	var lastErr error

	for i := 0; i < cgh.retryConfig.LocalMaxRetries; i++ {
		// 计算延迟时间（指数退避）
		delay := cgh.calculateBackoffDelay(retryMsg.LocalRetryCount)

		cgh.log.DebugF("Local retry %d/%d, waiting %v before retry",
			retryMsg.LocalRetryCount+1, cgh.retryConfig.LocalMaxRetries, delay)

		time.Sleep(delay)

		// 增加重试计数
		retryMsg.IncrementLocalRetry()

		// 尝试处理
		err := cgh.messageHandleFunc(message)
		if err == nil {
			return nil
		}

		lastErr = err
		cgh.log.ErrorF("Local retry %d failed: %v", retryMsg.LocalRetryCount, err)
		retryMsg.SetError(err.Error())
	}

	return lastErr
}

// calculateBackoffDelay 计算指数退避延迟
func (cgh *consumerGroupHandler) calculateBackoffDelay(retryCount int) time.Duration {
	// 基础延迟 * 2^retryCount，最大 1 分钟
	baseDelay := cgh.retryConfig.LocalRetryDelay
	maxDelay := 1 * time.Minute

	delay := time.Duration(float64(baseDelay) * math.Pow(2, float64(retryCount)))
	if delay > maxDelay {
		delay = maxDelay
	}

	return delay
}

// sendToRetryQueue 发送到重试队列
func (cgh *consumerGroupHandler) sendToRetryQueue(retryMsg *RetryMessage) {
	producer := GetProducer(cgh.retryConfig.ProducerKey)
	if producer == nil {
		cgh.log.Error("Producer not found for retry queue")
		return
	}

	// 计算下次重试时间
	nextDelay := cgh.retryConfig.GetRetryInterval(retryMsg.RetryCount)
	retryMsg.IncrementRetry(nextDelay)

	// 序列化消息
	data, err := retryMsg.ToJSON()
	if err != nil {
		cgh.log.ErrorF("Failed to serialize retry message: %v", err)
		return
	}

	retryTopic := GetRetryTopic(retryMsg.OriginalTopic)
	msg := &sarama.ProducerMessage{
		Topic: retryTopic,
		Key:   sarama.StringEncoder(fmt.Sprintf("%d", retryMsg.FirstFailedAt)),
		Value: sarama.ByteEncoder(data),
	}

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		cgh.log.ErrorF("Failed to send message to retry queue %s: %v", retryTopic, err)
		return
	}

	cgh.log.InfoF("Message sent to retry queue %s, retry count: %d, next retry in: %v",
		retryTopic, retryMsg.RetryCount, nextDelay)
}

// sendToDeadLetterQueue 发送到死信队列
func (cgh *consumerGroupHandler) sendToDeadLetterQueue(retryMsg *RetryMessage) {
	producer := GetProducer(cgh.retryConfig.ProducerKey)
	if producer == nil {
		cgh.log.Error("Producer not found for dead letter queue")
		return
	}

	// 序列化消息
	data, err := retryMsg.ToJSON()
	if err != nil {
		cgh.log.ErrorF("Failed to serialize dead letter message: %v", err)
		return
	}

	deadLetterTopic := GetDeadLetterTopic(retryMsg.OriginalTopic)
	msg := &sarama.ProducerMessage{
		Topic: deadLetterTopic,
		Key:   sarama.StringEncoder(fmt.Sprintf("%d", retryMsg.FirstFailedAt)),
		Value: sarama.ByteEncoder(data),
	}

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		cgh.log.ErrorF("Failed to send message to dead letter queue %s: %v", deadLetterTopic, err)
		return
	}

	cgh.log.InfoF("Message sent to dead letter queue %s after %d retries, original topic: %s, last error: %s",
		deadLetterTopic, retryMsg.RetryCount, retryMsg.OriginalTopic, retryMsg.LastError)

	// 调用全局死信队列处理器（更新数据库状态）
	HandleDeadLetter(retryMsg.OriginalValue, retryMsg.LastError, retryMsg.RetryCount)

	// 调用自定义回调函数（如果有）
	if cgh.retryConfig.OnDeadLetter != nil {
		cgh.retryConfig.OnDeadLetter(retryMsg.OriginalValue, retryMsg.LastError, retryMsg.RetryCount)
	}
}

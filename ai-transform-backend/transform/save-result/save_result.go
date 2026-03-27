package save_result

import (
	"ai-transform-backend/data"
	_interface "ai-transform-backend/interface"
	"ai-transform-backend/message"
	"ai-transform-backend/pkg/config"
	"ai-transform-backend/pkg/constants"
	"ai-transform-backend/pkg/log"
	"ai-transform-backend/pkg/mq/kafka"
	"ai-transform-backend/pkg/storage"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/IBM/sarama"
)

// ========== 测试重试机制 ==========
// 设置为 true 来测试重试功能，会在处理消息时模拟失败
// 测试完成后记得改回 false
const TEST_RETRY_ENABLED = false

// 控制模拟失败的次数（-1 表示一直失败，用于测试死信队列）
var testRetryFailCount = 3
var testCurrentFailCount = 0

// ===================================

type saveResult struct {
	conf              *config.Config
	log               log.ILogger
	cosStorageFactory storage.StorageFactory
	data              data.IData
}

func NewSaveResult(conf *config.Config, log log.ILogger, cosStorageFactory storage.StorageFactory, data data.IData) _interface.ConsumerTask {
	return &saveResult{
		conf:              conf,
		log:               log,
		cosStorageFactory: cosStorageFactory,
		data:              data,
	}
}

func (t *saveResult) Start(ctx context.Context) {
	cfg := t.conf

	// 测试队列重试时使用的配置（禁用本地重试，缩短队列重试间隔）
	retryConfig := kafka.DefaultRetryConfig()
	if TEST_RETRY_ENABLED {
		retryConfig.LocalRetryEnabled = false // 禁用本地重试，直接进入队列
		retryConfig.RetryQueueIntervals = []time.Duration{
			10 * time.Second, // 10秒后第一次队列重试
			30 * time.Second, // 30秒后第二次
			1 * time.Minute,  // 1分钟后第三次
			2 * time.Minute,  // 2分钟后第四次
			5 * time.Minute,  // 5分钟后第五次
		}
	}

	conf := &kafka.ConsumerGroupConfig{
		Config: kafka.Config{
			BrokerList:    cfg.Kafka.Address,
			User:          cfg.Kafka.User,
			Pwd:           cfg.Kafka.Pwd,
			SASLMechanism: cfg.Kafka.SaslMechanism,
			Version:       sarama.V3_7_0_0,
		},
		RetryConfig: retryConfig,
	}
	// 订阅原始 topic 和重试 topic
	topics := []string{
		constants.KAFKA_TOPIC_TRANSFORM_SAVE_RESULT,
		kafka.GetRetryTopic(constants.KAFKA_TOPIC_TRANSFORM_SAVE_RESULT),
	}
	cg := kafka.NewConsumerGroup(conf, t.log, t.messageHandleFunc)
	cg.Start(ctx, constants.KAFKA_TOPIC_TRANSFORM_SAVE_RESULT, topics)
}

func (t *saveResult) messageHandleFunc(consumerMessage *sarama.ConsumerMessage) error {
	fmt.Printf("save begin\n")

	// ========== 测试重试机制 ==========
	if TEST_RETRY_ENABLED {
		testCurrentFailCount++
		t.log.InfoF("[TEST] Test retry triggered, current fail count: %d, target: %d", testCurrentFailCount, testRetryFailCount)
		if testRetryFailCount == -1 || testCurrentFailCount <= testRetryFailCount {
			return errors.New("simulated error for retry testing")
		}
		// 达到指定失败次数后，重置计数器并继续正常处理
		t.log.InfoF("[TEST] Simulated failures completed, processing normally now")
		testCurrentFailCount = 0
	}
	// ===================================

	saveResultMsg := &message.KafkaMsg{}
	err := json.Unmarshal(consumerMessage.Value, saveResultMsg)
	if err != nil {
		t.log.Error(err)
		return err
	}
	s := t.cosStorageFactory.CreateStorage()
	saveFilePath := fmt.Sprintf("/%s/%s", constants.COS_OUTPUT, path.Base(saveResultMsg.OutPutFilePath))
	url, err := s.UploadFromFile(saveResultMsg.OutPutFilePath, saveFilePath)
	if err != nil {
		t.log.Error(err)
		return err
	}
	recordsData := t.data.NewTransformRecordsData()
	err = recordsData.Update(&data.TransformRecords{
		ID:                 saveResultMsg.RecordsID,
		Status:             constants.STATUS_COMPLETED,
		TranslatedVideoUrl: url,
		UpdateAt:           time.Now().Unix(),
		ExpirationAt:       time.Now().Add(time.Hour * 72).Unix(),
	})
	if err != nil {
		t.log.Error(err)
		return err
	}

	fmt.Printf("save end\n")

	return err
}

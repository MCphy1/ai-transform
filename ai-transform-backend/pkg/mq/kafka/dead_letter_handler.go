package kafka

import (
	"encoding/json"
	"time"

	"ai-transform-backend/data"
	"ai-transform-backend/message"
	"ai-transform-backend/pkg/constants"
	"ai-transform-backend/pkg/log"
)

// 全局死信队列处理器依赖
var (
	deadLetterDataFactory data.IData
	deadLetterLogger      log.ILogger
)

// InitDeadLetterHandler 初始化全局死信队列处理器
// 应该在程序启动时调用一次
func InitDeadLetterHandler(dataFactory data.IData, logger log.ILogger) {
	deadLetterDataFactory = dataFactory
	deadLetterLogger = logger
}

// HandleDeadLetter 处理进入死信队列的消息
// 更新数据库状态为 failed
func HandleDeadLetter(originalValue []byte, lastError string, retryCount int) {
	// 检查是否已初始化
	if deadLetterDataFactory == nil || deadLetterLogger == nil {
		return // 未初始化，跳过
	}

	// 解析原始消息获取 RecordsID
	msg := &message.KafkaMsg{}
	err := json.Unmarshal(originalValue, msg)
	if err != nil {
		deadLetterLogger.ErrorF("Failed to unmarshal message in dead letter handler: %v", err)
		return
	}

	// 如果没有 RecordsID，跳过
	if msg.RecordsID == 0 {
		deadLetterLogger.Debug("No RecordsID in message, skipping status update")
		return
	}

	now := time.Now().Unix()

	// 更新数据库状态为 failed
	recordsData := deadLetterDataFactory.NewTransformRecordsData()
	err = recordsData.Update(&data.TransformRecords{
		ID:       msg.RecordsID,
		Status:   constants.STATUS_FAILED,
		UpdateAt: now,
	})
	if err != nil {
		deadLetterLogger.ErrorF("Failed to update record %d status to failed: %v", msg.RecordsID, err)
		return
	}

	// 记录失败信息
	err = recordsData.UpdateFailedInfo(msg.RecordsID, lastError, now)
	if err != nil {
		deadLetterLogger.ErrorF("Failed to update failed info for record %d: %v", msg.RecordsID, err)
		return
	}

	deadLetterLogger.InfoF("Record %d status updated to failed after %d retries, error: %s", msg.RecordsID, retryCount, lastError)
}

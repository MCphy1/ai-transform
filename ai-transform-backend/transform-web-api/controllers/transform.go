package controllers

import (
	"ai-transform-backend/data"
	"ai-transform-backend/message"
	"ai-transform-backend/pkg/config"
	"ai-transform-backend/pkg/constants"
	"ai-transform-backend/pkg/log"
	"ai-transform-backend/pkg/mq/kafka"
	"ai-transform-backend/pkg/storage"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

type Transform struct {
	conf              *config.Config
	log               log.ILogger
	data              data.IData
	cosStorageFactory storage.StorageFactory
	referWavDir       string
}

func NewTransform(conf *config.Config, log log.ILogger, data data.IData, cosStorageFactory storage.StorageFactory) *Transform {
	referWavDir := "D:/program-GO/GPT-SoVITS-v2pro-20250604/runtime/test-refer"
	if conf.Http.Mode == gin.ReleaseMode {
		referWavDir = constants.REFER_WAV
	} else if conf.Http.Mode == gin.TestMode {
		referWavDir = constants.TEST_REFER_WAV
	}

	return &Transform{
		conf:              conf,
		log:               log,
		data:              data,
		cosStorageFactory: cosStorageFactory,
		referWavDir:       referWavDir,
	}
}

type transInfo struct {
	ID                int64  `form:"id"`
	ProjectName       string `form:"project_name" binding:"required"`
	OriginalLanguage  string `form:"original_language" binding:"required"`
	TranslateLanguage string `form:"translate_language" binding:"required"`
	FileUrl           string `form:"file_url" binding:"required,url"`
	ProofreadType     string `form:"proofread_type"`
}

func (c *Transform) Translate(ctx *gin.Context) {
	userID, _ := ctx.Get("User.ID")
	ti := &transInfo{}
	err := ctx.ShouldBind(ti)
	if err != nil {
		c.log.Error(err)
		ctx.JSON(http.StatusBadRequest, gin.H{})
		return
	}
	entity := &data.TransformRecords{
		UserID:             userID.(int64),
		ProjectName:        ti.ProjectName,
		OriginalLanguage:   ti.OriginalLanguage,
		TranslatedLanguage: ti.TranslateLanguage,
		ProofreadType:      ti.ProofreadType,
		OriginalVideoUrl:   ti.FileUrl,
		CreateAt:           time.Now().Unix(),
		UpdateAt:           time.Now().Unix(),
	}
	recordData := c.data.NewTransformRecordsData()

	// 判断是新建还是更新
	if ti.ID > 0 {
		// 重新翻译：清空旧结果，重新发送消息
		err = recordData.ClearTranslationResult(ti.ID, ti.ProofreadType, time.Now().Unix())
		entity.ID = ti.ID
	} else {
		// 创建新记录
		err = recordData.Add(entity)
	}

	// err = recordData.Add(entity)

	if err != nil {
		c.log.Error(err)
		ctx.JSON(http.StatusInternalServerError, gin.H{})
		return
	}
	// 将消息推送到kafka
	entryMsg := message.KafkaMsg{
		RecordsID:        entity.ID,
		UserID:           userID.(int64),
		OriginalVideoUrl: ti.FileUrl,
		SourceLanguage:   ti.OriginalLanguage,
		TargetLanguage:   ti.TranslateLanguage,
		ProofreadType:    ti.ProofreadType,
	}
	value, err := json.Marshal(entryMsg)
	if err != nil {
		c.log.Error(err)
		ctx.JSON(http.StatusInternalServerError, gin.H{})
		return
	}
	producer := kafka.GetProducer(kafka.ExternalProducer)
	msg := &sarama.ProducerMessage{
		Topic: constants.KAFKA_TOPIC_TRANSFORM_WEB_ENTRY,
		Value: sarama.StringEncoder(value),
	}
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		c.log.Error(err)
		ctx.JSON(http.StatusInternalServerError, gin.H{})
		return
	}
}

type record struct {
	ID                 int64  `json:"id"`
	ProjectName        string `json:"project_name"`
	OriginalLanguage   string `json:"original_language"`
	TranslatedLanguage string `json:"translated_language"`
	OriginalVideoUrl   string `json:"original_video_url"`
	TranslatedVideoUrl string `json:"translated_video_url"`
	ExpirationAt       int64  `json:"expiration_at"`
	CreateAt           int64  `json:"create_at"`
}

func (c *Transform) GetRecords(ctx *gin.Context) {
	userID, _ := ctx.Get("User.ID")
	recordsData := c.data.NewTransformRecordsData()
	list, err := recordsData.GetByUserID(userID.(int64))
	if err != nil {
		c.log.Error(err)
		ctx.JSON(http.StatusInternalServerError, gin.H{})
		return
	}
	records := make([]record, len(list))
	for index, l := range list {
		records[index].ID = l.ID
		records[index].ProjectName = l.ProjectName
		records[index].OriginalLanguage = l.OriginalLanguage
		records[index].TranslatedLanguage = l.TranslatedLanguage
		records[index].OriginalVideoUrl = l.OriginalVideoUrl
		records[index].TranslatedVideoUrl = l.TranslatedVideoUrl
		records[index].ExpirationAt = l.ExpirationAt
		records[index].CreateAt = l.CreateAt
	}
	ctx.JSON(http.StatusOK, records)
}

// proofreadRecord 待校对任务响应结构
type proofreadRecord struct {
	ID                           int64  `json:"id"`
	ProjectName                  string `json:"project_name"`
	OriginalLanguage             string `json:"original_language"`
	TranslatedLanguage           string `json:"translated_language"`
	Status                       string `json:"status"`
	OriginalVideoUrl             string `json:"original_video_url"`
	TranslatedSrtForProofreadUrl string `json:"translated_srt_for_proofread_url"`
	CreateAt                     int64  `json:"create_at"`
}

// GetPendingProofreads 获取待校对的任务列表
func (c *Transform) GetPendingProofreads(ctx *gin.Context) {
	userID, _ := ctx.Get("User.ID")
	recordsData := c.data.NewTransformRecordsData()
	list, err := recordsData.GetPendingProofreadsByUserID(userID.(int64))
	if err != nil {
		c.log.Error(err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "获取失败"})
		return
	}
	records := make([]proofreadRecord, len(list))
	for index, l := range list {
		records[index].ID = l.ID
		records[index].ProjectName = l.ProjectName
		records[index].OriginalLanguage = l.OriginalLanguage
		records[index].TranslatedLanguage = l.TranslatedLanguage
		records[index].Status = l.Status
		records[index].OriginalVideoUrl = l.OriginalVideoUrl
		records[index].TranslatedSrtForProofreadUrl = l.TranslatedSrtForProofreadUrl
		records[index].CreateAt = l.CreateAt
	}
	ctx.JSON(http.StatusOK, records)
}

// GetProofreadSrt 获取待校对的字幕内容
func (c *Transform) GetProofreadSrt(ctx *gin.Context) {
	userID, _ := ctx.Get("User.ID")
	recordIDStr := ctx.Param("id")
	recordID, err := strconv.ParseInt(recordIDStr, 10, 64)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "无效的ID"})
		return
	}

	// 获取记录
	recordsData := c.data.NewTransformRecordsData()
	record, err := recordsData.GetByID(recordID)
	if err != nil || record == nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "记录不存在"})
		return
	}

	// 验证用户权限
	if record.UserID != userID.(int64) {
		ctx.JSON(http.StatusForbidden, gin.H{"error": "无权访问"})
		return
	}

	// 获取原始字幕和翻译字幕内容
	originalSrtContent, _ := c.fetchSrtContent(record.OriginalSrtUrl)
	translatedSrtContent, _ := c.fetchSrtContent(record.TranslatedSrtForProofreadUrl)

	ctx.JSON(http.StatusOK, gin.H{
		"original_srt":       originalSrtContent,
		"translated_srt":     translatedSrtContent,
		"original_srt_url":   record.OriginalSrtUrl,
		"translated_srt_url": record.TranslatedSrtForProofreadUrl,
	})
}

// fetchSrtContent 从URL获取字幕内容
func (c *Transform) fetchSrtContent(url string) (string, error) {
	if url == "" {
		return "", nil
	}
	resp, err := http.Get(url)
	if err != nil {
		c.log.Error(fmt.Sprintf("Failed to fetch srt content: %v", err))
		return "", err
	}
	defer resp.Body.Close()
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		c.log.Error(fmt.Sprintf("Failed to read srt content: %v", err))
		return "", err
	}
	return string(content), nil
}

// SubmitProofread 提交校对后的字幕
func (c *Transform) SubmitProofread(ctx *gin.Context) {
	userID, _ := ctx.Get("User.ID")
	recordIDStr := ctx.Param("id")
	recordID, err := strconv.ParseInt(recordIDStr, 10, 64)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "无效的ID"})
		return
	}

	// 解析请求体
	var req struct {
		ProofreadSrt string `json:"proofread_srt" binding:"required"`
	}
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "无效的请求"})
		return
	}

	// 获取记录
	recordsData := c.data.NewTransformRecordsData()
	record, err := recordsData.GetByID(recordID)
	if err != nil || record == nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "记录不存在"})
		return
	}

	// 验证用户权限
	if record.UserID != userID.(int64) {
		ctx.JSON(http.StatusForbidden, gin.H{"error": "无权访问"})
		return
	}

	// 从 OriginalVideoUrl 派生 Filename
	filename := strings.TrimSuffix(path.Base(record.OriginalVideoUrl), path.Ext(record.OriginalVideoUrl))

	// 保存校对后的字幕到本地
	proofreadSrtFilename := fmt.Sprintf("%s_proofreadbyuser.srt", filename)
	proofreadSrtPath := fmt.Sprintf("%s/%s", constants.SRTS_DIR, proofreadSrtFilename)
	err = os.WriteFile(proofreadSrtPath, []byte(req.ProofreadSrt), 0644)
	if err != nil {
		c.log.Error(err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "保存失败"})
		return
	}

	// 上传到 COS
	storageSrtPath := fmt.Sprintf("%s/%s", constants.COS_SRTS, proofreadSrtFilename)
	st := c.cosStorageFactory.CreateStorage()
	proofreadSrtUrl, err := st.UploadFromFile(proofreadSrtPath, storageSrtPath)
	if err != nil {
		c.log.Error(err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "上传失败"})
		return
	}

	// 更新数据库
	err = recordsData.UpdateProofreadResult(recordID, proofreadSrtUrl, constants.STATUS_PROCESSING, time.Now().Unix())
	if err != nil {
		c.log.Error(err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "更新失败"})
		return
	}

	// 构造 ExtractVideoPath
	extractVideoPath := fmt.Sprintf("%s/%s/%s.mp4", constants.MIDDLE_DIR, filename, filename)

	// 构造 ReferWavPath
	referWavPath := c.getReferWavPath(recordID)

	// 从 refer-wav 服务获取 PromptText 和 PromptLanguage
	promptText, promptLanguage := c.getReferInfo(recordID)

	// 发送消息到音频生成队列
	msg := &message.KafkaMsg{
		RecordsID:           recordID,
		UserID:              record.UserID,
		OriginalVideoUrl:    record.OriginalVideoUrl,
		SourceLanguage:      record.OriginalLanguage,
		TargetLanguage:      record.TranslatedLanguage,
		ProofreadType:       constants.PROOFREAD_TYPE_MANUAL,
		ProofreadingSrtPath: proofreadSrtPath,
		Filename:            filename,
		ExtractVideoPath:    extractVideoPath,
		ReferWavPath:        referWavPath,
		PromptText:          promptText,
		PromptLanguage:      promptLanguage,
	}

	value, err := json.Marshal(msg)
	if err != nil {
		c.log.Error(err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "序列化消息失败"})
		return
	}

	producer := kafka.GetProducer(kafka.Producer)
	kafkaMsg := &sarama.ProducerMessage{
		Topic: constants.KAFKA_TOPIC_TRANSFORM_AUDIO_GENERATION,
		Value: sarama.StringEncoder(value),
	}
	_, _, err = producer.SendMessage(kafkaMsg)
	if err != nil {
		c.log.Error(err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "发送消息失败"})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "success"})
}

// getReferInfo 从 refer-wav 服务获取参考音频的提示文本和语言
func (c *Transform) getReferInfo(recordID int64) (promptText, promptLanguage string) {
	addr := c.conf.DependOn.ReferWav.Address
	url := fmt.Sprintf("%s/api/refer/wav?record_id=%d", addr, recordID)

	resp, err := http.Get(url)
	if err != nil {
		c.log.Error(fmt.Sprintf("Failed to get refer info: %v", err))
		return "", ""
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		c.log.Error(fmt.Sprintf("Failed to read refer info response: %v", err))
		return "", ""
	}

	var result struct {
		PromptText     string `json:"prompt_text"`
		PromptLanguage string `json:"prompt_language"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		c.log.Error(fmt.Sprintf("Failed to parse refer info response: %v", err))
		return "", ""
	}

	return result.PromptText, result.PromptLanguage
}

// getReferWavPath 获取参考音频的本地路径
func (c *Transform) getReferWavPath(recordID int64) string {
	return fmt.Sprintf("%s/%d.wav", c.referWavDir, recordID)
}

// failedRecord 失败任务响应结构
type failedRecord struct {
	ID                 int64  `json:"id"`
	ProjectName        string `json:"project_name"`
	OriginalLanguage   string `json:"original_language"`
	TranslatedLanguage string `json:"translated_language"`
	OriginalVideoUrl   string `json:"original_video_url"`
	ErrorMessage       string `json:"error_message"`
	FailedAt           int64  `json:"failed_at"`
	RetryCount         int    `json:"retry_count"`
	CreateAt           int64  `json:"create_at"`
}

// GetFailedRecords 获取失败的任务列表
func (c *Transform) GetFailedRecords(ctx *gin.Context) {
	userID, _ := ctx.Get("User.ID")
	recordsData := c.data.NewTransformRecordsData()
	list, err := recordsData.GetFailedByUserID(userID.(int64))
	if err != nil {
		c.log.Error(err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "获取失败"})
		return
	}
	records := make([]failedRecord, len(list))
	for index, l := range list {
		records[index].ID = l.ID
		records[index].ProjectName = l.ProjectName
		records[index].OriginalLanguage = l.OriginalLanguage
		records[index].TranslatedLanguage = l.TranslatedLanguage
		records[index].OriginalVideoUrl = l.OriginalVideoUrl
		records[index].ErrorMessage = l.ErrorMessage
		records[index].FailedAt = l.FailedAt
		records[index].RetryCount = l.RetryCount
		records[index].CreateAt = l.CreateAt
	}
	ctx.JSON(http.StatusOK, records)
}

// RetryFailedRecord 重试失败的任务
func (c *Transform) RetryFailedRecord(ctx *gin.Context) {
	userID, _ := ctx.Get("User.ID")
	recordIDStr := ctx.Param("id")
	recordID, err := strconv.ParseInt(recordIDStr, 10, 64)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "无效的ID"})
		return
	}

	// 获取记录
	recordsData := c.data.NewTransformRecordsData()
	record, err := recordsData.GetByID(recordID)
	if err != nil || record == nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "记录不存在"})
		return
	}

	// 验证用户权限
	if record.UserID != userID.(int64) {
		ctx.JSON(http.StatusForbidden, gin.H{"error": "无权访问"})
		return
	}

	// 验证状态是否为失败
	if record.Status != constants.STATUS_FAILED {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "该任务不是失败状态"})
		return
	}

	// 清空翻译结果，重置状态
	err = recordsData.ClearTranslationResult(recordID, record.ProofreadType, time.Now().Unix())
	if err != nil {
		c.log.Error(err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "重置失败"})
		return
	}

	// 将消息推送到kafka重新处理
	entryMsg := message.KafkaMsg{
		RecordsID:        recordID,
		UserID:           record.UserID,
		OriginalVideoUrl: record.OriginalVideoUrl,
		SourceLanguage:   record.OriginalLanguage,
		TargetLanguage:   record.TranslatedLanguage,
		ProofreadType:    record.ProofreadType,
	}
	value, err := json.Marshal(entryMsg)
	if err != nil {
		c.log.Error(err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "序列化消息失败"})
		return
	}
	producer := kafka.GetProducer(kafka.ExternalProducer)
	msg := &sarama.ProducerMessage{
		Topic: constants.KAFKA_TOPIC_TRANSFORM_WEB_ENTRY,
		Value: sarama.StringEncoder(value),
	}
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		c.log.Error(err)
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "发送消息失败"})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"message": "重试任务已提交"})
}

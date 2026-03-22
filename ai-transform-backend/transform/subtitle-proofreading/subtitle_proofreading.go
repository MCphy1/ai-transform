package subtitle_proofreading

import (
	_interface "ai-transform-backend/interface"
	"ai-transform-backend/message"
	"ai-transform-backend/pkg/config"
	"ai-transform-backend/pkg/constants"
	"ai-transform-backend/pkg/log"
	"ai-transform-backend/pkg/mq/kafka"
	"ai-transform-backend/pkg/storage"
	"ai-transform-backend/pkg/utils"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/IBM/sarama"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/openai"
	"gopkg.in/yaml.v3"
)

type subtitleProofreading struct {
	conf              *config.Config
	log               log.ILogger
	cosStorageFactory storage.StorageFactory
	llm               llms.Model
	proofreadingTerms map[string]string // 专有名词纠正表
}

func NewSubtitleProofreading(conf *config.Config, log log.ILogger, cosStorageFactory storage.StorageFactory) _interface.ConsumerTask {
	llm, err := openai.New(
		openai.WithModel(conf.LLM.Model),
		openai.WithBaseURL(conf.LLM.BaseURL),
		openai.WithToken(conf.LLM.APIKey),
	)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to create LLM: %v", err))
	}

	// 加载专有名词纠正表
	proofreadingTerms := loadProofreadingTerms(conf.ProofreadingTermsFile)

	return &subtitleProofreading{
		conf:              conf,
		log:               log,
		cosStorageFactory: cosStorageFactory,
		llm:               llm,
		proofreadingTerms: proofreadingTerms,
	}
}

func (s *subtitleProofreading) Start(ctx context.Context) {
	cfg := s.conf
	conf := &kafka.ConsumerGroupConfig{
		Config: kafka.Config{
			BrokerList:    cfg.Kafka.Address,
			User:          cfg.Kafka.User,
			Pwd:           cfg.Kafka.Pwd,
			SASLMechanism: cfg.Kafka.SaslMechanism,
			Version:       sarama.V3_7_0_0,
		},
	}
	cg := kafka.NewConsumerGroup(conf, s.log, s.messageHandleFunc)
	cg.Start(ctx, constants.KAFKA_TOPIC_TRANSFORM_SUBTITLE_PROOFREADING, []string{constants.KAFKA_TOPIC_TRANSFORM_SUBTITLE_PROOFREADING})
}

func (s *subtitleProofreading) messageHandleFunc(consumerMessage *sarama.ConsumerMessage) error {
	// fmt.Printf("subtitle proofreading begin\n")

	proofreadingMsg := &message.KafkaMsg{}
	err := json.Unmarshal(consumerMessage.Value, proofreadingMsg)
	if err != nil {
		s.log.Error(err)
		return err
	}
	s.log.Debug(proofreadingMsg)

	// 读取翻译后的字幕文件
	translateSrtPath := proofreadingMsg.TranslateSrtPath
	file, err := os.Open(translateSrtPath)
	if err != nil {
		s.log.Error(err)
		return err
	}
	defer file.Close()

	srtContentBytes, err := io.ReadAll(file)
	if err != nil {
		s.log.Error(err)
		return err
	}

	srtContentSlice := strings.Split(string(srtContentBytes), "\n")

	// 读取原始字幕文件（用于中英比对）
	var originalSrtMap map[int]string
	if proofreadingMsg.OriginalSrtPath != "" {
		originalSrtMap, err = s.readOriginalSrt(proofreadingMsg.OriginalSrtPath)
		if err != nil {
			s.log.Error(fmt.Sprintf("Failed to read original srt: %v", err))
			// 即使读取失败也继续，只是不进行中英比对
			originalSrtMap = nil
		}
	}

	// 使用 LLM 进行字幕校对（包含中英比对）
	err = s.proofreadSrtContent(srtContentSlice, originalSrtMap, proofreadingMsg.SourceLanguage, proofreadingMsg.TargetLanguage, s.proofreadingTerms)
	if err != nil {
		s.log.Error(err)
		return err
	}

	// 保存校对后的字幕
	proofreadingSrtFilename := fmt.Sprintf("%s_proofreading.srt", proofreadingMsg.Filename)
	proofreadingSrtPath := fmt.Sprintf("%s/%s", constants.SRTS_DIR, proofreadingSrtFilename)
	err = utils.SaveSrt(srtContentSlice, proofreadingSrtPath)
	if err != nil {
		s.log.Error(err)
		return err
	}

	// 上传校对后的字幕到 COS
	storageSrtPath := fmt.Sprintf("%s/%s", constants.COS_SRTS, path.Base(proofreadingSrtPath))
	st := s.cosStorageFactory.CreateStorage()
	_, err = st.UploadFromFile(proofreadingSrtPath, storageSrtPath)
	if err != nil {
		s.log.Error(err)
		return err
	}

	// 更新消息
	generationMsg := proofreadingMsg
	generationMsg.ProofreadingSrtPath = proofreadingSrtPath

	// 发送到音频生成队列
	value, err := json.Marshal(generationMsg)
	if err != nil {
		s.log.Error(err)
		return err
	}

	producer := kafka.GetProducer(kafka.Producer)
	msg := &sarama.ProducerMessage{
		Topic: constants.KAFKA_TOPIC_TRANSFORM_AUDIO_GENERATION,
		Value: sarama.StringEncoder(value),
	}
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		s.log.Error(err)
		return err
	}

	// fmt.Printf("subtitle proofreading end\n")

	return nil
}

// readOriginalSrt 读取原始字幕文件，返回位置到文本的映射
func (s *subtitleProofreading) readOriginalSrt(originalSrtPath string) (map[int]string, error) {
	file, err := os.Open(originalSrtPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	contentBytes, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	contentSlice := strings.Split(string(contentBytes), "\n")
	result := make(map[int]string)

	for i := 0; i < len(contentSlice); i += 4 {
		if i+2 < len(contentSlice) {
			position := 0
			fmt.Sscanf(contentSlice[i], "%d", &position)
			if position > 0 && i+2 < len(contentSlice) {
				result[position] = contentSlice[i+2]
			}
		}
	}

	return result, nil
}

// proofreadSrtContent 使用 LLM 对字幕内容进行校对（包含中英比对）
func (s *subtitleProofreading) proofreadSrtContent(srtContentSlice []string, originalSrtMap map[int]string, sourceLanguage, targetLanguage string, proofreadingTerms map[string]string) error {
	// 收集所有需要校对的文本
	type subtitleItem struct {
		position     int
		index        int
		text         string
		originalText string // 原始文本（用于中英比对）
	}
	items := make([]subtitleItem, 0)

	for i := 0; i < len(srtContentSlice); i += 4 {
		if i+2 < len(srtContentSlice) {
			text := srtContentSlice[i+2]
			if strings.TrimSpace(text) != "" {
				position := 0
				fmt.Sscanf(srtContentSlice[i], "%d", &position)

				item := subtitleItem{
					position: position,
					index:    i + 2,
					text:     text,
				}

				// 获取原始文本（如果有）
				if originalSrtMap != nil {
					if originalText, ok := originalSrtMap[position]; ok {
						item.originalText = originalText
					}
				}

				items = append(items, item)
			}
		}
	}

	if len(items) == 0 {
		return nil
	}

	// 使用并发进行校对
	var wg sync.WaitGroup
	var mu sync.Mutex
	errChan := make(chan error, len(items))

	for _, item := range items {
		wg.Add(1)
		go func(subItem subtitleItem) {
			defer wg.Done()

			correctedText, err := s.callLLM(subItem.text, subItem.originalText, sourceLanguage, targetLanguage, proofreadingTerms)
			if err != nil {
				s.log.Error(err)
				errChan <- err
				return
			}

			// 更新原始切片
			mu.Lock()
			srtContentSlice[subItem.index] = correctedText
			mu.Unlock()
		}(item)
	}

	wg.Wait()
	close(errChan)

	// 检查是否有错误
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// callLLM 调用 LLM 进行文本校对（包含中英比对）
func (s *subtitleProofreading) callLLM(text, originalText, sourceLanguage, targetLanguage string, proofreadingTerms map[string]string) (string, error) {
	if s.llm == nil {
		// 如果 LLM 未初始化，返回原文
		return text, nil
	}

	ctx := context.Background()

	// 构建消息
	var content []llms.MessageContent
	content = append(content, llms.TextParts(llms.ChatMessageTypeSystem, "你是一个专业的字幕校对和翻译审校助手。"))
	content = append(content, llms.TextParts(llms.ChatMessageTypeHuman, s.buildProofreadingPrompt(text, originalText, sourceLanguage, targetLanguage, proofreadingTerms)))

	// 调用 LLM
	var builder strings.Builder
	options := []llms.CallOption{
		llms.WithMaxTokens(s.conf.LLM.MaxTokens),
		llms.WithTemperature(s.conf.LLM.Temperature),
		llms.WithStreamingFunc(func(ctx context.Context, chunk []byte) error {
			builder.Write(chunk)
			return nil
		}),
	}

	_, err := s.llm.GenerateContent(ctx, content, options...)
	if err != nil {
		s.log.Error(fmt.Sprintf("LLM call failed: %v", err))
		return text, err
	}

	correctedText := strings.TrimSpace(builder.String())
	// 去除可能的前后引号
	correctedText = strings.Trim(correctedText, `"'`)
	if correctedText == "" {
		return text, nil
	}

	return correctedText, nil
}

// buildProofreadingPrompt 构建字幕校对的提示词（包含中英比对）
func (s *subtitleProofreading) buildProofreadingPrompt(text, originalText, sourceLanguage, targetLanguage string, proofreadingTerms map[string]string) string {
	var sourceLangName, targetLangName string

	switch sourceLanguage {
	case constants.LANG_ZH:
		sourceLangName = "中文"
	case constants.LANG_EN:
		sourceLangName = "英文"
	default:
		sourceLangName = "源语言"
	}

	switch targetLanguage {
	case constants.LANG_ZH:
		targetLangName = "中文"
	case constants.LANG_EN:
		targetLangName = "英文"
	default:
		targetLangName = "目标语言"
	}

	prompt := fmt.Sprintf(`请对以下%s字幕文本进行校对和优化：

`, targetLangName)

	// 如果有原始文本，添加中英比对说明
	if originalText != "" {
		prompt += fmt.Sprintf(`**原文（%s参考）**：
%s

`, sourceLangName, originalText)
	}

	prompt += fmt.Sprintf(`**当前译文（%s）**：
%s

`, targetLangName, text)

	// 添加专有名词纠正信息
	if len(proofreadingTerms) > 0 {
		prompt += `**专有名词纠正（ASR识别错误的词需要纠正）**：
`
		for wrong, correct := range proofreadingTerms {
			prompt += fmt.Sprintf("- %q 应纠正为 %q\n", wrong, correct)
		}
		prompt += "\n"
	}

	prompt += fmt.Sprintf(`**校对要求**：
1. 保持%s语言不变
2. 检查语法错误、拼写错误
3. 优化表达，使其更自然流畅
4. %s
5. 注意专有名词的正确性，参考上述纠正表
6. 只返回校对后的文本，不要添加任何解释或引号

校对后的文本：`, targetLangName, getComparisonRequirement(sourceLanguage, targetLanguage))

	return prompt
}

// getComparisonRequirement 根据源语言和目标语言生成比对要求
func getComparisonRequirement(sourceLanguage, targetLanguage string) string {
	if sourceLanguage == constants.LANG_ZH && targetLanguage == constants.LANG_EN {
		return "对比原文中文，确保英文翻译准确传达了原意，检查是否有漏译、误译"
	} else if sourceLanguage == constants.LANG_EN && targetLanguage == constants.LANG_ZH {
		return "对比原文英文，确保中文翻译准确传达了原意，检查是否有漏译、误译"
	} else {
		return "确保翻译准确传达了原意，检查是否有漏译、误译"
	}
}

// loadProofreadingTerms 从文件加载专有名词纠正表
// 支持 JSON、YAML 和简单的键值对格式
func loadProofreadingTerms(filePath string) map[string]string {
	if filePath == "" {
		// 没有配置纠错表文件，不是错误
		return nil
	}

	// 检查文件是否存在
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		log.Error(fmt.Sprintf("Proofreading terms file not found: %s", filePath))
		return nil
	}

	// 根据文件扩展名判断格式
	ext := strings.ToLower(path.Ext(filePath))

	var result map[string]string

	switch ext {
	case ".json":
		result = loadFromJSON(filePath)
	case ".yaml", ".yml":
		result = loadFromYAML(filePath)
	case ".txt", ".csv":
		result = loadFromText(filePath)
	default:
		// 尝试自动检测
		result = tryLoadFile(filePath)
	}

	if result == nil {
		log.Error(fmt.Sprintf("Failed to load proofreading terms from: %s", filePath))
		return nil
	}

	log.Info(fmt.Sprintf("Loaded %d proofreading terms from: %s", len(result), filePath))
	return result
}

// loadFromJSON 从 JSON 文件加载
func loadFromJSON(filePath string) map[string]string {
	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Error(err)
		return nil
	}

	var result map[string]string
	err = json.Unmarshal(data, &result)
	if err != nil {
		log.Error(err)
		return nil
	}

	return result
}

// loadFromYAML 从 YAML 文件加载
func loadFromYAML(filePath string) map[string]string {
	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Error(err)
		return nil
	}

	var result map[string]string
	err = yaml.Unmarshal(data, &result)
	if err != nil {
		log.Error(err)
		return nil
	}

	return result
}

// loadFromText 从文本文件加载（支持简单键值对格式）
// 格式：错误词=正确词 或 错误词:正确词
func loadFromText(filePath string) map[string]string {
	file, err := os.Open(filePath)
	if err != nil {
		log.Error(err)
		return nil
	}
	defer file.Close()

	result := make(map[string]string)
	// TODO: 实现文本解析
	_ = file // 暂时避免未使用警告
	return result
}

// tryLoadFile 尝试自动检测文件格式并加载
func tryLoadFile(filePath string) map[string]string {
	// 先尝试 JSON
	if result := loadFromJSON(filePath); result != nil {
		return result
	}
	// 再尝试 YAML
	if result := loadFromYAML(filePath); result != nil {
		return result
	}
	return nil
}

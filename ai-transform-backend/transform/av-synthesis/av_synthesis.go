package av_synthesis

import (
	_interface "ai-transform-backend/interface"
	"ai-transform-backend/message"
	"ai-transform-backend/pkg/config"
	"ai-transform-backend/pkg/constants"
	"ai-transform-backend/pkg/ffmpeg"
	"ai-transform-backend/pkg/log"
	"ai-transform-backend/pkg/mq/kafka"
	"ai-transform-backend/pkg/utils"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type avSynthesis struct {
	conf *config.Config
	log  log.ILogger
}

func NewAVSynthesis(conf *config.Config, log log.ILogger) _interface.ConsumerTask {
	return &avSynthesis{
		conf: conf,
		log:  log,
	}
}

func (t *avSynthesis) Start(ctx context.Context) {
	cfg := t.conf
	conf := &kafka.ConsumerGroupConfig{
		Config: kafka.Config{
			BrokerList:    cfg.Kafka.Address,
			User:          cfg.Kafka.User,
			Pwd:           cfg.Kafka.Pwd,
			SASLMechanism: cfg.Kafka.SaslMechanism,
			Version:       sarama.V3_7_0_0,
		},
		RetryConfig: kafka.DefaultRetryConfig(),
	}
	// 订阅原始 topic 和重试 topic
	topics := []string{
		constants.KAFKA_TOPIC_TRANSFORM_AV_SYNTHESIS,
		kafka.GetRetryTopic(constants.KAFKA_TOPIC_TRANSFORM_AV_SYNTHESIS),
	}
	cg := kafka.NewConsumerGroup(conf, t.log, t.messageHandleFunc)
	cg.Start(ctx, constants.KAFKA_TOPIC_TRANSFORM_AV_SYNTHESIS, topics)
}

func (t *avSynthesis) messageHandleFunc(consumerMessage *sarama.ConsumerMessage) error {
	fmt.Printf("av syn begin\n")

	avSynthesisMsg := &message.KafkaMsg{}
	err := json.Unmarshal(consumerMessage.Value, avSynthesisMsg)
	if err != nil {
		t.log.Error(err)
		return err
	}
	t.log.Debug(avSynthesisMsg)

	// 1. 读取 SRT 文件
	file, err := os.Open(avSynthesisMsg.TranslateSplitSrtPath)
	if err != nil {
		t.log.Error(err)
		return err
	}
	defer file.Close()
	srtContentBytes, err := io.ReadAll(file)
	if err != nil {
		t.log.Error(err)
		return err
	}
	srtContentSlice := strings.Split(string(srtContentBytes), "\n")

	sourceDir := fmt.Sprintf("%s/%s/%s", constants.MIDDLE_DIR, avSynthesisMsg.Filename, constants.AUDIOS_GENERATION_SUB_DIR)
	tmpOutputPath := fmt.Sprintf("%s/%s/%s", constants.MIDDLE_DIR, avSynthesisMsg.Filename, constants.TEMP_SUB_DIR)
	videoOutputPath := fmt.Sprintf("%s/%s/%s", constants.MIDDLE_DIR, avSynthesisMsg.Filename, constants.VIDEO_SUB_DIR)
	if err = utils.CreateDirIfNotExists(tmpOutputPath); err != nil {
		t.log.Error(err)
		return err
	}
	if err = utils.CreateDirIfNotExists(videoOutputPath); err != nil {
		t.log.Error(err)
		return err
	}

	// 2. 按SRT时间切分视频片段（含空白时长）
	videoSegments, err := t.splitVideoBySrt(avSynthesisMsg.ExtractVideoPath, srtContentSlice, videoOutputPath)
	if err != nil {
		t.log.Error(err)
		return err
	}

	// 3. 分组音频
	audioGroups := t.groupBySrt(srtContentSlice, sourceDir, "wav")

	// 4. 构建音频时长映射 (position -> 纯音频时长，不含空白)
	audioDurationMap := make(map[int]float64)
	for _, g := range audioGroups {
		for _, a := range g.Audios {
			duration, err := getDuration(t.log, a.AudioFile)
			if err != nil {
				t.log.Error(err)
				return err
			}
			audioDurationMap[a.Position] = duration
		}
	}

	var totalVideoDuration float64
	for _, seg := range videoSegments {
		totalVideoDuration += seg.Duration
	}
	fmt.Printf("[变速前总时长] 视频: %.3fs\n", totalVideoDuration)

	// 5. 并行对每个视频片段进行变速，目标时长 = 音频时长 + 前置空白时长
	adjustedVideoPaths, err := t.adjustAllSegments(videoSegments, audioDurationMap, videoOutputPath)
	if err != nil {
		t.log.Error(err)
		return err
	}

	// 6. 合并所有视频片段
	adjustedVideoPath := fmt.Sprintf("%s/%s/video_adjusted.mp4", constants.MIDDLE_DIR, avSynthesisMsg.Filename)
	if err = t.concatVideos(adjustedVideoPaths, adjustedVideoPath); err != nil {
		t.log.Error(err)
		return err
	}
	adjustedVideoDuration, _ := getDuration(t.log, adjustedVideoPath)
	fmt.Printf("[视频合并完成] 变速后总时长: %.3fs\n", adjustedVideoDuration)

	// 7. 合并音频
	audio, err := t.audioMerge(audioGroups, tmpOutputPath, "wav", "mp3")
	if err != nil {
		t.log.Error(err)
		return err
	}
	fmt.Printf("[音频合并完成] 总时长: %.3fs, ExpectEnd: %dms\n", float64(audio.ExpectEnd)/1000.0, audio.ExpectEnd)

	// 8. 合并音视频
	mergeVideo := fmt.Sprintf("%s/%s/%s/%s.mp4", constants.MIDDLE_DIR, avSynthesisMsg.Filename, constants.TEMP_SUB_DIR, avSynthesisMsg.Filename)
	if err = t.avMergeSimple(adjustedVideoPath, audio.AudioFile, mergeVideo); err != nil {
		t.log.Error(err)
		return err
	}

	// 输出合并前总时长对比
	videoDuration, _ := getDuration(t.log, adjustedVideoPath)
	audioDuration, _ := getDuration(t.log, audio.AudioFile)
	fmt.Printf("[音视频合并前时长对比] 视频: %.3fs, 音频: %.3fs, 差值: %.3fs\n", videoDuration, audioDuration, videoDuration-audioDuration)

	// 9. 生成调整后的字幕文件
	ext := path.Ext(avSynthesisMsg.TranslateSplitSrtPath)
	base := strings.TrimSuffix(avSynthesisMsg.TranslateSplitSrtPath, ext)
	srtAdjustFile := base + "_adjust" + ext
	if err = generateSrt(srtContentSlice, audioGroups, srtAdjustFile); err != nil {
		t.log.Error(err)
		return err
	}

	// 10. 添加字幕
	videoResultPath := fmt.Sprintf("%s/%s.mp4", constants.OUTPUTSDIR, avSynthesisMsg.Filename)
	if err = t.addSubtitles(mergeVideo, srtAdjustFile, videoResultPath); err != nil {
		t.log.Error(err)
		return err
	}

	// 11. 消息推送
	saveResultMsg := avSynthesisMsg
	saveResultMsg.OutPutFilePath = videoResultPath
	producer := kafka.GetProducer(kafka.Producer)
	value, err := json.Marshal(saveResultMsg)
	if err != nil {
		t.log.Error(err)
		return err
	}
	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: constants.KAFKA_TOPIC_TRANSFORM_SAVE_RESULT,
		Value: sarama.StringEncoder(value),
	})
	if err != nil {
		t.log.Error(err)
		return err
	}

	fmt.Printf("av syn end\n")

	return nil
}

// ========== 数据结构 ==========

type AudioGroup struct {
	Audios   []*Audio
	Position int
	// 本组第一个分片的开始时间（毫秒）
	ExpectStart int
	// 本组最后一个分片的结束时间（毫秒）
	ExpectEnd int
}

type Audio struct {
	AudioFile   string
	Position    int
	ExpectStart int // 毫秒
	ExpectEnd   int // 毫秒
}

// VideoSegment 视频片段结构
type VideoSegment struct {
	Position   int     // 片段序号
	StartMs    int     // 实际切分开始时间(毫秒)，含前置空白
	EndMs      int     // 实际切分结束时间(毫秒)，含片尾空白
	GapMs      int     // 前置空白时长(毫秒) = SrtStartMs - StartMs
	SrtStartMs int     // 原SRT开始时间(毫秒)
	SrtEndMs   int     // 原SRT结束时间(毫秒)
	FilePath   string  // 切分后的文件路径
	Duration   float64 // 视频实际时长(秒)
}

// ========== 视频切分 ==========

// splitVideoBySrt 按SRT时间切分视频，空白时间并入下一片段开头，片尾空白并入最后片段
func (t *avSynthesis) splitVideoBySrt(videoPath string, srtContentSlice []string, outputDir string) ([]*VideoSegment, error) {
	type srtEntry struct {
		position int
		startMs  int
		endMs    int
	}

	// 解析所有SRT条目
	entries := make([]srtEntry, 0)
	for i := 0; i < len(srtContentSlice); i += 4 {
		if i+1 >= len(srtContentSlice) {
			break
		}
		position, err := strconv.Atoi(srtContentSlice[i])
		if err != nil {
			return nil, fmt.Errorf("invalid srt position at line %d: %w", i, err)
		}
		startMs, endMs := utils.GetSrtTime(srtContentSlice[i+1])
		entries = append(entries, srtEntry{position, startMs, endMs})
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("no srt entries found")
	}

	// 获取视频总时长，用于处理片尾空白
	totalDuration, err := getDuration(t.log, videoPath)
	if err != nil {
		return nil, fmt.Errorf("get video duration failed: %w", err)
	}
	totalMs := int(totalDuration * 1000)

	// 计算每个片段的实际切分区间
	// 规则：
	//   片段1: srtStart[0] → srtEnd[0]  （片头空白丢弃）
	//   片段N: srtEnd[N-2] → srtEnd[N-1] （空白并入下一片段开头）
	//   最后一片: srtEnd[last-1] → totalMs （片尾空白并入最后片段）
	type cutEntry struct {
		position int
		cutStart int // 实际切分开始（含前置空白）
		cutEnd   int // 实际切分结束（含片尾空白）
		srtStart int // 原SRT开始
		srtEnd   int // 原SRT结束
	}

	cuts := make([]cutEntry, len(entries))
	for idx, e := range entries {
		cutStart := e.startMs
		if idx > 0 {
			cutStart = entries[idx-1].endMs
		}
		cutEnd := e.endMs
		if idx == len(entries)-1 && totalMs > e.endMs {
			cutEnd = totalMs
			t.log.Info(fmt.Sprintf("[片尾空白] %.3fs 并入最后片段 %d", float64(totalMs-e.endMs)/1000.0, e.position))
		}
		cuts[idx] = cutEntry{e.position, cutStart, cutEnd, e.startMs, e.endMs}
	}

	// 并行切分
	type result struct {
		seg *VideoSegment
		err error
	}
	resultChan := make(chan result, len(cuts))
	sem := make(chan struct{}, 4)

	for _, c := range cuts {
		c := c
		go func() {
			sem <- struct{}{}
			defer func() { <-sem }()

			duration := float64(c.cutEnd-c.cutStart) / 1000.0
			outputFile := fmt.Sprintf("%s/video_%d.mp4", outputDir, c.position)

			cmd := exec.Command(ffmpeg.FFmpeg,
				"-ss", fmt.Sprintf("%.3f", float64(c.cutStart)/1000.0),
				"-i", videoPath,
				"-t", fmt.Sprintf("%.3f", duration),
				"-c:v", "libx264",
				"-preset", "fast",
				"-an",
				"-y",
				outputFile,
			)
			t.log.Debug(cmd.String())

			if out, err := cmd.CombinedOutput(); err != nil {
				resultChan <- result{nil, fmt.Errorf("split segment %d failed: %w, ffmpeg: %s", c.position, err, string(out))}
				return
			}

			actualDuration, err := getDuration(t.log, outputFile)
			if err != nil {
				resultChan <- result{nil, fmt.Errorf("get duration of segment %d failed: %w", c.position, err)}
				return
			}

			gapMs := c.srtStart - c.cutStart
			t.log.Info(fmt.Sprintf("[片段 %d] cut: %dms→%dms, srt: %dms→%dms, gap: %dms, 实际时长: %.3fs",
				c.position, c.cutStart, c.cutEnd, c.srtStart, c.srtEnd, gapMs, actualDuration))

			resultChan <- result{&VideoSegment{
				Position:   c.position,
				StartMs:    c.cutStart,
				EndMs:      c.cutEnd,
				GapMs:      gapMs,
				SrtStartMs: c.srtStart,
				SrtEndMs:   c.srtEnd,
				FilePath:   outputFile,
				Duration:   actualDuration,
			}, nil}
		}()
	}

	segments := make([]*VideoSegment, len(cuts))
	for range cuts {
		r := <-resultChan
		if r.err != nil {
			return nil, r.err
		}
		segments[r.seg.Position-1] = r.seg
	}
	return segments, nil
}

// ========== 视频变速 ==========

const (
	minSpeedFactor = 0.25
	maxSpeedFactor = 4.0
)

// adjustAllSegments 并行调整所有片段速度
func (t *avSynthesis) adjustAllSegments(videoSegments []*VideoSegment, audioDurationMap map[int]float64, videoOutputPath string) ([]string, error) {
	type result struct {
		position int
		path     string
		err      error
	}

	resultChan := make(chan result, len(videoSegments))
	sem := make(chan struct{}, 4)

	for _, seg := range videoSegments {
		seg := seg
		audioDuration, ok := audioDurationMap[seg.Position]
		if !ok {
			return nil, fmt.Errorf("audio not found for position %d", seg.Position)
		}
		go func() {
			sem <- struct{}{}
			defer func() { <-sem }()

			// 目标时长 = 纯音频时长 + 本片段包含的前置空白时长
			// 这样视频变速后与音频轨（含adelay空白）完全对齐
			targetDuration := audioDuration + float64(seg.GapMs)/1000.0
			adjustedPath, err := t.adjustVideoSegment(seg, targetDuration, videoOutputPath)
			resultChan <- result{seg.Position, adjustedPath, err}
		}()
	}

	adjustedPaths := make([]string, len(videoSegments))
	for range videoSegments {
		r := <-resultChan
		if r.err != nil {
			return nil, r.err
		}
		adjustedPaths[r.position-1] = r.path
	}
	return adjustedPaths, nil
}

// adjustVideoSegment 调整单个视频片段速度以匹配目标时长
func (t *avSynthesis) adjustVideoSegment(segment *VideoSegment, targetDuration float64, outputDir string) (string, error) {
	if segment.Duration <= 0 {
		return "", fmt.Errorf("segment %d has invalid duration: %.3f", segment.Position, segment.Duration)
	}

	factor := targetDuration / segment.Duration

	// 限制变速范围，防止画面过快/过慢
	clamped := factor
	if clamped < minSpeedFactor {
		clamped = minSpeedFactor
	} else if clamped > maxSpeedFactor {
		clamped = maxSpeedFactor
	}
	if clamped != factor {
		t.log.Warning(fmt.Sprintf("[片段 %d] 变速因子 %.4f 超出范围 [%.2f, %.2f]，已限制为 %.4f",
			segment.Position, factor, minSpeedFactor, maxSpeedFactor, clamped))
	}

	t.log.Info(fmt.Sprintf("[片段 %d] 视频: %.3fs(gap %.3fs), 目标: %.3fs, factor: %.4f",
		segment.Position, segment.Duration, float64(segment.GapMs)/1000.0, targetDuration, clamped))

	ext := path.Ext(segment.FilePath)
	base := strings.TrimSuffix(segment.FilePath, ext)
	outputFile := fmt.Sprintf("%s_adj%s", base, ext)

	cmd := exec.Command(ffmpeg.FFmpeg,
		"-i", segment.FilePath,
		"-filter:v", fmt.Sprintf("setpts=%.6f*PTS", clamped),
		"-c:v", "libx264",
		"-preset", "fast",
		"-an",
		"-y",
		outputFile,
	)
	t.log.Debug(cmd.String())

	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("adjust segment %d failed: %w, ffmpeg: %s", segment.Position, err, string(out))
	}

	adjustedDuration, err := getDuration(t.log, outputFile)
	if err != nil {
		return "", err
	}
	t.log.Info(fmt.Sprintf("[片段 %d] 变速后: %.3fs, 与目标差值: %.3fs",
		segment.Position, adjustedDuration, adjustedDuration-targetDuration))

	return outputFile, nil
}

// ========== 音频处理 ==========

func (t *avSynthesis) groupBySrt(srtContentSlice []string, rootDir, format string) []*AudioGroup {
	minDuration := 2 * 60 * 1000
	groups := make([]*AudioGroup, 0)
	tmpGroup := &AudioGroup{
		Audios:   []*Audio{},
		Position: 1,
	}
	groups = append(groups, tmpGroup)

	accumulatedEnd := 0    // 累积的音频结束时间（含空白）
	accumulatedSrtEnd := 0 // 累积的SRT结束时间

	for i := 0; i < len(srtContentSlice); i += 4 {
		srtStart, srtEnd := utils.GetSrtTime(srtContentSlice[i+1])
		position, _ := strconv.Atoi(srtContentSlice[i])
		file := fmt.Sprintf("%s/%s.%s", rootDir, srtContentSlice[i], format)

		actualDuration, _ := getDuration(t.log, file)
		actualMs := int(actualDuration * 1000)

		// 计算前置空白 = 当前SRT开始 - 上一个SRT结束
		gapMs := 0
		if i > 0 {
			gapMs = srtStart - accumulatedSrtEnd
		}

		var start, end int
		if i == 0 {
			start = srtStart
			accumulatedEnd = start
		} else {
			// 音频开始时间 = 上一个音频结束时间 + 前置空白
			start = accumulatedEnd + gapMs
		}
		end = start + actualMs
		accumulatedEnd = end
		accumulatedSrtEnd = srtEnd

		t.log.Info(fmt.Sprintf("[音频 %d] SRT: %dms→%dms, 音频时长: %dms, gap: %dms, ExpectStart: %dms, ExpectEnd: %dms",
			position, srtStart, srtEnd, actualMs, gapMs, start, end))

		a := &Audio{
			ExpectEnd:   end,
			ExpectStart: start,
			Position:    position,
			AudioFile:   file,
		}
		if i == 0 {
			tmpGroup.ExpectStart = a.ExpectStart
		}
		if end-tmpGroup.ExpectStart < minDuration {
			tmpGroup.Audios = append(tmpGroup.Audios, a)
			tmpGroup.ExpectEnd = end
		} else {
			nextPosition := tmpGroup.Position + 1
			tmpGroup = &AudioGroup{
				Audios:   []*Audio{},
				Position: nextPosition,
			}
			groups = append(groups, tmpGroup)
			tmpGroup.Audios = append(tmpGroup.Audios, a)
			tmpGroup.ExpectStart = a.ExpectStart
			tmpGroup.ExpectEnd = a.ExpectEnd
		}
	}
	return groups
}

func (t *avSynthesis) audioMerge(groups []*AudioGroup, tmpDir, tmpFormat, lastFormat string) (*Audio, error) {
	errChan := make(chan error, len(groups))
	audioChan := make(chan *Audio, len(groups))
	wg := sync.WaitGroup{}
	for _, g := range groups {
		wg.Add(1)
		go func(group *AudioGroup) {
			defer wg.Done()
			a, e := t.audioGroupMerge(group, 0, tmpDir, tmpFormat)
			audioChan <- a
			errChan <- e
		}(g)
	}
	wg.Wait()
	close(errChan)
	close(audioChan)

	for err := range errChan {
		if err != nil {
			t.log.Error(err)
			return nil, err
		}
	}

	group1 := &AudioGroup{
		ExpectStart: 0,
		ExpectEnd:   groups[len(groups)-1].ExpectEnd,
		Position:    1,
		Audios:      make([]*Audio, len(groups)),
	}
	for a := range audioChan {
		group1.Audios[a.Position-1] = a
	}
	audio, err := t.audioGroupMerge(group1, 1, tmpDir, lastFormat)
	if err != nil {
		t.log.Error(err)
		return nil, err
	}
	return audio, err
}

func (t *avSynthesis) audioGroupMerge(g *AudioGroup, level int, outputPath string, format string) (*Audio, error) {
	errChan := make(chan error, len(g.Audios)+1)
	silenceFile := fmt.Sprintf("%s/group_%d_level_%d_silence.wav", outputPath, g.Position, level)
	groupFile := fmt.Sprintf("%s/group_%d_level_%d.%s", outputPath, g.Position, level, format)

	inputArgs := []string{"-i", silenceFile}
	avolumes := []string{"[0:a]volume=1[0]"}
	audios := []string{"[0]"}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := t.createSilence(g.ExpectEnd-g.ExpectStart, silenceFile); err != nil {
			t.log.Error(err)
			errChan <- err
		}
	}()

	for i, a := range g.Audios {
		avolumes = append(avolumes, fmt.Sprintf("[%d:a]volume=%d[%d]", i+1, len(g.Audios)-i+1, i+1))
		audios = append(audios, fmt.Sprintf("[%d]", i+1))

		adelay := a.ExpectStart - g.ExpectStart
		input := a.AudioFile
		adelayFile := fmt.Sprintf("%s/%s_%s", outputPath, "adelay", path.Base(input))
		inputArgs = append(inputArgs, "-i", adelayFile)

		wg.Add(1)
		go func(adelay int, input, output string) {
			defer wg.Done()
			if err := t.audioDelay(adelay, input, output); err != nil {
				errChan <- err
			}
		}(adelay, input, adelayFile)
	}

	inputArgs = append(inputArgs,
		"-filter_complex", fmt.Sprintf("%s;%samix=inputs=%d:duration=first[a]",
			strings.Join(avolumes, ";"), strings.Join(audios, ""), len(g.Audios)+1),
		"-map", "[a]",
		"-f", format, groupFile,
	)

	wg.Wait()
	close(errChan)
	for err := range errChan {
		if err != nil {
			t.log.Error(err)
			return nil, err
		}
	}

	cmd := exec.Command(ffmpeg.FFmpeg, inputArgs...)
	t.log.Debug(cmd.String())
	if err := cmd.Run(); err != nil {
		t.log.Error(err)
		return nil, err
	}
	return &Audio{
		ExpectStart: g.ExpectStart,
		ExpectEnd:   g.ExpectEnd,
		Position:    g.Position,
		AudioFile:   groupFile,
	}, nil
}

func (t *avSynthesis) createSilence(duration int, output string) error {
	i := 0
retry:
	cmd := exec.Command(ffmpeg.FFmpeg,
		"-f", "lavfi",
		"-i", "anullsrc=r=44100:cl=mono",
		"-t", fmt.Sprintf("%dms", duration),
		output,
	)
	t.log.Debug(cmd.String())
	err := cmd.Run()
	if err != nil && i < 3 {
		i++
		<-time.After(time.Millisecond * 500)
		goto retry
	}
	return err
}

func (t *avSynthesis) audioDelay(adelay int, input, output string) error {
	i := 0
retry:
	cmd := exec.Command(ffmpeg.FFmpeg, "-i", input, "-af", fmt.Sprintf("adelay=%d", adelay), output)
	t.log.Debug(cmd.String())
	err := cmd.Run()
	if err != nil && i < 3 {
		i++
		<-time.After(time.Millisecond * 500)
		goto retry
	}
	return err
}

// ========== 视频合并 ==========

// concatVideos 合并视频片段
func (t *avSynthesis) concatVideos(videoPaths []string, outputPath string) error {
	// 构建 -i 参数
	args := make([]string, 0, len(videoPaths)*2+4)
	for _, p := range videoPaths {
		args = append(args, "-i", p)
	}

	// 构建 filter_complex: [0:v][1:v][2:v]...concat=n=N:v=1:a=0[v]
	filterInputs := ""
	for i := range videoPaths {
		filterInputs += fmt.Sprintf("[%d:v]", i)
	}
	filter := fmt.Sprintf("%sconcat=n=%d:v=1:a=0[v]", filterInputs, len(videoPaths))

	args = append(args,
		"-filter_complex", filter,
		"-map", "[v]",
		"-c:v", "libx264",
		"-preset", "fast",
		"-y",
		outputPath,
	)

	cmd := exec.Command(ffmpeg.FFmpeg, args...)
	t.log.Debug(cmd.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("concat videos failed: %w, ffmpeg: %s", err, string(out))
	}
	return nil
}

// avMergeSimple 合并音视频
func (t *avSynthesis) avMergeSimple(videoPath, audioPath, output string) error {
	i := 0
retry:
	cmd := exec.Command(ffmpeg.FFmpeg,
		"-i", videoPath,
		"-i", audioPath,
		"-c:v", "copy",
		"-c:a", "copy",
		output,
	)
	t.log.Debug(cmd.String())
	err := cmd.Run()
	if err != nil && i < 3 {
		i++
		<-time.After(time.Millisecond * 500)
		goto retry
	}
	return err
}

func (t *avSynthesis) addSubtitles(videoPath, srtPath, output string) error {
	i := 0
retry:
	cmd := exec.Command(ffmpeg.FFmpeg, "-i", videoPath, "-vf", fmt.Sprintf("subtitles=%s", srtPath), output)
	t.log.Debug(cmd.String())
	err := cmd.Run()
	if err != nil && i < 3 {
		i++
		<-time.After(time.Millisecond * 500)
		goto retry
	}
	return err
}

// avMerge 带整体变速的音视频合并（暂未使用，保留备用）
func (t *avSynthesis) avMerge(videoPath, audioPath, output string) error {
	videoDuration, _ := getDuration(t.log, videoPath)
	audioDuration, _ := getDuration(t.log, audioPath)
	factor := audioDuration / videoDuration

	outputVideoPath, err := SlowVideo(videoPath, factor)
	if err != nil {
		t.log.Error(err)
		return err
	}

	i := 0
retry:
	cmd := exec.Command(ffmpeg.FFmpeg,
		"-i", outputVideoPath,
		"-i", audioPath,
		"-c:v", "copy",
		"-c:a", "copy",
		output,
	)
	t.log.Debug(cmd.String())
	err = cmd.Run()
	if err != nil && i < 3 {
		i++
		<-time.After(time.Millisecond * 500)
		goto retry
	}
	return err
}

// ========== 工具函数 ==========

func getDuration(log log.ILogger, filePath string) (float64, error) {
	cmd := exec.Command(ffmpeg.FFprobe,
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		filePath,
	)
	output, err := cmd.Output()
	if err != nil {
		log.Error(err)
		return 0, err
	}
	var duration float64
	fmt.Sscanf(string(output), "%f", &duration)
	return duration, nil
}

func SlowVideo(inputFile string, factor float64) (string, error) {
	ext := path.Ext(inputFile)
	base := strings.TrimSuffix(inputFile, ext)
	outputFile := base + "_slow" + ext

	cmd := exec.Command(ffmpeg.FFmpeg,
		"-i", inputFile,
		"-filter:v", fmt.Sprintf("setpts=%.6f*PTS", factor),
		"-filter:a", "atempo=0.5",
		outputFile,
	)
	err := cmd.Run()
	if err != nil {
		log.Error(err)
		return inputFile, err
	}
	return outputFile, err
}

func generateSrt(srtContentSlice []string, audioGroups []*AudioGroup, output string) error {
	audioMap := make(map[int]*Audio)
	for _, g := range audioGroups {
		for _, a := range g.Audios {
			audioMap[a.Position] = a
		}
	}

	newSrtSlice := make([]string, 0, len(srtContentSlice))
	for i := 0; i < len(srtContentSlice); i += 4 {
		if i+3 > len(srtContentSlice) {
			break
		}
		positionStr := srtContentSlice[i]
		text := srtContentSlice[i+2]

		position, err := strconv.Atoi(positionStr)
		if err != nil {
			return fmt.Errorf("invalid srt position at line %d: %w", i, err)
		}
		a, ok := audioMap[position]
		if !ok {
			return fmt.Errorf("audio not found for position %d", position)
		}

		newSrtSlice = append(newSrtSlice,
			positionStr,
			utils.BuildStrItemTimeStr(a.ExpectStart, a.ExpectEnd),
			text,
			"",
		)
	}

	return utils.SaveSrt(newSrtSlice, output)
}

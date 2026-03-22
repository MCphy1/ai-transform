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
	}
	cg := kafka.NewConsumerGroup(conf, t.log, t.messageHandleFunc)
	cg.Start(ctx, constants.KAFKA_TOPIC_TRANSFORM_AV_SYNTHESIS, []string{constants.KAFKA_TOPIC_TRANSFORM_AV_SYNTHESIS})
}
func (t *avSynthesis) messageHandleFunc(consumerMessage *sarama.ConsumerMessage) error {
	// fmt.Printf("av synthesis begin\n")

	avSynthesisMsg := &message.KafkaMsg{}
	err := json.Unmarshal(consumerMessage.Value, avSynthesisMsg)
	if err != nil {
		t.log.Error(err)
		return err
	}
	t.log.Debug(avSynthesisMsg)
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
	err = utils.CreateDirIfNotExists(tmpOutputPath)
	audioGroups := t.groupBySrt(srtContentSlice, sourceDir, "wav")

	// fmt.Printf("avSynthesisMsg.ExtractVideoPath is : %s\n", avSynthesisMsg.ExtractVideoPath)

	// fmt.Printf("avSynthesisMsg.TranslateSplitSrtPath is: %s\n", avSynthesisMsg.TranslateSplitSrtPath)
	ext := path.Ext(avSynthesisMsg.TranslateSplitSrtPath)
	base := strings.TrimSuffix(avSynthesisMsg.TranslateSplitSrtPath, ext)
	srt_adjust_File := base + "_adjust" + ext
	err = generateSrt(srtContentSlice, audioGroups, srt_adjust_File)
	if err != nil {
		t.log.Error(err)
		return err
	}

	// // 视频播放减慢
	// err = t.SlowVideo(audioGroups, srtContentSlice, avSynthesisMsg.ExtractVideoPath)
	// if err != nil {
	// 	t.log.Error(err)
	// 	return err
	// }

	// 合并音频
	audio, err := t.audioMerge(audioGroups, tmpOutputPath, "wav", "mp3")
	if err != nil {
		t.log.Error(err)
		return err
	}
	//合并音视频
	mergeVideo := fmt.Sprintf("%s/%s/%s/%s.mp4", constants.MIDDLE_DIR, avSynthesisMsg.Filename, constants.TEMP_SUB_DIR, avSynthesisMsg.Filename)
	err = t.avMerge(avSynthesisMsg.ExtractVideoPath, audio.AudioFile, mergeVideo)
	if err != nil {
		t.log.Error(err)
		return err
	}
	// 添加字幕
	videoResultPath := fmt.Sprintf("%s/%s.mp4", constants.OUTPUTSDIR, avSynthesisMsg.Filename)
	// err = t.addSubtitles(mergeVideo, avSynthesisMsg.TranslateSplitSrtPath, videoResultPath)
	err = t.addSubtitles(mergeVideo, srt_adjust_File, videoResultPath)
	if err != nil {
		t.log.Error(err)
		return err
	}
	// 消息推送
	saveResultMsg := avSynthesisMsg
	saveResultMsg.OutPutFilePath = videoResultPath
	producer := kafka.GetProducer(kafka.Producer)
	value, err := json.Marshal(saveResultMsg)
	if err != nil {
		t.log.Error(err)
		return err
	}
	msg := &sarama.ProducerMessage{
		Topic: constants.KAFKA_TOPIC_TRANSFORM_SAVE_RESULT,
		Value: sarama.StringEncoder(value),
	}
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		t.log.Error(err)
		return err
	}

	// fmt.Printf("av synthesis end\n")

	return nil
}

type AudioGroup struct {
	Audios   []*Audio
	Position int
	// 预期开始的毫秒数，本组第一个分片的开始时间，即中文原视频的开始时间
	ExpectStart int
	// 预期结束的毫秒数，本组最后一个分片的结束时间，即中文原视频的结束时间
	ExpectEnd int
}
type Audio struct {
	AudioFile string
	Position  int
	// 旧代码：预期开始的毫秒数，srt里的的开始时间，即中文原视频的开始时间
	ExpectStart int
	// 旧代码：预期结束的毫秒数，srt里的的结束时间，即中文原视频的结束时间
	ExpectEnd int
}

func (t *avSynthesis) groupBySrt(srtContentSlice []string, rootDir, format string) []*AudioGroup {
	minDuration := 2 * 60 * 1000
	groups := make([]*AudioGroup, 0)
	tmpGroup := &AudioGroup{
		Audios:   []*Audio{},
		Position: 1,
	}
	groups = append(groups, tmpGroup)

	// 根据生成音频的实际时长累计计算
	accumulatedEnd := 0

	for i := 0; i < len(srtContentSlice); i += 4 {
		// srtStart, srtEnd := utils.GetSrtTime(srtContentSlice[i+1])
		srtStart, _ := utils.GetSrtTime(srtContentSlice[i+1])
		position, _ := strconv.Atoi(srtContentSlice[i])
		file := fmt.Sprintf("%s/%s.%s", rootDir, srtContentSlice[i], format)

		// 获取实际音频时长
		actualDuration, _ := getDuration(t.log, file)
		actualMs := int(actualDuration * 1000)
		// 计算实际的 start 和 end
		var start, end int
		if i == 0 {
			// 第一个片段：使用 SRT 的 start
			start = srtStart
			accumulatedEnd = start
		} else {
			// 后续片段：使用累积的结束时间
			start = accumulatedEnd
		}
		end = start + actualMs
		accumulatedEnd = end

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
		ExpectStart: 0, //groups[0].ExpectStart,
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
	inputArgs := []string{
		"-i", silenceFile,
	}
	avolumes := []string{
		"[0:a]volume=1[0]",
	}
	audios := []string{
		"[0]",
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := t.createSilence(g.ExpectEnd-g.ExpectStart, silenceFile)
		if err != nil {
			t.log.Error(err)
			errChan <- err
		}
	}()

	for i, a := range g.Audios {

		avolumes = append(avolumes, fmt.Sprintf("[%d:a]volume=%d[%d]", i+1, len(g.Audios)-i+1, i+1))
		audios = append(audios, fmt.Sprintf("[%d]", i+1))

		// 原a.ExpectStart是根据srt的原视频时间计算，现在是根据生成音频的实际时长计算
		adelay := a.ExpectStart - g.ExpectStart

		input := a.AudioFile
		adelayFile := fmt.Sprintf("%s/%s_%s", outputPath, "adelay", path.Base(input))
		inputArgs = append(inputArgs, "-i", adelayFile)
		wg.Add(1)
		go func(adelay int, input, output string) {
			defer wg.Done()
			err := t.audioDelay(adelay, input, output)
			if err != nil {
				errChan <- err
			}
		}(adelay, input, adelayFile)
	}
	inputArgs = append(inputArgs, "-filter_complex", fmt.Sprintf("%s;%samix=inputs=%d:duration=first[a]", strings.Join(avolumes, ";"), strings.Join(audios, ""), len(g.Audios)+1))
	inputArgs = append(inputArgs, "-map", "[a]")
	inputArgs = append(inputArgs, "-f", format, groupFile)
	wg.Wait()
	close(errChan)
	for err := range errChan {
		if err != nil {
			t.log.Error(err)
			return nil, err
		}
	}
	cmd := exec.Command(ffmpeg.FFmpeg, inputArgs...)
	log.Debug(cmd.String())
	err := cmd.Run()
	if err != nil {
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
	cmd := exec.Command(ffmpeg.FFmpeg, "-f", "lavfi", "-i", "anullsrc=r=44100:cl=mono", "-t", fmt.Sprintf("%dms", duration), output)
	log.Debug(cmd.String())
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
	log.Debug(cmd.String())
	err := cmd.Run()
	if err != nil && i < 3 {
		i++
		<-time.After(time.Millisecond * 500)
		goto retry
	}
	return err
}
func (t *avSynthesis) avMerge(videoPath, audioPath, output string) error {

	// debug
	// 获取时长信息
	videoDuration, _ := getDuration(t.log, videoPath)
	audioDuration, _ := getDuration(t.log, audioPath)
	// fmt.Printf("变速前: \n视频时长: %.2f秒, 音频时长: %.2f秒, 差值: %.2f秒\n", videoDuration, audioDuration, audioDuration-videoDuration)

	factor := float64(audioDuration) / float64(videoDuration)
	outputVideoPath, err := SlowVideo(videoPath, factor)
	if err != nil {
		t.log.Error(err)
		return err
	}

	// videoDuration, _ = getDuration(t.log, outputVideoPath)
	// audioDuration, _ = getDuration(t.log, audioPath)
	// fmt.Printf("变速后: \n视频时长: %.2f秒, 音频时长: %.2f秒, 差值: %.2f秒\n", videoDuration, audioDuration, audioDuration-videoDuration)

	i := 0
retry:
	cmd := exec.Command(ffmpeg.FFmpeg, "-i", outputVideoPath, "-i", audioPath, "-c:v", "copy", "-c:a", "copy", output)
	log.Debug(cmd.String())
	err = cmd.Run()
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
	log.Debug(cmd.String())
	err := cmd.Run()
	if err != nil && i < 3 {
		i++
		<-time.After(time.Millisecond * 500)
		goto retry
	}
	return err
}

func getDuration(log log.ILogger, filePath string) (float64, error) {
	cmd := exec.Command(ffmpeg.FFprobe, "-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		filePath)
	output, err := cmd.Output()
	if err != nil {
		log.Error(err)
		return 0, err
	}
	var duration float64
	fmt.Sscanf(string(output), "%f", &duration)
	return duration, nil // 返回毫秒
}

func SlowVideo(inputFile string, factor float64) (string, error) {
	// 生成输出文件名
	ext := path.Ext(inputFile)
	base := strings.TrimSuffix(inputFile, ext)
	outputFile := base + "_slow" + ext

	cmd := exec.Command(ffmpeg.FFmpeg, "-i", inputFile,
		"-filter:v", fmt.Sprintf("setpts=%.6f*PTS", factor),
		"-filter:a", "atempo=0.5",
		outputFile)

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

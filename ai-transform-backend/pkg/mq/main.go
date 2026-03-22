package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"192.168.179.134:29092", "192.168.179.134:39092", "192.168.179.134:49092"}
	dlqTopics := []string{
		"dlq-asr",
		"dlq-audio-generation",
		"dlq-av-extract",
		"dlq-av-synthesis",
		"dlq-entry",
		"dlq-refer-wav",
		"dlq-save-result",
		"dlq-translate",
	}

	config := sarama.NewConfig()
	config.Version = sarama.V3_7_0_0
	config.Net.SASL.Enable = true
	config.Net.SASL.User = "admin"
	config.Net.SASL.Password = "123456"
	config.Net.SASL.Mechanism = sarama.SASLMechanism("PLAIN")

	// 从最早的消息开始读
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		fmt.Printf("创建 consumer 失败: %v\n", err)
		os.Exit(1)
	}
	defer consumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	msgChan := make(chan *sarama.ConsumerMessage, 100)

	fmt.Println("=== 开始消费所有 DLQ Topics ===")
	for _, topic := range dlqTopics {
		fmt.Printf("Topic: %s\n", topic)

		// 获取 topic 的所有 partition
		partitions, err := consumer.Partitions(topic)
		if err != nil {
			fmt.Printf("  获取 partition 失败: %v\n", err)
			continue
		}

		// 每个 partition 起一个 goroutine 消费
		for _, partition := range partitions {
			pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
			if err != nil {
				fmt.Printf("  消费 partition %d 失败: %v\n", partition, err)
				continue
			}
			defer pc.Close()

			go func(pc sarama.PartitionConsumer) {
				for msg := range pc.Messages() {
					msgChan <- msg
				}
			}(pc)
		}
	}
	fmt.Println("=== 等待消息... (Ctrl+C 退出) ===\n")

	// 打印消息
	for {
		select {
		case msg := <-msgChan:
			printDLQMessage(msg)
		case <-signals:
			fmt.Println("\n退出")
			return
		}
	}
}

func printDLQMessage(msg *sarama.ConsumerMessage) {
	fmt.Println("─────────────────────────────────────")
	fmt.Printf("DLQ Topic : %s\n", msg.Topic)           // 来自哪个 DLQ
	fmt.Printf("Partition : %d\n", msg.Partition)
	fmt.Printf("Offset    : %d\n", msg.Offset)
	fmt.Printf("Key       : %s\n", string(msg.Key))
	fmt.Printf("Value     : %s\n", string(msg.Value))
	fmt.Printf("Timestamp : %s\n", msg.Timestamp.Format(time.RFC3339))

	// 解析附加的 Header 信息
	fmt.Println("Headers   :")
	for _, h := range msg.Headers {
		if h == nil {
			continue
		}
		fmt.Printf("  %-20s = %s\n", string(h.Key), string(h.Value))
	}
	// 输出示例：
	//   DLQ Topic            : dlq-asr
	//   original-topic       = transform-asr
	//   original-offset      = 12345
	//   failed-at            = 2024-01-15T08:30:00Z
	fmt.Println()
}

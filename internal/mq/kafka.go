package mq

import (
	"context"
	"encoding/json"
	"log"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	writer *kafka.Writer
	reader *kafka.Reader

	// 本地降级模式（Kafka不可用时启用，消息静默丢弃并计数）
	available     bool
	droppedCend   atomic.Int64
	droppedIoT    atomic.Int64
)

// 初始化Kafka；失败时自动降级（消息丢弃模式），保证网关可单机运行
func Init(brokers []string, topic string, groupID string) error {
	// 快速探测broker可用性（2秒超时）
	probeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	probeConn, err := kafka.DialContext(probeCtx, "tcp", brokers[0])
	if err != nil {
		log.Printf("Kafka unavailable (%v), falling back to drop mode", err)
		available = false
		return nil
	}
	_ = probeConn.Close()

	// 创建Writer
	writer = &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
		Compression:  kafka.Snappy,
	}

	// 创建Reader
	reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	})

	available = true
	log.Printf("Connected to Kafka: %v, topic: %s", brokers, topic)
	return nil
}

// 是否可用
func IsAvailable() bool {
	return available
}

// 发布C端消息
func PublishCendMessage(message interface{}) error {
	if !available {
		droppedCend.Add(1)
		return nil
	}

	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("cend"),
		Value: data,
	})
}

// 发布IoT事件
func PublishIoTEvent(event interface{}) error {
	if !available {
		droppedIoT.Add(1)
		return nil
	}

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte("iot"),
		Value: data,
	})
}

// 消费消息
func ConsumeMessages(handler func([]byte)) {
	if !available {
		log.Printf("Kafka unavailable: message consumption disabled")
		return
	}

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Failed to read message: %v", err)
			time.Sleep(time.Second)
			continue
		}

		handler(msg.Value)
	}
}

// 关闭Kafka连接
func Close() {
	if writer != nil {
		writer.Close()
	}
	if reader != nil {
		reader.Close()
	}
}

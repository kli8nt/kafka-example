package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	partition = 0
)

type KConfig struct {
	host string
	port string
	deadline int 
	topic string
}

type Kf struct {
	config KConfig
	cleanups []func()
}

type KReader struct {
	topic string
	connection *kafka.Reader
}

type KWriter struct {
	topic string
	connection *kafka.Conn
}


func onError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func (kf *Kf) Init(config KConfig) {
	if config.topic == "" {
		panic("Topic is required")
	}
	kf.config = config
}

func (kf *Kf) Writer() (KWriter) {
	topic := kf.config.topic
	uri := fmt.Sprintf("%s:%s", kf.config.host, kf.config.port)
	connection, err := kafka.DialLeader(context.Background(), "tcp", uri, topic, partition)
	connection.SetWriteDeadline(time.Now().Add(time.Duration(kf.config.deadline)*time.Second))
	onError(err, "failed to dial Writer")
	kWriter := KWriter{connection: connection, topic: topic}
	kf.cleanups = append(kf.cleanups, func() {
		kWriter.connection.Close()
	})
	return kWriter
}

func (kf *Kf) Reader() (KReader) {
	topic := kf.config.topic
	uri := fmt.Sprintf("%s:%s", kf.config.host, kf.config.port)
	connection := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{uri},
		Topic:     topic,
		Partition: 0,
		MaxBytes:  10e6, // 10MB
	})
	// .SetOffset(42)

	kReader := KReader{connection: connection, topic: topic}
	kf.cleanups = append(kf.cleanups, func() {
		kReader.connection.Close()
	})
	return kReader
}



func (kReader *KReader) Read(cb func(key string, msg []byte)) {
	log.Printf(" [*] Waiting for messages. for Topic %s", kReader.topic)

	for {
		message, err := kReader.connection.ReadMessage(context.Background())
		onError(err, "Failed to listen to this message")

		key := string(message.Key)
		value :=message.Value
		cb(key, value)	
	}
}



func (kWriter *KWriter) Write(key string, body []byte) {
	message := kafka.Message{
		Key:   []byte(key),
		Value: body,
	}

	_, err := kWriter.connection.WriteMessages(message)
	onError(err, "Failed to write message")
}

func (kf *Kf) Close() {
	for _, cleanup := range kf.cleanups {
		cleanup()
	}
}
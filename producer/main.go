package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Establish a connection to the Kafka leader for the specified topic and partition.
	// This is necessary to send messages directly to the leader of the partition.
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "my-topic", 0)
	if err != nil {
		log.Fatalf("failed to connect to Kafka: %v", err)
	}

	// Ensure the connection is closed when the function exits to free up system resources.
	defer conn.Close()

	// Send a single message with the content "hello world" to the Kafka topic.
	_, err = conn.WriteMessages(kafka.Message{
		Value: []byte("hello world"),
	})
	if err != nil {
		log.Fatalf("failed to write messages: %v", err)
	}
}

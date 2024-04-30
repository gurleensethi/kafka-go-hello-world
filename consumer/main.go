package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Establish a connection to the Kafka leader for the specified topic and partition.
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", "my-topic", 0)
	if err != nil {
		log.Fatalf("failed to connect to Kafka: %v", err)
	}
	// Ensure the connection is closed when the function returns.
	defer conn.Close()

	// Create a batch reader that can read up to 1024 bytes of messages.
	batch := conn.ReadBatch(1, 1024)
	if err != nil {
		log.Fatalf("failed to create a message batch: %v", err)
	}

	// Attempt to read a single message from the batch.
	msg, err := batch.ReadMessage()
	if err != nil {
		log.Fatalf("failed to read message: %v", err)
	}

	// Output the message value to the console.
	fmt.Println("Received message:", string(msg.Value))

	err = batch.Close()
	if err != nil {
		log.Fatalf("failed to close message batch: %v", err)
	}
}
